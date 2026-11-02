import asyncio
from unittest.mock import MagicMock

import pytest
from faststream.message import AckStatus

from faststream_celery.message import CeleryMessage, ConsumerMessage, local_message
from faststream_celery.parser import CeleryParser
from tests.helpers import RecordingMessage, raw_message


class PendingAcknowledgement:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.operations: list[AckStatus] = []

    async def ack(self) -> None:
        self.operations.append(AckStatus.ACKED)
        self.started.set()
        await self.release.wait()

    async def reject(self, *, requeue: bool = False) -> None:
        self.operations.append(AckStatus.NACKED if requeue else AckStatus.REJECTED)
        await self.release.wait()


@pytest.fixture()
def raw() -> RecordingMessage:
    return raw_message([[1, 2], {}, {}], headers={"task": "proj.tasks.add"})


@pytest.fixture()
def message(raw: RecordingMessage) -> CeleryMessage:
    return CeleryMessage(
        raw_message=ConsumerMessage(raw, raw),
        body=b"",
    )


class TestAcknowledgement:
    @pytest.mark.asyncio()
    async def test_concurrent_settlement_waits_and_keeps_the_first_operation(
        self,
        raw: RecordingMessage,
    ) -> None:
        acknowledgement = PendingAcknowledgement()
        message = CeleryMessage(
            raw_message=ConsumerMessage(raw, acknowledgement),
            body=b"",
        )
        first = asyncio.create_task(message.ack())
        following: list[asyncio.Task[None]] = []
        try:
            await asyncio.wait_for(acknowledgement.started.wait(), timeout=1)
            assert message.committed is None
            following = [
                asyncio.create_task(message.nack()),
                asyncio.create_task(message.reject()),
            ]
            await asyncio.sleep(0)
            assert not any(task.done() for task in following)
            acknowledgement.release.set()
            await asyncio.wait_for(asyncio.gather(first, *following), timeout=1)
            assert message.committed is AckStatus.ACKED
            assert acknowledgement.operations == [AckStatus.ACKED]
        finally:
            acknowledgement.release.set()
            await asyncio.gather(first, *following, return_exceptions=True)

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("operation", ("ack", "nack", "reject"))
    async def test_failed_settlement_leaves_the_message_uncommitted(
        self,
        message: CeleryMessage,
        raw: RecordingMessage,
        operation: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        method = "ack" if operation == "ack" else "reject"
        monkeypatch.setattr(
            raw,
            method,
            MagicMock(side_effect=OSError("connection lost")),
        )
        with pytest.raises(OSError, match="connection lost"):
            await getattr(message, operation)()
        assert message.committed is None
        assert raw.acks == raw.rejects == []

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("operation", ("ack", "nack", "reject"))
    async def test_backend_results_have_no_transport_to_settle(
        self,
        operation: str,
    ) -> None:
        raw = local_message(
            b"{}",
            content_type="application/json",
            correlation_id="result",
        )
        message = await CeleryParser().parse_message(raw)
        await getattr(message, operation)()
        assert message.committed is not None

    @pytest.mark.asyncio()
    async def test_ack_acknowledges_the_kombu_message(
        self,
        message: CeleryMessage,
        raw: RecordingMessage,
    ) -> None:
        await message.ack()

        assert raw.acks == [False]
        assert message.committed is AckStatus.ACKED

    @pytest.mark.asyncio()
    async def test_nack_requeues(
        self,
        message: CeleryMessage,
        raw: RecordingMessage,
    ) -> None:
        """Nack maps to a kombu reject with requeue of the same delivery."""
        await message.nack()

        assert raw.rejects == [True]
        assert message.committed is AckStatus.NACKED

    @pytest.mark.asyncio()
    async def test_reject_does_not_requeue(
        self,
        message: CeleryMessage,
        raw: RecordingMessage,
    ) -> None:
        await message.reject()

        assert raw.rejects == [False]
        assert message.committed is AckStatus.REJECTED

    @pytest.mark.asyncio()
    async def test_ack_is_idempotent(
        self,
        message: CeleryMessage,
        raw: RecordingMessage,
    ) -> None:
        await message.ack()
        await message.ack()

        assert raw.acks == [False]

    @pytest.mark.asyncio()
    async def test_only_the_first_settlement_counts(
        self,
        message: CeleryMessage,
        raw: RecordingMessage,
    ) -> None:
        await message.ack()
        await message.nack()
        await message.reject()

        assert raw.acks == [False]
        assert raw.rejects == []
        assert message.committed is AckStatus.ACKED

    @pytest.mark.asyncio()
    async def test_a_rejected_message_is_not_acked_later(
        self,
        message: CeleryMessage,
        raw: RecordingMessage,
    ) -> None:
        await message.reject()
        await message.ack()

        assert raw.rejects == [False]
        assert raw.acks == []


class TestKombuMessage:
    def test_it_exposes_the_message_behind_the_wrapper(
        self,
        message: CeleryMessage,
        raw: RecordingMessage,
    ) -> None:
        assert message.kombu_message is raw

    def test_the_raw_message_carries_its_acknowledgement(
        self,
        message: CeleryMessage,
    ) -> None:
        """`raw_message` is the broker's own message type, as FastStream expects."""
        assert isinstance(message.raw_message, ConsumerMessage)
        assert message.raw_message.acknowledgement is message.kombu_message
