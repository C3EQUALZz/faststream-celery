import pytest
from faststream.message import AckStatus

from faststream_celery.message import CeleryMessage, ConsumerMessage, run_inline
from tests.helpers import RecordingMessage, raw_message


@pytest.fixture()
def raw() -> RecordingMessage:
    return raw_message([[1, 2], {}, {}], headers={"task": "proj.tasks.add"})


@pytest.fixture()
def message(raw: RecordingMessage) -> CeleryMessage:
    return CeleryMessage(
        raw_message=ConsumerMessage(raw, run_inline),
        body=b"",
        ack_executor=run_inline,
    )


class TestAcknowledgement:
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
        """Nack maps to a kombu reject with requeue (Celery retry semantics)."""
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

    def test_the_raw_message_carries_the_ack_executor(
        self,
        message: CeleryMessage,
    ) -> None:
        """`raw_message` is the broker's own message type, as FastStream expects."""
        assert isinstance(message.raw_message, ConsumerMessage)
        assert message.raw_message.executor is run_inline
