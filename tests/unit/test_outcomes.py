"""Delivery control must not finish Celery results or run continuations."""

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from faststream.exceptions import (
    AckMessage,
    HandlerException,
    NackMessage,
    RejectMessage,
    SkipMessage,
)
from faststream.middlewares import AckPolicy

from faststream_celery import CeleryBroker, CeleryTask, TestCeleryBroker, signature
from faststream_celery.annotations import CeleryMessage
from faststream_celery.backend import InMemoryResultBackend
from faststream_celery.message import ConsumerMessage
from faststream_celery.schemas.task import build_task_envelope
from tests.helpers import consumer_message, recorded

if TYPE_CHECKING:
    from faststream_celery.publisher import CeleryPublisher


@pytest.fixture()
def publications() -> tuple[CeleryBroker, "CeleryPublisher", "CeleryPublisher"]:
    # Observe outgoing messages without recording their handlers as new
    # outcomes in the result backend under test.
    observer = CeleryBroker()
    return (
        observer,
        observer.publisher("reply-queue"),
        observer.publisher("continuations"),
    )


def incoming() -> ConsumerMessage:
    envelope = build_task_envelope(
        CeleryTask(
            "tests.outcome",
            link=[signature("tests.callback", options={"queue": "continuations"})],
            link_error=[signature("tests.errback", options={"queue": "continuations"})],
            chain=[signature("tests.following", options={"queue": "continuations"})],
        ),
        task_id="outcome-id",
    )
    return consumer_message(
        envelope.body,
        headers=envelope.headers,
        properties={"correlation_id": "outcome-id", "reply_to": "reply-queue"},
    )


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "exception",
    (AckMessage, NackMessage, RejectMessage, SkipMessage),
)
@pytest.mark.parametrize(
    "policy",
    (AckPolicy.REJECT_ON_ERROR, AckPolicy.MANUAL, AckPolicy.ACK_FIRST),
)
async def test_control_exception_leaves_result_and_canvas_untouched(
    exception: type[HandlerException],
    policy: AckPolicy,
    queue: str,
    publications: tuple[CeleryBroker, "CeleryPublisher", "CeleryPublisher"],
) -> None:
    observer, reply, canvas = publications
    broker = CeleryBroker(result_backend="redis://localhost", ack_policy=policy)
    subscriber = broker.subscriber(queue)
    instruction = exception()

    @subscriber
    async def handler() -> None:
        raise instruction

    message = incoming()
    async with TestCeleryBroker(broker, observer):
        backend = broker.config.broker_config.result_backend
        assert isinstance(backend, InMemoryResultBackend)
        if policy in {AckPolicy.MANUAL, AckPolicy.ACK_FIRST}:
            with pytest.raises(HandlerException) as error:
                await subscriber.process_message(message)
            assert error.value is instruction
        else:
            await subscriber.process_message(message)
        assert backend.results == {}
        reply.mock.assert_not_called()
        canvas.mock.assert_not_called()

    raw = recorded(message)
    if policy is AckPolicy.MANUAL:
        assert raw.acks == raw.rejects == []
    elif policy is AckPolicy.ACK_FIRST or exception is AckMessage:
        assert raw.acks == [False]
        assert raw.rejects == []
    elif exception is NackMessage:
        assert raw.acks == []
        assert raw.rejects == [True]
    elif exception is RejectMessage:
        assert raw.acks == []
        assert raw.rejects == [False]
    else:
        assert raw.acks == raw.rejects == []


@pytest.mark.asyncio()
@pytest.mark.parametrize("disposition", ("nack", "reject"))
@pytest.mark.parametrize("fail", (False, True))
async def test_manual_disposition_leaves_result_and_canvas_untouched(
    disposition: str,
    fail: bool,
    queue: str,
    publications: tuple[CeleryBroker, "CeleryPublisher", "CeleryPublisher"],
) -> None:
    observer, reply, canvas = publications
    broker = CeleryBroker(result_backend="redis://localhost")
    subscriber = broker.subscriber(queue, ack_policy=AckPolicy.MANUAL)

    @subscriber
    async def handler(message: CeleryMessage) -> int:
        await getattr(message, disposition)()
        if fail:
            raise ValueError("incomplete")
        return 7

    async with TestCeleryBroker(broker, observer):
        backend = broker.config.broker_config.result_backend
        assert isinstance(backend, InMemoryResultBackend)
        if fail:
            with pytest.raises(ValueError, match="incomplete"):
                await subscriber.process_message(incoming())
        else:
            await subscriber.process_message(incoming())
        assert backend.results == {}
        reply.mock.assert_not_called()
        canvas.mock.assert_not_called()


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ("broker_policy", "subscriber_policy"),
    (
        (AckPolicy.REJECT_ON_ERROR, AckPolicy.NACK_ON_ERROR),
        (AckPolicy.NACK_ON_ERROR, AckPolicy.REJECT_ON_ERROR),
    ),
)
async def test_subscriber_policy_decides_whether_failure_is_terminal(
    broker_policy: AckPolicy,
    subscriber_policy: AckPolicy,
    queue: str,
    publications: tuple[CeleryBroker, "CeleryPublisher", "CeleryPublisher"],
) -> None:
    observer, reply, canvas = publications
    broker = CeleryBroker(result_backend="redis://localhost", ack_policy=broker_policy)
    subscriber = broker.subscriber(queue, ack_policy=subscriber_policy)

    @subscriber
    async def handler() -> None:
        raise ValueError("failure")

    message = incoming()
    async with TestCeleryBroker(broker, observer):
        backend = broker.config.broker_config.result_backend
        assert isinstance(backend, InMemoryResultBackend)
        with pytest.raises(ValueError, match="failure"):
            await subscriber.process_message(message)
        if subscriber_policy is AckPolicy.NACK_ON_ERROR:
            assert backend.results == {}
            reply.mock.assert_not_called()
            canvas.mock.assert_not_called()
            assert recorded(message).rejects == [True]
        else:
            assert backend.results["outcome-id"]["status"] == "FAILURE"
            reply.mock.assert_called_once_with(backend.results["outcome-id"])
            canvas.mock.assert_called_once_with({"args": ["outcome-id"], "kwargs": {}})
            assert recorded(message).rejects == [False]


@pytest.mark.asyncio()
@pytest.mark.parametrize("fail", (False, True))
async def test_manual_ack_does_not_hide_a_normal_task_outcome(
    fail: bool,
    queue: str,
    publications: tuple[CeleryBroker, "CeleryPublisher", "CeleryPublisher"],
) -> None:
    observer, reply, canvas = publications
    broker = CeleryBroker(result_backend="redis://localhost")
    subscriber = broker.subscriber(queue, ack_policy=AckPolicy.NACK_ON_ERROR)

    @subscriber
    async def handler(message: CeleryMessage) -> int:
        await message.ack()
        if fail:
            raise ValueError("failure")
        return 7

    async with TestCeleryBroker(broker, observer):
        backend = broker.config.broker_config.result_backend
        assert isinstance(backend, InMemoryResultBackend)
        if fail:
            with pytest.raises(ValueError, match="failure"):
                await subscriber.process_message(incoming())
        else:
            await subscriber.process_message(incoming())
        assert backend.results["outcome-id"]["status"] == (
            "FAILURE" if fail else "SUCCESS"
        )
        reply.mock.assert_called_once()
        assert (
            reply.mock.call_args.args[0]["result"]
            == backend.results["outcome-id"]["result"]
        )
        if fail:
            canvas.mock.assert_called_once_with({"args": ["outcome-id"], "kwargs": {}})
        else:
            assert canvas.mock.call_count == 2


@pytest.mark.asyncio()
async def test_failed_ack_remains_eligible_for_redelivery(
    queue: str,
    publications: tuple[CeleryBroker, "CeleryPublisher", "CeleryPublisher"],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer, reply, canvas = publications
    broker = CeleryBroker(result_backend="redis://localhost")
    subscriber = broker.subscriber(queue, ack_policy=AckPolicy.NACK_ON_ERROR)

    @subscriber
    async def handler(message: CeleryMessage) -> None:
        await message.ack()

    message = incoming()
    raw = recorded(message)
    monkeypatch.setattr(raw, "ack", MagicMock(side_effect=OSError("connection lost")))
    async with TestCeleryBroker(broker, observer):
        backend = broker.config.broker_config.result_backend
        assert isinstance(backend, InMemoryResultBackend)
        with pytest.raises(OSError, match="connection lost"):
            await subscriber.process_message(message)
        assert raw.acks == []
        assert raw.rejects == [True]
        assert backend.results == {}
        reply.mock.assert_not_called()
        canvas.mock.assert_not_called()
