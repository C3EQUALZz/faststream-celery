import json
from datetime import datetime, timezone
from typing import Any

import pytest
from faststream.response import PublishCommand, PublishType

from faststream_celery import CeleryBroker, CeleryTask, TestCeleryBroker
from faststream_celery.annotations import CeleryMessage
from faststream_celery.publisher.fake import CeleryFakePublisher
from faststream_celery.publisher.producer import CeleryFastProducer
from faststream_celery.schemas.result import (
    FAILURE,
    SUCCESS,
    build_failure,
    build_success,
)


def test_build_success_envelope() -> None:
    envelope = build_success("task-id-1", {"value": 3})

    assert envelope["task_id"] == "task-id-1"
    assert envelope["status"] == SUCCESS
    assert envelope["result"] == {"value": 3}
    assert envelope["traceback"] is None
    assert envelope["children"] == []


def test_an_envelope_is_stamped_with_the_time_it_finished() -> None:
    """Celery clients read `date_done` off the result meta."""
    before = datetime.now(timezone.utc)

    envelope = build_success("task-id-1", None)

    assert before <= datetime.fromisoformat(envelope["date_done"])


def test_build_failure_envelope() -> None:
    exc = ValueError("boom")

    envelope = build_failure("task-id-1", exc)

    assert envelope["task_id"] == "task-id-1"
    assert envelope["status"] == FAILURE
    assert envelope["result"] == {
        "exc_type": "ValueError",
        "exc_message": ["boom"],
        "exc_module": "builtins",
    }
    assert envelope["traceback"] is not None
    assert "ValueError: boom" in envelope["traceback"]


def test_failure_envelope_is_json_serializable() -> None:
    envelope = build_failure("task-id-1", RuntimeError("boom", 42))

    assert json.loads(json.dumps(envelope))["result"]["exc_message"] == ["boom", 42]


@pytest.fixture()
def reply_observer(queue: str) -> tuple[CeleryBroker, list[CeleryMessage]]:
    observer = CeleryBroker()
    replies: list[CeleryMessage] = []

    @observer.subscriber(f"{queue}.replies")
    async def capture(message: CeleryMessage) -> None:
        replies.append(message)

    return observer, replies


def test_success_reply_command_uses_the_client_owned_queue() -> None:
    publisher = CeleryFakePublisher(
        CeleryFastProducer(parser=None, decoder=None),
        queue="reply-queue",
        task_id="task-id-1",
    )
    reply = publisher.patch_command(PublishCommand(3, _publish_type=PublishType.REPLY))

    assert reply.queue == "reply-queue"
    assert reply.exchange == ""
    assert reply.declare is False
    assert reply.correlation_id == "task-id-1"
    assert reply.body["task_id"] == "task-id-1"
    assert reply.body["status"] == SUCCESS
    assert reply.body["result"] == 3


@pytest.mark.asyncio()
async def test_successful_handler_replies_with_a_result_envelope(
    queue: str,
    reply_observer: tuple[CeleryBroker, list[CeleryMessage]],
) -> None:
    broker = CeleryBroker()
    observer, replies = reply_observer

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler() -> int:
        return 3

    async with TestCeleryBroker(broker, observer):
        await broker.publish(
            CeleryTask("proj.tasks.add"),
            queue=queue,
            correlation_id="task-id-1",
            reply_to=f"{queue}.replies",
        )

    (reply,) = replies
    assert reply.raw_message.message.delivery_info["routing_key"] == f"{queue}.replies"
    assert reply.raw_message.message.delivery_info["exchange"] == ""
    assert reply.correlation_id == "task-id-1"
    envelope = await reply.decode()
    assert isinstance(envelope, dict)
    assert envelope["task_id"] == "task-id-1"
    assert envelope["status"] == SUCCESS
    assert envelope["result"] == 3
    assert envelope["traceback"] is None


@pytest.mark.asyncio()
async def test_failing_handler_replies_with_a_failure_envelope(
    queue: str,
    reply_observer: tuple[CeleryBroker, list[CeleryMessage]],
) -> None:
    broker = CeleryBroker()
    observer, replies = reply_observer

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler() -> None:
        msg = "boom"
        raise ValueError(msg)

    async with TestCeleryBroker(broker, observer):
        # A test broker re-raises handler errors so the test can see them;
        # the FAILURE reply is published on the way out.
        with pytest.raises(ValueError, match="boom"):
            await broker.publish(
                CeleryTask("proj.tasks.add"),
                queue=queue,
                correlation_id="task-id-1",
                reply_to=f"{queue}.replies",
            )

    (reply,) = replies
    assert reply.raw_message.message.delivery_info["routing_key"] == f"{queue}.replies"
    assert reply.raw_message.message.delivery_info["exchange"] == ""
    assert reply.correlation_id == "task-id-1"
    envelope = await reply.decode()
    assert isinstance(envelope, dict)
    assert envelope["status"] == FAILURE
    assert envelope["result"]["exc_type"] == "ValueError"
    assert envelope["result"]["exc_message"] == ["boom"]


@pytest.mark.asyncio()
async def test_ignore_result_suppresses_the_reply(
    queue: str,
    reply_observer: tuple[CeleryBroker, list[CeleryMessage]],
) -> None:
    broker = CeleryBroker()
    observer, replies = reply_observer

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler() -> int:
        return 3

    async with TestCeleryBroker(broker, observer):
        await broker.publish(
            CeleryTask("proj.tasks.add"),
            queue=queue,
            correlation_id="task-id-1",
            reply_to=f"{queue}.replies",
            headers={"ignore_result": True},
        )

    assert replies == []


@pytest.mark.asyncio()
async def test_ignore_result_suppresses_the_failure_reply(
    queue: str,
    reply_observer: tuple[CeleryBroker, list[CeleryMessage]],
) -> None:
    broker = CeleryBroker()
    observer, replies = reply_observer

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler() -> None:
        msg = "boom"
        raise ValueError(msg)

    async with TestCeleryBroker(broker, observer):
        with pytest.raises(ValueError, match="boom"):
            await broker.publish(
                CeleryTask("proj.tasks.add"),
                queue=queue,
                correlation_id="task-id-1",
                reply_to=f"{queue}.replies",
                headers={"ignore_result": True},
            )

    assert replies == []


@pytest.mark.asyncio()
async def test_no_reply_to_means_no_reply(
    queue: str,
    reply_observer: tuple[CeleryBroker, list[CeleryMessage]],
) -> None:
    broker = CeleryBroker()
    observer, replies = reply_observer

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler() -> int:
        return 3

    async with TestCeleryBroker(broker, observer):
        await broker.publish(
            CeleryTask("proj.tasks.add"),
            queue=queue,
            correlation_id="task-id-1",
        )

    assert replies == []


@pytest.mark.asyncio()
async def test_request_round_trip_in_fake_mode(queue: str) -> None:
    broker = CeleryBroker()

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler(args: list[int], kwargs: dict[str, Any]) -> int:
        return sum(args)

    async with TestCeleryBroker(broker):
        response = await broker.request(
            CeleryTask("proj.tasks.add", args=[1, 2]),
            queue=queue,
        )

        envelope = await response.decode()
        assert isinstance(envelope, dict)
        assert envelope["status"] == SUCCESS
        assert envelope["result"] == 3
