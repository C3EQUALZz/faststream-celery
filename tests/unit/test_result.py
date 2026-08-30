import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import patch

import pytest
from faststream.response import PublishType

from faststream_celery import CeleryBroker, CeleryTask, TestCeleryBroker
from faststream_celery.response import CeleryPublishCommand
from faststream_celery.schemas.result import (
    FAILURE,
    SUCCESS,
    build_failure,
    build_success,
)


def test_build_success_envelope() -> None:
    envelope = build_success("task-id-1", {"value": 3})

    assert envelope == {
        "task_id": "task-id-1",
        "status": SUCCESS,
        "result": {"value": 3},
        "traceback": None,
        "children": [],
    }


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


@asynccontextmanager
async def _replies(broker: CeleryBroker) -> AsyncIterator[list[CeleryPublishCommand]]:
    """Collect every reply the broker publishes while the block runs."""
    recorded: list[CeleryPublishCommand] = []

    async with TestCeleryBroker(broker):
        producer = broker.config.producer
        original = producer.publish

        async def recording_publish(cmd: CeleryPublishCommand) -> None:
            if cmd.publish_type is PublishType.REPLY:
                recorded.append(cmd)
            await original(cmd)

        with patch.object(producer, "publish", new=recording_publish):
            yield recorded


@pytest.mark.asyncio()
async def test_successful_handler_replies_with_a_result_envelope(queue: str) -> None:
    broker = CeleryBroker()

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler() -> int:
        return 3

    async with _replies(broker) as replies:
        await broker.publish(
            CeleryTask("proj.tasks.add"),
            queue=queue,
            correlation_id="task-id-1",
            reply_to="reply-queue",
        )

    (reply,) = replies
    assert reply.queue == "reply-queue"
    assert reply.exchange == ""
    assert reply.declare is False
    assert reply.correlation_id == "task-id-1"
    assert reply.body == {
        "task_id": "task-id-1",
        "status": SUCCESS,
        "result": 3,
        "traceback": None,
        "children": [],
    }


@pytest.mark.asyncio()
async def test_failing_handler_replies_with_a_failure_envelope(queue: str) -> None:
    broker = CeleryBroker()

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler() -> None:
        msg = "boom"
        raise ValueError(msg)

    async with _replies(broker) as replies:
        # A test broker re-raises handler errors so the test can see them;
        # the FAILURE reply is published on the way out.
        with pytest.raises(ValueError, match="boom"):
            await broker.publish(
                CeleryTask("proj.tasks.add"),
                queue=queue,
                correlation_id="task-id-1",
                reply_to="reply-queue",
            )

    (reply,) = replies
    assert reply.queue == "reply-queue"
    assert reply.exchange == ""
    assert reply.correlation_id == "task-id-1"
    assert reply.body["status"] == FAILURE
    assert reply.body["result"]["exc_type"] == "ValueError"
    assert reply.body["result"]["exc_message"] == ["boom"]


@pytest.mark.asyncio()
async def test_ignore_result_suppresses_the_reply(queue: str) -> None:
    broker = CeleryBroker()

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler() -> int:
        return 3

    async with _replies(broker) as replies:
        await broker.publish(
            CeleryTask("proj.tasks.add"),
            queue=queue,
            correlation_id="task-id-1",
            reply_to="reply-queue",
            headers={"ignore_result": True},
        )

    assert replies == []


@pytest.mark.asyncio()
async def test_ignore_result_suppresses_the_failure_reply(queue: str) -> None:
    broker = CeleryBroker()

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler() -> None:
        msg = "boom"
        raise ValueError(msg)

    async with _replies(broker) as replies:
        with pytest.raises(ValueError, match="boom"):
            await broker.publish(
                CeleryTask("proj.tasks.add"),
                queue=queue,
                correlation_id="task-id-1",
                reply_to="reply-queue",
                headers={"ignore_result": True},
            )

    assert replies == []


@pytest.mark.asyncio()
async def test_no_reply_to_means_no_reply(queue: str) -> None:
    broker = CeleryBroker()

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler() -> int:
        return 3

    async with _replies(broker) as replies:
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

        assert await response.decode() == 3
