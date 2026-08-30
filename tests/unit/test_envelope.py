import json
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from faststream.response import PublishType
from faststream_celery.publisher.producer import (
    PERSISTENT_DELIVERY_MODE,
    CeleryFastProducer,
)
from faststream_celery.response import CeleryPublishCommand
from faststream_celery.task import CeleryTask, build_task_envelope

if TYPE_CHECKING:
    from kombu import Connection


def test_build_task_envelope() -> None:
    envelope = build_task_envelope(
        CeleryTask("proj.tasks.add", args=[1, 2], kwargs={"debug": True}),
        task_id="task-id-1",
    )

    args, kwargs, embed = envelope.body
    assert args == [1, 2]
    assert kwargs == {"debug": True}
    assert embed == {
        "callbacks": None,
        "errbacks": None,
        "chain": None,
        "chord": None,
    }

    headers = envelope.headers
    assert headers["task"] == "proj.tasks.add"
    assert headers["id"] == "task-id-1"
    assert headers["argsrepr"] == "[1, 2]"
    assert headers["kwargsrepr"] == "{'debug': True}"
    assert headers["lang"] == "py"
    assert headers["retries"] == 0


def test_celery_task_defaults() -> None:
    task = CeleryTask("proj.tasks.add")

    assert task.args == ()
    assert task.kwargs == {}


@pytest.mark.asyncio()
async def test_producer_publishes_v2_envelope() -> None:
    producer = CeleryFastProducer(parser=None, decoder=None)
    producer.connect(cast("Connection", MagicMock()))

    with patch.object(producer, "_producer") as kombu_producer:
        await producer.publish(
            CeleryPublishCommand(
                CeleryTask("proj.tasks.add", args=[1, 2]),
                queue="celery",
                correlation_id="task-id-1",
                _publish_type=PublishType.PUBLISH,
            ),
        )

    kombu_producer.publish.assert_called_once()
    args, kwargs = kombu_producer.publish.call_args

    body = args[0]
    assert body[0] == [1, 2]
    assert body[1] == {}
    assert body[2] == {
        "callbacks": None,
        "errbacks": None,
        "chain": None,
        "chord": None,
    }

    assert kwargs["serializer"] == "json"
    assert kwargs["headers"]["task"] == "proj.tasks.add"
    assert kwargs["headers"]["id"] == "task-id-1"
    assert kwargs["correlation_id"] == "task-id-1"
    assert kwargs["exchange"].name == "celery"
    assert kwargs["routing_key"] == "celery"
    assert kwargs["delivery_mode"] == PERSISTENT_DELIVERY_MODE


@pytest.mark.asyncio()
async def test_producer_generates_task_id() -> None:
    producer = CeleryFastProducer(parser=None, decoder=None)
    producer.connect(cast("Connection", MagicMock()))

    with patch.object(producer, "_producer") as kombu_producer:
        await producer.publish(
            CeleryPublishCommand(
                CeleryTask("proj.tasks.add"),
                queue="celery",
                _publish_type=PublishType.PUBLISH,
            ),
        )

    _, kwargs = kombu_producer.publish.call_args
    assert kwargs["correlation_id"]
    assert kwargs["headers"]["id"] == kwargs["correlation_id"]


@pytest.mark.asyncio()
async def test_producer_publishes_raw_message() -> None:
    producer = CeleryFastProducer(parser=None, decoder=None)
    producer.connect(cast("Connection", MagicMock()))

    with patch.object(producer, "_producer") as kombu_producer:
        await producer.publish(
            CeleryPublishCommand(
                {"payload": 1},
                queue="celery",
                headers={"h": "v"},
                correlation_id="cor-1",
                _publish_type=PublishType.PUBLISH,
            ),
        )

    kombu_producer.publish.assert_called_once()
    args, kwargs = kombu_producer.publish.call_args

    assert json.loads(args[0]) == {"payload": 1}
    assert kwargs["serializer"] is None
    assert kwargs["content_type"] == "application/json"
    assert kwargs["headers"] == {"h": "v"}
    assert kwargs["correlation_id"] == "cor-1"


@pytest.mark.asyncio()
async def test_producer_reply_publish_uses_default_exchange() -> None:
    producer = CeleryFastProducer(parser=None, decoder=None)
    producer.connect(cast("Connection", MagicMock()))

    with patch.object(producer, "_producer") as kombu_producer:
        await producer.publish(
            CeleryPublishCommand(
                "result",
                queue="reply-queue",
                exchange="",
                declare=False,
                _publish_type=PublishType.REPLY,
            ),
        )

    _, kwargs = kombu_producer.publish.call_args
    assert kwargs["exchange"].name == ""
    assert kwargs["routing_key"] == "reply-queue"
    assert kwargs["declare"] is None
