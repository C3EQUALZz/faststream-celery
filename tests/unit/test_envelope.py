import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, cast
from unittest.mock import MagicMock, patch

import pytest
from faststream.response import PublishType
from pydantic import BaseModel

from faststream_celery.publisher.producer import CeleryFastProducer, jsonable_body
from faststream_celery.response import CeleryPublishCommand
from faststream_celery.schemas.constants import PERSISTENT_DELIVERY_MODE
from faststream_celery.schemas.task import CeleryTask, build_task_envelope
from faststream_celery.types import TaskEmbed

if TYPE_CHECKING:
    from kombu import Connection

    from faststream_celery.types import TaskBody

_MOMENT = datetime(2026, 1, 1, tzinfo=timezone.utc)

_EMPTY_EMBED = TaskEmbed(callbacks=None, errbacks=None, chain=None, chord=None)


class _Report(BaseModel):
    """A task argument that is not JSON by itself."""

    rows: int


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
    producer.connect(
        cast("Connection", MagicMock()),
        connection_factory=MagicMock(),
    )

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
    producer.connect(
        cast("Connection", MagicMock()),
        connection_factory=MagicMock(),
    )

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
    producer.connect(
        cast("Connection", MagicMock()),
        connection_factory=MagicMock(),
    )

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
    producer.connect(
        cast("Connection", MagicMock()),
        connection_factory=MagicMock(),
    )

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


class TestBodyIsReducedToJson:
    """kombu's `json` serializer only takes plain types.

    A task argument may be anything FastStream can serialize — and a canvas
    step is called with whatever the previous handler returned, which is often
    a Pydantic model. Handing that to kombu raises `EncodeError` at publish
    time, with the task already run and its continuation lost.
    """

    def test_a_model_becomes_a_dict(self) -> None:
        args, kwargs, _ = jsonable_body(
            ([_Report(rows=3)], {"at": _MOMENT}, _EMPTY_EMBED),
        )

        assert args == [{"rows": 3}]
        assert kwargs == {"at": "2026-01-01T00:00:00Z"}

    def test_plain_types_survive_unchanged(self) -> None:
        body: TaskBody = ([1, "two"], {"three": True}, _EMPTY_EMBED)

        assert jsonable_body(body) == body

    @pytest.mark.asyncio()
    async def test_a_model_argument_reaches_the_wire(self) -> None:
        producer = CeleryFastProducer(parser=None, decoder=None)
        producer.connect(
            cast("Connection", MagicMock()),
            connection_factory=MagicMock(),
        )

        with patch.object(producer, "_producer") as kombu_producer:
            await producer.publish(
                CeleryPublishCommand(
                    CeleryTask("proj.tasks.notify", args=[_Report(rows=3)]),
                    queue="celery",
                    correlation_id="task-id-1",
                    _publish_type=PublishType.PUBLISH,
                ),
            )

        (body, *_), kwargs = kombu_producer.publish.call_args

        assert body[0] == [{"rows": 3}]
        # Unchanged from what Celery writes: the serializer still does the
        # encoding, so content type and encoding are its own.
        assert kwargs["serializer"] == "json"
