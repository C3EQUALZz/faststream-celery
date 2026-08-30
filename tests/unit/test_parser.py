import json
from collections.abc import Callable
from typing import Any

import pytest
from kombu import Message

from faststream_celery.message import ConsumerMessage
from faststream_celery.parser import CeleryParser


async def _run_inline(action: Callable[[], None]) -> None:
    action()


def _make_consumer_message(
    body: Any,
    *,
    headers: dict[str, Any] | None = None,
    properties: dict[str, Any] | None = None,
) -> ConsumerMessage:
    raw = Message(
        body=json.dumps(body).encode(),
        content_type="application/json",
        content_encoding="utf-8",
        headers=headers or {},
        properties=properties or {},
    )
    return ConsumerMessage(raw, _run_inline)


@pytest.mark.asyncio()
async def test_parse_v2_message() -> None:
    parser = CeleryParser()

    msg = await parser.parse_message(
        _make_consumer_message(
            [[1, 2], {"debug": True}, {}],
            headers={"task": "proj.tasks.add", "id": "task-id-1"},
            properties={"correlation_id": "task-id-1", "reply_to": "reply-queue"},
        ),
    )

    assert msg.headers["task"] == "proj.tasks.add"
    assert msg.message_id == "task-id-1"
    assert msg.correlation_id == "task-id-1"
    assert msg.reply_to == "reply-queue"
    assert json.loads(msg.body) == {"args": [1, 2], "kwargs": {"debug": True}}


@pytest.mark.asyncio()
async def test_parse_v2_message_without_properties() -> None:
    parser = CeleryParser()

    msg = await parser.parse_message(
        _make_consumer_message(
            [[], {}, {}],
            headers={"task": "proj.tasks.add", "id": "task-id-2"},
        ),
    )

    assert msg.reply_to == ""
    assert msg.message_id == "task-id-2"
    assert msg.correlation_id  # generated fallback


@pytest.mark.asyncio()
async def test_decode_message() -> None:
    parser = CeleryParser()

    msg = await parser.parse_message(
        _make_consumer_message(
            [["a"], {"b": 1}, {}],
            headers={"task": "proj.tasks.add"},
        ),
    )

    assert await parser.decode_message(msg) == {"args": ["a"], "kwargs": {"b": 1}}


@pytest.mark.asyncio()
async def test_v1_message_is_rejected() -> None:
    """A body without the ``task`` header is a protocol v1 message."""
    parser = CeleryParser()

    with pytest.raises(ValueError, match="protocol v1"):
        await parser.parse_message(
            _make_consumer_message(
                {"task": "proj.tasks.add", "args": [1, 2], "kwargs": {}},
            ),
        )


@pytest.mark.asyncio()
async def test_invalid_v2_body_is_rejected() -> None:
    parser = CeleryParser()

    with pytest.raises(ValueError, match="Invalid Celery protocol v2 body"):
        await parser.parse_message(
            _make_consumer_message(
                {"not": "a sequence"},
                headers={"task": "proj.tasks.add"},
            ),
        )


@pytest.mark.asyncio()
async def test_incomplete_v2_body_is_rejected() -> None:
    parser = CeleryParser()

    with pytest.raises(ValueError, match="Invalid Celery protocol v2 body"):
        await parser.parse_message(
            _make_consumer_message(
                [[1, 2]],
                headers={"task": "proj.tasks.add"},
            ),
        )
