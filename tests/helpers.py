"""Shared building blocks for the test suite.

A plain module rather than a ``conftest``: pytest loads conftest files
specially, so importing from one can produce a duplicated module and
confusing collection errors.
"""

import json
from collections.abc import AsyncGenerator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from kombu import Message
from typing_extensions import override

from faststream_celery import CeleryBroker, CeleryTask
from faststream_celery.message import ConsumerMessage
from faststream_celery.schemas.task import build_task_envelope

# kombu's in-process transport: a real Consumer, real acks, real
# `drain_events`, and no broker to run.
MEMORY_URL = "memory://faststream-celery/"

JSON_CONTENT_TYPE = "application/json"

DEFAULT_TASK = "proj.tasks.add"
DEFAULT_TASK_ID = "task-id-1"


class RecordingMessage(Message):
    """A kombu message that settles with no channel, and remembers how.

    Lets a test assert on acknowledgements without patching kombu: the real
    `Message.ack()` needs a channel, and a `MagicMock` would not move the
    message's own state, which the broker reads back.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.acks: list[bool] = []
        self.rejects: list[bool] = []

    @override
    def ack(self, multiple: bool = False) -> None:
        self.acks.append(multiple)
        self._state = "ACK"

    @override
    def reject(self, requeue: bool = False) -> None:
        self.rejects.append(requeue)
        self._state = "REJECTED"


async def run_inline(action: Callable[[], None]) -> None:
    """Ack executor that runs on the caller's thread."""
    action()


def raw_message(
    body: Any,
    *,
    headers: Mapping[str, Any] | None = None,
    properties: Mapping[str, Any] | None = None,
    content_type: str | None = JSON_CONTENT_TYPE,
) -> RecordingMessage:
    """A kombu message as a consumer would have received it.

    The body goes in as bytes, the way every real transport delivers it; the
    kombu stubs describe the narrower `str` a producer may pass instead.
    """
    return RecordingMessage(
        body=json.dumps(body).encode(),
        content_type=content_type,
        content_encoding="utf-8",
        headers=dict(headers or {}),
        properties=dict(properties or {}),
    )


def consumer_message(
    body: Any,
    *,
    headers: Mapping[str, Any] | None = None,
    properties: Mapping[str, Any] | None = None,
    content_type: str | None = JSON_CONTENT_TYPE,
) -> ConsumerMessage:
    """A raw message wrapped for the FastStream pipeline."""
    return ConsumerMessage(
        raw_message(
            body,
            headers=headers,
            properties=properties,
            content_type=content_type,
        ),
        run_inline,
    )


def task_message(  # ruff: ignore[too-many-arguments]
    task: str = DEFAULT_TASK,
    *,
    args: Sequence[Any] = (),
    kwargs: Mapping[str, Any] | None = None,
    task_id: str = DEFAULT_TASK_ID,
    eta: datetime | None = None,
    expires: datetime | float | None = None,
    properties: Mapping[str, Any] | None = None,
) -> ConsumerMessage:
    """A protocol v2 task message, as Celery would have published it."""
    envelope = build_task_envelope(
        CeleryTask(
            task,
            args=args,
            kwargs=dict(kwargs or {}),
            eta=eta,
            expires=expires,
        ),
        task_id=task_id,
    )

    return consumer_message(
        envelope.body,
        headers=envelope.headers,
        properties=properties,
    )


def v1_task_message(
    task: str = DEFAULT_TASK,
    *,
    args: Sequence[Any] = (),
    kwargs: Mapping[str, Any] | None = None,
    task_id: str = DEFAULT_TASK_ID,
    **extra: Any,
) -> ConsumerMessage:
    """A protocol v1 task message: a flat body and no headers at all."""
    return consumer_message(
        {
            "task": task,
            "id": task_id,
            "args": list(args),
            "kwargs": dict(kwargs or {}),
            **extra,
        },
    )


def recorded(msg: ConsumerMessage) -> RecordingMessage:
    """The recording message behind a consumer message."""
    assert isinstance(msg.message, RecordingMessage)
    return msg.message


@asynccontextmanager
async def running(broker: CeleryBroker) -> AsyncGenerator[CeleryBroker]:
    """Start the broker and its subscribers for the duration of a block."""
    await broker.start()
    try:
        yield broker
    finally:
        await broker.stop()
