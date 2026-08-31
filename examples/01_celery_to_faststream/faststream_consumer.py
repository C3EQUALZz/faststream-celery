"""Celery publishes, FastStream consumes — the FastStream side.

Every handler validates its input with a Pydantic model instead of poking at
raw dicts: a Celery task body is `(args, kwargs, embed)` on the wire, which
this broker normalizes to `{"args": [...], "kwargs": {...}}`, so a handler
declares `args` (positional, validated as a typed tuple) and `kwargs`
(keyword, validated as a model). A payload that does not fit fails the task
with a `ValidationError`, and the Celery caller sees `FAILURE` — try it with
`python celery_client.py bad`.

Start this first, then run `celery_client.py` in another terminal.

Run:
    faststream run faststream_consumer.py:app        # needs faststream[cli]
    python faststream_consumer.py                    # or without the CLI
"""

import asyncio
from typing import Any, Literal

from faststream import FastStream
from pydantic import BaseModel, Field, PositiveInt

from faststream_celery import CeleryBroker
from faststream_celery.annotations import CeleryMessage, Logger

BROKER_URL = "amqp://guest:guest@localhost:5672//"
QUEUE = "celery"

# No positional arguments expected: a caller that sends any fails validation.
NoArgs = tuple[()]


class EmailOptions(BaseModel):
    """`kwargs` of `examples.send_email`."""

    user_id: PositiveInt
    urgent: bool = False
    locale: str = Field(default="en", pattern="^[a-z]{2}$")


class EmailSent(BaseModel):
    """What the caller gets back from `AsyncResult.get()`."""

    status: Literal["sent"] = "sent"
    user_id: int
    locale: str


class ResizeOptions(BaseModel):
    """`kwargs` of `examples.resize_image`."""

    width: int = Field(gt=0, le=4096)
    height: int = Field(gt=0, le=4096)


broker = CeleryBroker(BROKER_URL)
app = FastStream(broker)


@broker.subscriber(QUEUE, task="examples.add")
async def add(args: tuple[int, int], kwargs: dict[str, Any], logger: Logger) -> int:
    """Positional arguments, validated and coerced by the tuple annotation.

    `send_task("examples.add", args=["2", 3])` arrives here as `(2, 3)`.
    """
    logger.info("add%s", args)
    return sum(args)


@broker.subscriber(QUEUE, task="examples.send_email")
async def send_email(
    args: NoArgs,
    kwargs: EmailOptions,
    logger: Logger,
) -> EmailSent:
    """Keyword arguments, validated by a Pydantic model.

    A Pydantic model returned from a handler is serialized to JSON for the
    Celery result envelope, so the caller reads it as a plain dict.
    """
    logger.info("emailing user %s (urgent=%s)", kwargs.user_id, kwargs.urgent)
    return EmailSent(user_id=kwargs.user_id, locale=kwargs.locale)


@broker.subscriber(QUEUE, task="examples.resize_image")
async def resize_image(
    args: tuple[str],
    kwargs: ResizeOptions,
    message: CeleryMessage,
    logger: Logger,
) -> dict[str, Any]:
    """Both halves at once, plus the raw message for the Celery metadata."""
    (upload_id,) = args

    logger.info("resize %s to %sx%s", upload_id, kwargs.width, kwargs.height)

    return {
        "upload_id": upload_id,
        "size": [kwargs.width, kwargs.height],
        "task_id": message.headers["id"],
        "retries": message.headers.get("retries"),
    }


@broker.subscriber(QUEUE, task="examples.fail")
async def fail(args: NoArgs, kwargs: dict[str, str]) -> None:
    """An exception here becomes a Celery `FAILURE`, traceback included."""
    raise RuntimeError(kwargs.get("message", "boom"))


@broker.subscriber(QUEUE)
async def anything_else(message: CeleryMessage, logger: Logger) -> None:
    """A catch-all. Without it, an unclaimed task is rejected."""
    logger.warning("no handler claims %s", message.headers["task"])


if __name__ == "__main__":
    asyncio.run(app.run())
