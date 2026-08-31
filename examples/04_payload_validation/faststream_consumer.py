"""Three ways to validate a Celery task payload, none of them a raw dict.

A Celery task body is `(args, kwargs, embed)`; this broker normalizes it to
`{"args": [...], "kwargs": {...}}` and hands that to the FastStream pipeline,
which fills the handler's parameters from the body's keys and validates each
one with Pydantic.

    1. `args` + `kwargs`      — a typed tuple and a model, closest to the wire
    2. one whole-body model   — when a task has an invariant across both halves
    3. a `decoder`            — a handler that reads like the Celery task
                                signature it replaces: `send_email(user_id, urgent)`

Whichever you pick, an invalid payload fails the task with a `ValidationError`
before the handler body runs, and the Celery caller sees `FAILURE`.

One gotcha behind all three: the body's keys are mapped onto parameters only
when a handler declares **two or more** of them. A handler with a single body
parameter is handed the whole body instead — which is exactly what form 2
wants, and what form 1 must avoid by always declaring both `args` and
`kwargs`.

Run:
    faststream run faststream_consumer.py:app     # or: python faststream_consumer.py
"""

import asyncio
from typing import Annotated, Any, Literal

from faststream import FastStream
from faststream.message import StreamMessage
from pydantic import BaseModel, Field, PositiveInt, model_validator

from faststream_celery import CeleryBroker
from faststream_celery.annotations import Logger

BROKER_URL = "amqp://guest:guest@localhost:5672//"
QUEUE = "celery"

NoArgs = tuple[()]


# --- form 1: a typed tuple for `args`, a model for `kwargs` -----------------


class EmailOptions(BaseModel):
    """`kwargs` of `examples.send_email`."""

    user_id: PositiveInt
    urgent: bool = False
    locale: str = Field(default="en", pattern="^[a-z]{2}$")

    # An unexpected keyword is a bug in the caller, not something to ignore.
    model_config = {"extra": "forbid"}


class EmailSent(BaseModel):
    status: Literal["sent"] = "sent"
    user_id: int


# --- form 2: one model over the whole body ---------------------------------


class TransferOptions(BaseModel):
    """`kwargs` of `examples.transfer`."""

    amount_cents: PositiveInt
    currency: Literal["EUR", "USD"] = "EUR"


class TransferBody(BaseModel):
    """The whole body of `examples.transfer`.

    Worth a single model when a rule spans both halves — here, the account
    ids arrive positionally and the amount by keyword, and they have to
    agree.
    """

    args: tuple[PositiveInt, PositiveInt]
    kwargs: TransferOptions

    @model_validator(mode="after")
    def different_accounts(self) -> "TransferBody":
        source, target = self.args
        if source == target:
            msg = "cannot transfer to the same account"
            raise ValueError(msg)
        return self


# --- form 3: a decoder, so the handler reads like a Celery task ------------


async def celery_kwargs(
    msg: StreamMessage[Any],
    original: Any,
) -> Any:
    """Hand the handler the task's `kwargs`, and nothing else.

    A decoder runs after the parser and reshapes the decoded body. The
    normalized body is `{"args": ..., "kwargs": ...}`; returning just the
    `kwargs` half lets a handler declare the task's own parameter names.
    """
    body = await original(msg)

    if isinstance(body, dict) and "kwargs" in body:
        return body["kwargs"]

    return body


broker = CeleryBroker(BROKER_URL)
app = FastStream(broker)


@broker.subscriber(QUEUE, task="examples.send_email")
async def send_email(
    args: NoArgs,
    kwargs: EmailOptions,
    logger: Logger,
) -> EmailSent:
    """Form 1. `args` is declared even though the task takes none — which
    also rejects a caller that sends some.
    """
    logger.info("emailing %s (urgent=%s)", kwargs.user_id, kwargs.urgent)
    return EmailSent(user_id=kwargs.user_id)


@broker.subscriber(QUEUE, task="examples.transfer")
async def transfer(body: TransferBody, logger: Logger) -> dict[str, Any]:
    """Form 2. One parameter, so the whole body is validated as one model."""
    source, target = body.args

    logger.info(
        "transferring %s %s from %s to %s",
        body.kwargs.amount_cents,
        body.kwargs.currency,
        source,
        target,
    )

    return {"from": source, "to": target, "amount_cents": body.kwargs.amount_cents}


@broker.subscriber(QUEUE, task="examples.resize_image", decoder=celery_kwargs)
async def resize_image(
    upload_id: str,
    width: Annotated[int, Field(gt=0, le=4096)],
    height: Annotated[int, Field(gt=0, le=4096)],
    logger: Logger,
) -> dict[str, Any]:
    """Form 3. The signature the Celery task had, validated per parameter.

    Called as `send_task("examples.resize_image", kwargs={"upload_id": ...,
    "width": 800, "height": 600})` — keyword arguments only, since that is
    what the decoder passes on.
    """
    logger.info("resize %s to %sx%s", upload_id, width, height)
    return {"upload_id": upload_id, "size": [width, height]}


if __name__ == "__main__":
    asyncio.run(app.run())
