"""ETA, countdown and expiry, as a Celery worker does them.

A task with a future `eta` is held by an in-memory scheduler inside the
subscriber until it is due, then handed to the handler — the same thing a Celery
worker does, with the same consequence: a restart loses what was still waiting
(Celery does not persist held tasks either).

A task whose `expires` has passed is dropped instead of run, and never reaches
the handler.

Run:
    faststream run faststream_consumer.py:app     # or: python faststream_consumer.py
"""

import asyncio
from datetime import datetime, timezone
from typing import Literal

from faststream import FastStream
from pydantic import BaseModel

from faststream_celery import CeleryBroker
from faststream_celery.annotations import CeleryMessage, Logger

BROKER_URL = "amqp://guest:guest@localhost:5672//"
QUEUE = "celery"

NoArgs = tuple[()]


class Reminder(BaseModel):
    """`kwargs` of `examples.remind`."""

    user_id: int
    label: str


class ReminderSent(BaseModel):
    status: Literal["sent"] = "sent"
    label: str
    late_by_seconds: float


broker = CeleryBroker(BROKER_URL)
app = FastStream(broker)


@broker.subscriber(QUEUE, task="examples.remind")
async def remind(
    args: NoArgs,
    kwargs: Reminder,
    message: CeleryMessage,
    logger: Logger,
) -> ReminderSent:
    """Report how close to its `eta` the task actually ran."""
    eta = message.headers.get("eta")
    late_by = 0.0

    if eta is not None:
        due = datetime.fromisoformat(eta)
        late_by = (datetime.now(timezone.utc) - due).total_seconds()

    logger.info("reminder %r ran %.2fs after its eta", kwargs.label, late_by)

    return ReminderSent(label=kwargs.label, late_by_seconds=round(late_by, 2))


if __name__ == "__main__":
    asyncio.run(app.run())
