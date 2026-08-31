"""Scheduling in the other direction: FastStream defers, Celery runs.

`countdown`, `eta` and `expires` on `CeleryTask` are the `apply_async` options
of the same name, written into the task headers — so the deferral is decided
here and honoured by the stock Celery worker next door.

`celery_worker.py` must be running.

Run:
    faststream run faststream_publisher.py:app    # or: python faststream_publisher.py
"""

import asyncio
from datetime import datetime, timedelta, timezone

from faststream import FastStream

from faststream_celery import CeleryBroker, CeleryTask

BROKER_URL = "amqp://guest:guest@localhost:5672//"
QUEUE = "celery"
COUNTDOWN = 5.0
ETA_SECONDS = 8.0
EXPIRES_SECONDS = 60.0

broker = CeleryBroker(BROKER_URL)
app = FastStream(broker)


@app.after_startup
async def schedule() -> None:
    # Seconds from now.
    await broker.publish(
        CeleryTask("examples.remind", kwargs={"label": "countdown"}, countdown=COUNTDOWN),
        queue=QUEUE,
    )

    # An absolute moment. A naive datetime is read as UTC, as Celery does with
    # `enable_utc`.
    await broker.publish(
        CeleryTask(
            "examples.remind",
            kwargs={"label": "eta"},
            eta=datetime.now(timezone.utc) + timedelta(seconds=ETA_SECONDS),
        ),
        queue=QUEUE,
    )

    # Deferred and perishable: due in five seconds, worthless after a minute.
    await broker.publish(
        CeleryTask(
            "examples.remind",
            kwargs={"label": "countdown with expiry"},
            countdown=COUNTDOWN,
            expires=EXPIRES_SECONDS,
        ),
        queue=QUEUE,
    )

    # Already expired: the worker drops it instead of running it.
    await broker.publish(
        CeleryTask("examples.remind", kwargs={"label": "too late"}, expires=-1),
        queue=QUEUE,
    )

    print(f"scheduled 4 tasks on {QUEUE!r}")


if __name__ == "__main__":
    asyncio.run(app.run())
