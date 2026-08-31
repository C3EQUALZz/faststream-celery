"""faststream-celery publishes, Celery consumes — the FastStream side.

`CeleryTask` writes the same protocol v2 envelope `Task.apply_async` writes, so
the `celery worker` next door runs it without knowing who sent it. The payload
is built from a Pydantic model, which is what keeps a typo out of the `kwargs`
dict a stock Celery task will unpack.

`celery_worker.py` must be running. Watch its log.

Run:
    faststream run faststream_publisher.py:app     # or: python faststream_publisher.py
"""

import asyncio

from faststream import FastStream
from pydantic import BaseModel, Field, PositiveInt

from faststream_celery import CeleryBroker, CeleryTask

BROKER_URL = "amqp://guest:guest@localhost:5672//"
QUEUE = "celery"
COUNTDOWN = 5.0
EXPIRES = 3600.0


class EmailOptions(BaseModel):
    """The schema `celery_worker.py` validates on arrival."""

    user_id: PositiveInt
    urgent: bool = False
    locale: str = Field(default="en", pattern="^[a-z]{2}$")


broker = CeleryBroker(BROKER_URL)
app = FastStream(broker)

# A long-living publisher: bound to a queue once, and documented in the
# AsyncAPI schema. `broker.publish(..., queue=...)` is the ad-hoc equivalent.
tasks = broker.publisher(QUEUE, description="Tasks handed to the Celery workers.")


@app.after_startup
async def publish_tasks() -> None:
    # Positional arguments land on the Celery signature as `add(2, 3)`.
    await tasks.publish(CeleryTask("examples.add", args=[2, 3]))

    # Keyword arguments, validated here before they go on the wire.
    options = EmailOptions(user_id=42, urgent=True, locale="ru")
    await tasks.publish(
        CeleryTask("examples.send_email", kwargs=options.model_dump()),
    )

    # Your own task id, so a log line on either side can be correlated.
    await broker.publish(
        CeleryTask("examples.add", args=[10, 5]),
        queue=QUEUE,
        correlation_id="order-1234-total",
    )

    # Deferred, exactly as `apply_async(countdown=...)` defers.
    await broker.publish(
        CeleryTask("examples.send_email", kwargs={"user_id": 43}, countdown=COUNTDOWN),
        queue=QUEUE,
    )

    # Stops being worth running after an hour: a worker picking it up later
    # drops it instead of running it.
    await broker.publish(
        CeleryTask("examples.add", args=[1, 1], expires=EXPIRES),
        queue=QUEUE,
    )

    print(f"published 5 tasks to {QUEUE!r}")


if __name__ == "__main__":
    asyncio.run(app.run())
