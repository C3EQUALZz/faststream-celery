"""The other side of the backend: publish from FastStream, read the result.

`broker.request()` with a `result_backend` configured polls
`celery-task-meta-<id>` exactly as `AsyncResult.get()` does — so this works on
Redis, where there is no AMQP reply queue to wait on.

Run a Celery worker (`celery_worker.py`) to have someone answer:
    celery -A celery_worker worker --loglevel=info -Q celery

Run:
    faststream run faststream_reader.py:app       # or: python faststream_reader.py
"""

import asyncio
from typing import Any, Literal

from faststream import FastStream
from pydantic import BaseModel

from faststream_celery import CeleryBroker, CeleryTask

REDIS_URL = "redis://localhost:6379/0"
QUEUE = "celery"
VISIBILITY_TIMEOUT = 3600
REQUEST_TIMEOUT = 15.0


class TaskResult(BaseModel):
    """A Celery result envelope, as stored in the backend."""

    task_id: str
    status: Literal["SUCCESS", "FAILURE"]
    result: Any
    traceback: str | None = None


broker = CeleryBroker(
    REDIS_URL,
    result_backend=REDIS_URL,
    transport_options={"visibility_timeout": VISIBILITY_TIMEOUT},
)
app = FastStream(broker)


@app.after_startup
async def read_results() -> None:
    response = await broker.request(
        CeleryTask("examples.add", args=[2, 3]),
        queue=QUEUE,
        timeout=REQUEST_TIMEOUT,
    )
    envelope = TaskResult.model_validate(await response.decode())

    print(f"examples.add -> {envelope.status} {envelope.result}")


if __name__ == "__main__":
    asyncio.run(app.run())
