"""Publish to a Celery worker and wait for its result.

`broker.request()` is to `broker.publish()` what `AsyncResult.get()` is to
`apply_async`. What comes back is the Celery result envelope, so a failure
arrives as data rather than as an exception — `status` says which happened, and
a Pydantic model turns the envelope into something typed.

Without a result backend the reply is awaited on a temporary AMQP reply queue
(RabbitMQ only). With `result_backend=` it reads `celery-task-meta-<id>` — see
`../06_results/`.

`celery_worker.py` must be running.

Run:
    faststream run faststream_request.py:app       # or: python faststream_request.py
"""

import asyncio
from typing import Any, Literal

from faststream import FastStream
from pydantic import BaseModel

from faststream_celery import CeleryBroker, CeleryTask

BROKER_URL = "amqp://guest:guest@localhost:5672//"
QUEUE = "celery"
REQUEST_TIMEOUT = 15.0


class TaskFailure(BaseModel):
    """The `result` field of a `FAILURE` envelope."""

    exc_type: str
    exc_message: list[Any]
    exc_module: str


class TaskResult(BaseModel):
    """A Celery result message, whichever way it went."""

    task_id: str
    status: Literal["SUCCESS", "FAILURE"]
    result: Any
    traceback: str | None = None

    @property
    def failure(self) -> TaskFailure:
        return TaskFailure.model_validate(self.result)


broker = CeleryBroker(BROKER_URL)
app = FastStream(broker)


async def call(task: CeleryTask) -> TaskResult:
    """Publish a task, wait for the worker, validate what came back."""
    response = await broker.request(task, queue=QUEUE, timeout=REQUEST_TIMEOUT)

    return TaskResult.model_validate(await response.decode())


@app.after_startup
async def roundtrip() -> None:
    added = await call(CeleryTask("examples.add", args=[2, 3]))
    # SUCCESS 5
    print(f"examples.add -> {added.status} {added.result}")

    emailed = await call(
        CeleryTask("examples.send_email", kwargs={"user_id": 42, "urgent": True}),
    )
    # SUCCESS {'status': 'sent', 'user_id': 42, 'locale': 'en'}
    print(f"examples.send_email -> {emailed.status} {emailed.result}")

    failed = await call(CeleryTask("examples.fail", args=["nope"]))
    # FAILURE ValueError: ['nope']
    print(
        f"examples.fail -> {failed.status} "
        f"{failed.failure.exc_type}: {failed.failure.exc_message}",
    )


if __name__ == "__main__":
    asyncio.run(app.run())
