"""Results in a Redis backend, over the Redis transport.

Two independent choices:

* the **transport** — where tasks travel. `redis://...` here instead of AMQP,
  which is a virtual transport kombu emulates with Redis lists.
* the **result backend** — where results are kept. `result_backend=` writes the
  `celery-task-meta-<id>` keys a Celery worker writes, so a Celery client's
  `AsyncResult.get()` reads them without knowing who produced them.

Without a result backend, a result can only travel back on an AMQP reply queue
(`backend="rpc://"` on the Celery side, `broker.request()` on this one). On
Redis there is no reply queue, so a backend is how results work at all.

Run:
    faststream run faststream_consumer.py:app     # or: python faststream_consumer.py
"""

import asyncio
from typing import Literal

from faststream import FastStream
from pydantic import BaseModel, PositiveInt

from faststream_celery import CeleryBroker
from faststream_celery.annotations import CeleryMessage, Logger

REDIS_URL = "redis://localhost:6379/0"
QUEUE = "celery"

# Every client and worker on a Redis bus must agree on this: it is how long a
# delivered-but-unacknowledged message stays invisible to everyone else. If one
# side's is shorter, it redelivers what another is still working on.
VISIBILITY_TIMEOUT = 3600

NoArgs = tuple[()]


class ReportRequest(BaseModel):
    """`kwargs` of `examples.build_report`."""

    month: str
    include_drafts: bool = False


class Report(BaseModel):
    status: Literal["built"] = "built"
    month: str
    rows: PositiveInt


broker = CeleryBroker(
    REDIS_URL,
    result_backend=REDIS_URL,
    transport_options={"visibility_timeout": VISIBILITY_TIMEOUT},
)
app = FastStream(broker)

ROWS_PER_MONTH = 128


@broker.subscriber(QUEUE, task="examples.build_report")
async def build_report(
    args: NoArgs,
    kwargs: ReportRequest,
    message: CeleryMessage,
    logger: Logger,
) -> Report:
    """The return value is written to `celery-task-meta-<task_id>`."""
    logger.info("building the %s report as %s", kwargs.month, message.headers["id"])

    return Report(month=kwargs.month, rows=ROWS_PER_MONTH)


@broker.subscriber(QUEUE, task="examples.fail")
async def fail(args: NoArgs, kwargs: dict[str, str]) -> None:
    """A failure is written to the same key, as a `FAILURE` envelope."""
    raise RuntimeError(kwargs.get("message", "boom"))


if __name__ == "__main__":
    asyncio.run(app.run())
