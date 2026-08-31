"""Dependencies, annotations and a middleware around every task.

Three layers, each in its place:

* **annotations** — what the handler needs from the framework (the raw Celery
  message, a logger, the broker, the kombu connection);
* **dependencies** — what the handler needs from your app, resolved per
  message, with teardown (`Depends`), or attached to a subscriber for their
  side effect alone (`dependencies=`);
* **middlewares** — what wraps every task regardless of the handler: retry
  bookkeeping, an audit log, a metric, a redelivery guard.

Run:
    faststream run faststream_app.py:app          # or: python faststream_app.py
"""

import asyncio
import time
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any, Literal

from faststream import Depends, FastStream
from faststream.exceptions import RejectMessage
from faststream.message import StreamMessage
from faststream.middlewares import BaseMiddleware
from pydantic import BaseModel, PositiveInt

from faststream_celery import CeleryBroker
from faststream_celery.annotations import CeleryMessage, Logger

BROKER_URL = "amqp://guest:guest@localhost:5672//"
QUEUE = "celery"
MAX_RETRIES = 3
CHARGE_LIMIT_CENTS = 100_000

NoArgs = tuple[()]


class Charge(BaseModel):
    """`kwargs` of `examples.charge`."""

    order_id: PositiveInt
    amount_cents: PositiveInt


class Charged(BaseModel):
    status: Literal["captured"] = "captured"
    order_id: int
    attempt: int


class TaskAuditMiddleware(BaseMiddleware[Any, Any]):
    """Times every task and logs how it ended.

    A broker middleware sees the message before the parser and the handler,
    and sees whatever the handler raised — which is what makes it the right
    place for anything that must not be repeated in each handler.
    """

    async def consume_scope(
        self,
        call_next: Callable[[Any], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        task = msg.headers.get("task", "<not a celery task>")
        started = time.monotonic()

        try:
            result = await call_next(msg)

        except Exception as exc:
            elapsed = time.monotonic() - started
            print(f"audit: {task} failed in {elapsed:.3f}s: {type(exc).__name__}")
            raise

        print(f"audit: {task} ok in {time.monotonic() - started:.3f}s")
        return result


class RetryLimitMiddleware(BaseMiddleware[Any, Any]):
    """Reject a task that has already been retried too often.

    Celery counts retries in the `retries` header; rejecting instead of
    running keeps a poison task from cycling forever.
    """

    async def consume_scope(
        self,
        call_next: Callable[[Any], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        retries = msg.headers.get("retries") or 0

        if retries > MAX_RETRIES:
            print(f"giving up on {msg.headers.get('task')} after {retries} retries")
            raise RejectMessage

        return await call_next(msg)


class Ledger:
    """Stands in for whatever the handler actually writes to."""

    def __init__(self) -> None:
        self.entries: list[tuple[int, int]] = []

    def record(self, order_id: int, cents: int) -> None:
        self.entries.append((order_id, cents))


async def get_ledger() -> AsyncGenerator[Ledger]:
    """A per-message dependency with teardown."""
    ledger = Ledger()
    try:
        yield ledger
    finally:
        # Commit the transaction, return the connection to the pool, ...
        pass


async def within_limit(kwargs: Charge) -> None:
    """A dependency used for its side effect: it checks and returns nothing.

    Attached with `dependencies=`, it runs before the handler and can fail the
    task — the place for a policy check no handler should be able to forget.
    Pydantic covers the shape of the payload; this covers what the shape
    cannot say.
    """
    if kwargs.amount_cents > CHARGE_LIMIT_CENTS:
        msg = f"a charge over {CHARGE_LIMIT_CENTS} needs a review"
        raise PermissionError(msg)


broker = CeleryBroker(
    BROKER_URL,
    # Applied to every subscriber and publisher on this broker.
    middlewares=(TaskAuditMiddleware, RetryLimitMiddleware),
)
app = FastStream(broker)


@broker.subscriber(
    QUEUE,
    task="examples.charge",
    dependencies=(Depends(within_limit),),
)
async def charge(
    args: NoArgs,
    kwargs: Charge,
    ledger: Ledger = Depends(get_ledger),
) -> Charged:
    ledger.record(kwargs.order_id, kwargs.amount_cents)

    return Charged(order_id=kwargs.order_id, attempt=1)


@broker.subscriber(QUEUE, task="examples.inspect")
async def inspect(
    args: NoArgs,
    kwargs: dict[str, Any],
    message: CeleryMessage,
    logger: Logger,
) -> dict[str, Any]:
    """Everything Celery put in the headers is readable here."""
    logger.info("inspecting %s", message.headers["task"])

    # Note the key is `task_name`, not `task`: a result whose top-level dict
    # has a `task` key looks like a Celery protocol v1 task body to a parser
    # reading it back, and would be decoded as one.
    return {
        "task_name": message.headers["task"],
        "task_id": message.headers["id"],
        "retries": message.headers.get("retries"),
        "root_id": message.headers.get("root_id"),
        "parent_id": message.headers.get("parent_id"),
        "eta": message.headers.get("eta"),
        "expires": message.headers.get("expires"),
        "reply_to": message.reply_to,
        "correlation_id": message.correlation_id,
    }


if __name__ == "__main__":
    asyncio.run(app.run())
