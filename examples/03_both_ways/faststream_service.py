"""Both directions in one service.

A single Celery call travels the whole loop:

    celery_client.py  --examples.order_placed-->  this service   (queue: orders)
    this service      --examples.charge------->  celery_worker.py (queue: payments)
    celery_worker.py  --result--------------->  this service
    this service      --result--------------->  celery_client.py

So the FastStream side is a Celery *worker* and a Celery *client* at once, and
the stock Celery processes on either end cannot tell.

Run:
    faststream run faststream_service.py:app      # or: python faststream_service.py
"""

import asyncio
from typing import Any, Literal

from faststream import FastStream
from pydantic import BaseModel, PositiveInt

from faststream_celery import CeleryBroker, CeleryTask
from faststream_celery.annotations import Logger

BROKER_URL = "amqp://guest:guest@localhost:5672//"
ORDERS_QUEUE = "orders"
PAYMENTS_QUEUE = "payments"
REQUEST_TIMEOUT = 15.0

NoArgs = tuple[()]


class Order(BaseModel):
    """`kwargs` of `examples.order_placed`, sent by the Celery client."""

    order_id: PositiveInt
    amount_cents: PositiveInt
    currency: Literal["EUR", "USD"] = "EUR"


class Charge(BaseModel):
    """`kwargs` of `examples.charge`, run by the Celery worker."""

    order_id: PositiveInt
    amount_cents: PositiveInt
    currency: str


class OrderProcessed(BaseModel):
    """What the Celery client reads back from `AsyncResult.get()`."""

    order_id: int
    charged: bool
    charge_id: str | None = None
    error: str | None = None


broker = CeleryBroker(BROKER_URL)
app = FastStream(broker)


@broker.subscriber(ORDERS_QUEUE, task="examples.order_placed")
async def order_placed(
    args: NoArgs,
    kwargs: Order,
    logger: Logger,
) -> OrderProcessed:
    """Consume from Celery, call Celery, answer Celery."""
    logger.info("order %s placed", kwargs.order_id)

    charge = Charge(
        order_id=kwargs.order_id,
        amount_cents=kwargs.amount_cents,
        currency=kwargs.currency,
    )

    # Publish to the stock Celery worker and wait for its result — the same
    # thing `AsyncResult.get()` does, from inside a handler.
    response = await broker.request(
        CeleryTask("examples.charge", kwargs=charge.model_dump()),
        queue=PAYMENTS_QUEUE,
        timeout=REQUEST_TIMEOUT,
    )
    envelope: Any = await response.decode()

    if envelope["status"] != "SUCCESS":
        logger.warning("charge failed: %s", envelope["result"])
        return OrderProcessed(
            order_id=kwargs.order_id,
            charged=False,
            error=envelope["result"]["exc_type"],
        )

    return OrderProcessed(
        order_id=kwargs.order_id,
        charged=True,
        charge_id=envelope["result"]["charge_id"],
    )


@broker.subscriber(ORDERS_QUEUE, task="examples.order_cancelled")
async def order_cancelled(
    args: tuple[PositiveInt],
    kwargs: dict[str, Any],
    logger: Logger,
) -> None:
    """Fire and forget in the other direction: no result, no waiting.

    Both `args` and `kwargs` are declared even though only `args` is used:
    a handler with a single body parameter is handed the whole body
    (`{"args": ..., "kwargs": ...}`) instead of that one key. See
    `../04_payload_validation/`.
    """
    (order_id,) = args

    logger.info("order %s cancelled, refunding", order_id)

    await broker.publish(
        CeleryTask("examples.refund", kwargs={"order_id": order_id}),
        queue=PAYMENTS_QUEUE,
    )


if __name__ == "__main__":
    asyncio.run(app.run())
