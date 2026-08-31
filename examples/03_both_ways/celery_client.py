"""The order service: a stock Celery client kicking off the loop.

It calls `examples.order_placed` and waits. The result it prints was assembled
by the FastStream service out of a result a *different* Celery worker produced.

Run (with `faststream_service.py` and `celery_worker.py` already running):
    python celery_client.py
    python celery_client.py declined
    python celery_client.py cancelled
"""

import sys

from celery import Celery
from pydantic import BaseModel, PositiveInt

BROKER_URL = "amqp://guest:guest@localhost:5672//"
ORDERS_QUEUE = "orders"
RESULT_TIMEOUT = 30.0

DECLINED_AMOUNT_CENTS = 250_000

app = Celery("examples.orders", broker=BROKER_URL, backend="rpc://")
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)


class Order(BaseModel):
    """The schema `faststream_service.py` validates on arrival."""

    order_id: PositiveInt
    amount_cents: PositiveInt
    currency: str = "EUR"


def place_order() -> None:
    """Celery → FastStream → Celery → FastStream → Celery, in one call."""
    order = Order(order_id=1234, amount_cents=4999)

    result = app.send_task(
        "examples.order_placed",
        args=[],
        kwargs=order.model_dump(),
        queue=ORDERS_QUEUE,
    )
    print("examples.order_placed ->", result.get(timeout=RESULT_TIMEOUT))


def declined_order() -> None:
    """The Celery worker raises; the FastStream service reports it as data."""
    order = Order(order_id=1235, amount_cents=DECLINED_AMOUNT_CENTS)

    result = app.send_task(
        "examples.order_placed",
        args=[],
        kwargs=order.model_dump(),
        queue=ORDERS_QUEUE,
    )
    print("examples.order_placed (declined) ->", result.get(timeout=RESULT_TIMEOUT))


def cancel_order() -> None:
    """No result at either hop — the refund just happens."""
    app.send_task("examples.order_cancelled", args=[1234], kwargs={}, queue=ORDERS_QUEUE)
    print("examples.order_cancelled -> queued, watch the payments worker")


SCENARIOS = {
    "placed": place_order,
    "declined": declined_order,
    "cancelled": cancel_order,
}


def main(argv: list[str]) -> int:
    for name in argv or list(SCENARIOS):
        scenario = SCENARIOS.get(name)
        if scenario is None:
            print(f"unknown scenario {name!r}; pick from {', '.join(SCENARIOS)}")
            return 1
        scenario()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
