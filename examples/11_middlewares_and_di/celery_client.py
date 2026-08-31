"""A stock Celery client exercising the middlewares and the dependencies.

Run (with `faststream_app.py` already running):
    python celery_client.py
    python celery_client.py over_limit
    python celery_client.py retried
"""

import sys
from typing import Any

from celery import Celery
from pydantic import BaseModel, PositiveInt

BROKER_URL = "amqp://guest:guest@localhost:5672//"
QUEUE = "celery"
RESULT_TIMEOUT = 15.0

OVER_LIMIT_CENTS = 250_000
TOO_MANY_RETRIES = 9

app = Celery("examples.client", broker=BROKER_URL, backend="rpc://")
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)


class Charge(BaseModel):
    order_id: PositiveInt
    amount_cents: PositiveInt


def ordinary_charge() -> None:
    charge = Charge(order_id=1234, amount_cents=4999)
    result = app.send_task(
        "examples.charge",
        args=[],
        kwargs=charge.model_dump(),
        queue=QUEUE,
    )
    print("examples.charge ->", result.get(timeout=RESULT_TIMEOUT))


def over_limit() -> None:
    """The `dependencies=` check fails the task before the handler runs."""
    charge = Charge(order_id=1235, amount_cents=OVER_LIMIT_CENTS)
    result = app.send_task(
        "examples.charge",
        args=[],
        kwargs=charge.model_dump(),
        queue=QUEUE,
    )
    _show_failure("examples.charge (over limit)", result)


def inspect_headers() -> None:
    """What the handler sees of Celery's own metadata."""
    result = app.send_task("examples.inspect", args=[], kwargs={}, queue=QUEUE)
    print("examples.inspect ->", result.get(timeout=RESULT_TIMEOUT))


def already_retried() -> None:
    """A high `retries` header: the retry-limit middleware rejects the task.

    Rejected means gone — no result will arrive, so nothing waits for one.
    """
    app.send_task(
        "examples.charge",
        args=[],
        kwargs={"order_id": 1236, "amount_cents": 100},
        queue=QUEUE,
        retries=TOO_MANY_RETRIES,
    )
    print("examples.charge with retries=9 -> rejected, watch the app log")


def _show_failure(label: str, result: Any) -> None:
    try:
        result.get(timeout=RESULT_TIMEOUT)
    except Exception as exc:  # noqa: BLE001 - this is what a caller sees
        print(f"{label} -> {result.status}: {type(exc).__name__}: {exc}")
    else:
        print(f"{label} -> unexpectedly succeeded")


SCENARIOS = {
    "charge": ordinary_charge,
    "over_limit": over_limit,
    "inspect": inspect_headers,
    "retried": already_retried,
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
