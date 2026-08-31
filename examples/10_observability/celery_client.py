"""A stock Celery client producing traffic to trace and to count.

Run (with `faststream_app.py` already running):
    python celery_client.py
    python celery_client.py 50        # 50 of each task
"""

import sys

from celery import Celery
from pydantic import BaseModel, PositiveInt

BROKER_URL = "amqp://guest:guest@localhost:5672//"
QUEUE = "celery"
DEFAULT_COUNT = 5

app = Celery("examples.client", broker=BROKER_URL, backend="rpc://")
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)


class Charge(BaseModel):
    order_id: PositiveInt
    amount_cents: PositiveInt


def main(argv: list[str]) -> int:
    count = int(argv[0]) if argv else DEFAULT_COUNT

    for index in range(count):
        charge = Charge(order_id=index + 1, amount_cents=1000 + index)
        app.send_task(
            "examples.charge",
            args=[],
            kwargs=charge.model_dump(),
            queue=QUEUE,
        )
        app.send_task("examples.refund", args=[index + 1], kwargs={}, queue=QUEUE)

    print(f"sent {count} charges and {count} refunds")
    print("spans: the app's stdout; metrics: http://localhost:9000/metrics")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
