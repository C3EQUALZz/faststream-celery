# mypy: disable-error-code="untyped-decorator"
"""The payments service: a stock Celery worker on queue `payments`.

It is called by `faststream_service.py` and knows nothing about it.

Run from this directory:
    celery -A celery_worker worker --loglevel=info -Q payments
"""

from celery import Celery
from pydantic import BaseModel, PositiveInt

BROKER_URL = "amqp://guest:guest@localhost:5672//"

# Cards ending in this many cents are declined, to show a failure travelling
# back through the FastStream service to the original Celery caller.
DECLINE_ABOVE_CENTS = 100_000

app = Celery("examples.payments", broker=BROKER_URL, backend="rpc://")
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    worker_prefetch_multiplier=1,
)


class Charge(BaseModel):
    """What `faststream_service.py` sends as `kwargs`."""

    order_id: PositiveInt
    amount_cents: PositiveInt
    currency: str


@app.task(name="examples.charge")
def charge(**kwargs: object) -> dict[str, object]:
    request = Charge.model_validate(kwargs)

    print(f"charging {request.amount_cents} {request.currency} for {request.order_id}")

    if request.amount_cents > DECLINE_ABOVE_CENTS:
        msg = f"card declined for order {request.order_id}"
        raise ValueError(msg)

    return {"charge_id": f"ch_{request.order_id}", "captured": True}


@app.task(name="examples.refund")
def refund(order_id: int) -> str:
    print(f"refunding order {order_id}")
    return f"rf_{order_id}"
