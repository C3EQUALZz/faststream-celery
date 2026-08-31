"""The app under test in `test_app.py`.

Nothing test-specific here: a broker, two handlers, a publisher. The tests run
it in memory.
"""

from typing import Any, Literal

from faststream import FastStream
from pydantic import BaseModel, PositiveInt

from faststream_celery import CeleryBroker, CeleryTask
from faststream_celery.annotations import Logger

BROKER_URL = "amqp://guest:guest@localhost:5672//"
QUEUE = "celery"
AUDIT_QUEUE = "audit"

NoArgs = tuple[()]


class Charge(BaseModel):
    """`kwargs` of `examples.charge`."""

    order_id: PositiveInt
    amount_cents: PositiveInt


class Charged(BaseModel):
    status: Literal["captured"] = "captured"
    order_id: int
    amount_cents: int


broker = CeleryBroker(BROKER_URL)
app = FastStream(broker)

audit = broker.publisher(AUDIT_QUEUE, description="Audit trail of every charge.")


@broker.subscriber(QUEUE, task="examples.charge")
async def charge(args: NoArgs, kwargs: Charge, logger: Logger) -> Charged:
    logger.info("charging order %s", kwargs.order_id)

    await audit.publish(
        CeleryTask("examples.record_charge", kwargs=kwargs.model_dump()),
    )

    return Charged(order_id=kwargs.order_id, amount_cents=kwargs.amount_cents)


@broker.subscriber(QUEUE, task="examples.fail")
async def fail(args: NoArgs, kwargs: dict[str, Any]) -> None:
    raise RuntimeError(kwargs.get("message", "boom"))
