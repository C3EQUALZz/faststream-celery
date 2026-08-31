"""Splitting an app across routers.

`CeleryRouter` groups subscribers and publishers by domain and applies a prefix
to their queue names, so a module owns its queues without repeating the prefix
in every decorator. Handlers can be registered by decorator or up front, with
`CeleryRoute` — useful when the handler function comes from somewhere else.

Run:
    faststream run faststream_app.py:app          # or: python faststream_app.py
"""

import asyncio
from typing import Any, Literal

from faststream import FastStream
from pydantic import BaseModel, Field, PositiveInt

from faststream_celery import CeleryBroker, CeleryRouter, CeleryTask
from faststream_celery.annotations import Logger
from faststream_celery.broker import CeleryRoute

BROKER_URL = "amqp://guest:guest@localhost:5672//"

NoArgs = tuple[()]


class ThumbnailRequest(BaseModel):
    """`kwargs` of `imaging.make_thumbnail`."""

    upload_id: str
    size: int = Field(gt=0, le=4096)


class Thumbnail(BaseModel):
    status: Literal["ready"] = "ready"
    upload_id: str
    size: int


class InvoiceRequest(BaseModel):
    """`kwargs` of `billing.issue_invoice`."""

    order_id: PositiveInt
    amount_cents: PositiveInt


# --- imaging: queues `imaging.thumbnails` and `imaging.exports` -------------

imaging = CeleryRouter(prefix="imaging.")

# A publisher on the router carries the prefix too: `imaging.exports`.
exports = imaging.publisher("exports", description="Export jobs.")


@imaging.subscriber("thumbnails", task="imaging.make_thumbnail")
async def make_thumbnail(
    args: NoArgs,
    kwargs: ThumbnailRequest,
    logger: Logger,
) -> Thumbnail:
    logger.info("thumbnail %s at %s", kwargs.upload_id, kwargs.size)

    # Hand the upload on to the export queue of the same domain.
    await exports.publish(
        CeleryTask("imaging.export", kwargs={"upload_id": kwargs.upload_id}),
    )

    return Thumbnail(upload_id=kwargs.upload_id, size=kwargs.size)


@imaging.subscriber("exports", task="imaging.export")
async def export(args: NoArgs, kwargs: dict[str, Any], logger: Logger) -> None:
    logger.info("exporting %s", kwargs["upload_id"])


# --- billing: registered up front instead of by decorator ------------------


async def issue_invoice(
    args: NoArgs,
    kwargs: InvoiceRequest,
    logger: Logger,
) -> dict[str, Any]:
    """An ordinary function — the router decides where it is subscribed."""
    logger.info("invoice for order %s", kwargs.order_id)
    return {"invoice_id": f"inv_{kwargs.order_id}", "cents": kwargs.amount_cents}


billing = CeleryRouter(
    prefix="billing.",
    handlers=(
        CeleryRoute(
            issue_invoice,
            "invoices",
            task="billing.issue_invoice",
        ),
    ),
)


broker = CeleryBroker(BROKER_URL, routers=(imaging, billing))
app = FastStream(broker)


if __name__ == "__main__":
    asyncio.run(app.run())
