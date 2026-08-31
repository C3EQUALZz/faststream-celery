"""A stock Celery client addressing the prefixed queues.

The prefix is part of the queue name on the wire, so a Celery producer sends to
`imaging.thumbnails`, not to `thumbnails`.

Run (with `faststream_app.py` already running):
    python celery_client.py
"""

import sys

from celery import Celery
from pydantic import BaseModel, Field, PositiveInt

BROKER_URL = "amqp://guest:guest@localhost:5672//"
RESULT_TIMEOUT = 15.0

app = Celery("examples.client", broker=BROKER_URL, backend="rpc://")
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)


class ThumbnailRequest(BaseModel):
    upload_id: str
    size: int = Field(gt=0, le=4096)


class InvoiceRequest(BaseModel):
    order_id: PositiveInt
    amount_cents: PositiveInt


def main() -> int:
    thumbnail = ThumbnailRequest(upload_id="upload-7", size=512)
    result = app.send_task(
        "imaging.make_thumbnail",
        args=[],
        kwargs=thumbnail.model_dump(),
        queue="imaging.thumbnails",
    )
    print("imaging.make_thumbnail ->", result.get(timeout=RESULT_TIMEOUT))
    print("  (the handler also published imaging.export to imaging.exports)")

    invoice = InvoiceRequest(order_id=1234, amount_cents=4999)
    result = app.send_task(
        "billing.issue_invoice",
        args=[],
        kwargs=invoice.model_dump(),
        queue="billing.invoices",
    )
    print("billing.issue_invoice ->", result.get(timeout=RESULT_TIMEOUT))

    return 0


if __name__ == "__main__":
    sys.exit(main())
