"""A stock Celery client calling the FastAPI service's task handler.

The same handler `POST /emails` enqueues to is reachable from any Celery
producer — the HTTP app is a Celery worker too.

Run (with `uvicorn app:app` already running):
    python celery_client.py
"""

import sys

from celery import Celery
from pydantic import BaseModel, Field, PositiveInt

BROKER_URL = "amqp://guest:guest@localhost:5672//"
EMAILS_QUEUE = "emails"
RESULT_TIMEOUT = 15.0

app = Celery("examples.client", broker=BROKER_URL, backend="rpc://")
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)


class EmailOptions(BaseModel):
    """The schema `app.py` validates on arrival."""

    user_id: PositiveInt
    urgent: bool = False
    locale: str = Field(default="en", pattern="^[a-z]{2}$")


def main() -> int:
    options = EmailOptions(user_id=42, urgent=True, locale="ru")

    result = app.send_task(
        "examples.send_email",
        args=[],
        kwargs=options.model_dump(),
        queue=EMAILS_QUEUE,
    )
    print("examples.send_email ->", result.get(timeout=RESULT_TIMEOUT))

    return 0


if __name__ == "__main__":
    sys.exit(main())
