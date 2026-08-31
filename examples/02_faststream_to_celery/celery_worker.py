# mypy: disable-error-code="untyped-decorator"
"""faststream-celery publishes, Celery consumes — the Celery side.

A stock Celery worker: `@app.task` functions with ordinary signatures, and
Pydantic models validating what arrives, so the worker rejects a malformed
payload the same way the FastStream side does. Nothing here knows about
FastStream.

Celery ships no type information, so `@app.task` is an untyped decorator.

Run from this directory:
    celery -A celery_worker worker --loglevel=info -Q celery
"""

from celery import Celery
from pydantic import BaseModel, Field, PositiveInt

BROKER_URL = "amqp://guest:guest@localhost:5672//"

app = Celery("examples.worker", broker=BROKER_URL, backend="rpc://")
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    worker_prefetch_multiplier=1,
)


class EmailOptions(BaseModel):
    """The schema the FastStream publisher builds its `kwargs` from."""

    user_id: PositiveInt
    urgent: bool = False
    locale: str = Field(default="en", pattern="^[a-z]{2}$")


@app.task(name="examples.add")
def add(x: int, y: int) -> int:
    print(f"add({x}, {y})")
    return x + y


@app.task(name="examples.send_email")
def send_email(**kwargs: object) -> dict[str, object]:
    """Validate on arrival, then do the work."""
    options = EmailOptions.model_validate(kwargs)

    print(f"emailing user {options.user_id} (urgent={options.urgent})")

    return {"status": "sent", "user_id": options.user_id, "locale": options.locale}


@app.task(name="examples.fail")
def fail(message: str = "boom") -> None:
    raise ValueError(message)


@app.task(name="examples.notify")
def notify(payload: object) -> object:
    """Takes the previous task's result as its first argument.

    Used as a `link=` callback and as a chain step — see `../05_canvas/`.
    """
    print(f"notify({payload!r})")
    return payload
