"""A stock Celery client exercising each validation form, valid and invalid.

Run (with `faststream_consumer.py` already running):
    python celery_client.py           # every call
    python celery_client.py invalid   # only the ones that must be rejected
"""

import sys
from typing import Any

from celery import Celery
from pydantic import BaseModel, Field, PositiveInt

BROKER_URL = "amqp://guest:guest@localhost:5672//"
QUEUE = "celery"
RESULT_TIMEOUT = 15.0

OVERSIZED_WIDTH = 99_999

app = Celery("examples.client", broker=BROKER_URL, backend="rpc://")
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)


class EmailOptions(BaseModel):
    """Shared with the consumer: one schema, validated on both ends."""

    user_id: PositiveInt
    urgent: bool = False
    locale: str = Field(default="en", pattern="^[a-z]{2}$")


def valid_calls() -> None:
    """One call per validation form, all of them accepted."""
    options = EmailOptions(user_id=42, urgent=True)
    _call("form 1", "examples.send_email", [], options.model_dump())

    _call("form 2", "examples.transfer", [1, 2], {"amount_cents": 4999})

    _call(
        "form 3",
        "examples.resize_image",
        [],
        {"upload_id": "upload-7", "width": 800, "height": 600},
    )


def invalid_calls() -> None:
    """The same tasks with payloads the models refuse."""
    # `extra="forbid"`: an unexpected keyword is a caller bug.
    _call("unknown keyword", "examples.send_email", [], {"user_id": 42, "urgnet": True})

    # `PositiveInt`.
    _call("negative id", "examples.send_email", [], {"user_id": -1})

    # `args` is declared as an empty tuple, so positional arguments are refused.
    _call("unexpected args", "examples.send_email", [42], {"user_id": 42})

    # A cross-field rule on the whole body.
    _call("same account", "examples.transfer", [1, 1], {"amount_cents": 4999})

    # A per-parameter bound.
    _call(
        "oversized",
        "examples.resize_image",
        [],
        {"upload_id": "upload-7", "width": OVERSIZED_WIDTH, "height": 600},
    )


def _call(label: str, task: str, args: list[Any], kwargs: dict[str, Any]) -> None:
    result = app.send_task(task, args=args, kwargs=kwargs, queue=QUEUE)

    try:
        value = result.get(timeout=RESULT_TIMEOUT)
    except Exception as exc:  # noqa: BLE001 - this is what a caller sees
        first_line = str(exc).splitlines()[0]
        print(f"{label}: {task} -> {result.status}: {type(exc).__name__}: {first_line}")
    else:
        print(f"{label}: {task} -> {value}")


SCENARIOS = {
    "valid": valid_calls,
    "invalid": invalid_calls,
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
