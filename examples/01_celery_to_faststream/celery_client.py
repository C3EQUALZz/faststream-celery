"""Celery publishes, FastStream consumes — the Celery side.

A stock Celery client. It registers no tasks: `send_task` addresses a worker by
task name and queue, which is all a producer needs. The worker on the other end
is `faststream_consumer.py`, and nothing here knows that.

The outgoing payload is built from a Pydantic model too, so producer and
consumer agree on one schema instead of two hand-written dicts.

Run (with `faststream_consumer.py` already running):
    python celery_client.py            # every scenario
    python celery_client.py bad        # a payload that fails validation
    python celery_client.py unknown    # a task no handler claims
"""

import sys
from typing import Any

from celery import Celery
from pydantic import BaseModel, Field, PositiveInt

BROKER_URL = "amqp://guest:guest@localhost:5672//"
QUEUE = "celery"
RESULT_TIMEOUT = 15.0
COUNTDOWN = 3.0

# `rpc://` keeps results on a per-client AMQP reply queue — no extra service to
# run. Use `redis://localhost:6379/0` on both sides for a shared backend
# (see ../06_results/).
app = Celery("examples.client", broker=BROKER_URL, backend="rpc://")
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)


class EmailOptions(BaseModel):
    """The same schema `faststream_consumer.py` validates on arrival."""

    user_id: PositiveInt
    urgent: bool = False
    locale: str = Field(default="en", pattern="^[a-z]{2}$")


def positional_args() -> None:
    """A task with positional arguments, and its result."""
    result = app.send_task("examples.add", args=[2, 3], kwargs={}, queue=QUEUE)
    print("examples.add(2, 3) ->", result.get(timeout=RESULT_TIMEOUT))


def validated_kwargs() -> None:
    """A task whose keyword arguments come from a shared Pydantic model."""
    options = EmailOptions(user_id=42, urgent=True, locale="ru")

    result = app.send_task(
        "examples.send_email",
        args=[],
        kwargs=options.model_dump(),
        queue=QUEUE,
    )
    print("examples.send_email ->", result.get(timeout=RESULT_TIMEOUT))


def mixed_args() -> None:
    """Positional and keyword arguments in one call."""
    result = app.send_task(
        "examples.resize_image",
        args=["upload-7"],
        kwargs={"width": 800, "height": 600},
        queue=QUEUE,
    )
    print("examples.resize_image ->", result.get(timeout=RESULT_TIMEOUT))


def fire_and_forget() -> None:
    """No result asked for; the client does not wait."""
    app.send_task("examples.send_email", args=[], kwargs={"user_id": 1}, queue=QUEUE)
    print("examples.send_email -> queued, not waiting")


def deferred() -> None:
    """`countdown` is honoured by the FastStream side as by a Celery worker."""
    result = app.send_task(
        "examples.add",
        args=[10, 5],
        kwargs={},
        queue=QUEUE,
        countdown=COUNTDOWN,
    )
    print(f"examples.add in {COUNTDOWN}s ->", result.get(timeout=RESULT_TIMEOUT))


def failing_task() -> None:
    """A handler that raises: `FAILURE`, and `get()` re-raises the exception."""
    result = app.send_task(
        "examples.fail",
        args=[],
        kwargs={"message": "nope"},
        queue=QUEUE,
    )
    _show_failure("examples.fail", result)


def bad_payload() -> None:
    """`user_id=-1` fails the model, so the task fails before any work."""
    result = app.send_task(
        "examples.send_email",
        args=[],
        kwargs={"user_id": -1},
        queue=QUEUE,
    )
    _show_failure("examples.send_email(user_id=-1)", result)


def unknown_task() -> None:
    """A task name no handler claims lands on the catch-all subscriber."""
    app.send_task("examples.not_implemented", args=[], kwargs={}, queue=QUEUE)
    print("examples.not_implemented -> queued, watch the consumer's warning")


def _show_failure(label: str, result: Any) -> None:
    try:
        result.get(timeout=RESULT_TIMEOUT)
    except Exception as exc:  # noqa: BLE001 - this is what a caller sees
        print(f"{label} -> {result.status}: {type(exc).__name__}: {exc}")
    else:
        print(f"{label} -> unexpectedly succeeded")


SCENARIOS = {
    "args": positional_args,
    "kwargs": validated_kwargs,
    "mixed": mixed_args,
    "nowait": fire_and_forget,
    "later": deferred,
    "fail": failing_task,
    "bad": bad_payload,
    "unknown": unknown_task,
}


def main(argv: list[str]) -> int:
    names = argv or list(SCENARIOS)

    for name in names:
        scenario = SCENARIOS.get(name)
        if scenario is None:
            print(f"unknown scenario {name!r}; pick from {', '.join(SCENARIOS)}")
            return 1
        scenario()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
