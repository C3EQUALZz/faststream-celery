"""A stock Celery client scheduling tasks for the FastStream consumer.

Run (with `faststream_consumer.py` already running):
    python celery_client.py
    python celery_client.py eta
    python celery_client.py expired
"""

import sys
import time
from datetime import datetime, timedelta, timezone

from celery import Celery
from pydantic import BaseModel

BROKER_URL = "amqp://guest:guest@localhost:5672//"
QUEUE = "celery"
RESULT_TIMEOUT = 30.0
COUNTDOWN = 5.0
ETA_SECONDS = 8.0

app = Celery("examples.client", broker=BROKER_URL, backend="rpc://")
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    enable_utc=True,
)


class Reminder(BaseModel):
    """The schema `faststream_consumer.py` validates on arrival."""

    user_id: int
    label: str


def with_countdown() -> None:
    """`countdown` is seconds from now, and becomes an absolute `eta`."""
    reminder = Reminder(user_id=42, label=f"countdown {COUNTDOWN}s")

    started = time.monotonic()
    result = app.send_task(
        "examples.remind",
        args=[],
        kwargs=reminder.model_dump(),
        queue=QUEUE,
        countdown=COUNTDOWN,
    )
    value = result.get(timeout=RESULT_TIMEOUT)

    print(f"countdown -> {value} (waited {time.monotonic() - started:.1f}s)")


def with_eta() -> None:
    """`eta` is an absolute, timezone-aware moment."""
    due = datetime.now(timezone.utc) + timedelta(seconds=ETA_SECONDS)
    reminder = Reminder(user_id=42, label=f"eta {due.isoformat()}")

    started = time.monotonic()
    result = app.send_task(
        "examples.remind",
        args=[],
        kwargs=reminder.model_dump(),
        queue=QUEUE,
        eta=due,
    )
    value = result.get(timeout=RESULT_TIMEOUT)

    print(f"eta -> {value} (waited {time.monotonic() - started:.1f}s)")


def already_expired() -> None:
    """An expiry in the past: the task is dropped, never handled.

    No result will ever arrive, so nothing waits for one here.
    """
    reminder = Reminder(user_id=42, label="too late")

    app.send_task(
        "examples.remind",
        args=[],
        kwargs=reminder.model_dump(),
        queue=QUEUE,
        expires=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    print("expired -> queued and dropped; the consumer logs nothing for it")


SCENARIOS = {
    "countdown": with_countdown,
    "eta": with_eta,
    "expired": already_expired,
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
