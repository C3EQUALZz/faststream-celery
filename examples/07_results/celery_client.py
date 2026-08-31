"""A stock Celery client reading results from the shared Redis backend.

It also prints the raw `celery-task-meta-<id>` key, to show that what the
FastStream side wrote is the envelope Celery itself writes — same key, same
fields.

Run (with `faststream_consumer.py` already running):
    python celery_client.py
    python celery_client.py fail
"""

import json
import sys

import redis
from celery import Celery
from pydantic import BaseModel

REDIS_URL = "redis://localhost:6379/0"
QUEUE = "celery"
RESULT_TIMEOUT = 15.0

# Must match the FastStream side, or one of them redelivers what the other is
# still working on.
VISIBILITY_TIMEOUT = 3600

KEY_PREFIX = "celery-task-meta-"

app = Celery("examples.client", broker=REDIS_URL, backend=REDIS_URL)
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    broker_transport_options={"visibility_timeout": VISIBILITY_TIMEOUT},
)


class ReportRequest(BaseModel):
    """The schema `faststream_consumer.py` validates on arrival."""

    month: str
    include_drafts: bool = False


def successful_task() -> None:
    request = ReportRequest(month="2026-08")

    result = app.send_task(
        "examples.build_report",
        args=[],
        kwargs=request.model_dump(),
        queue=QUEUE,
    )
    print("examples.build_report ->", result.get(timeout=RESULT_TIMEOUT))

    _show_raw_key(result.id)


def failing_task() -> None:
    result = app.send_task(
        "examples.fail",
        args=[],
        kwargs={"message": "nope"},
        queue=QUEUE,
    )

    try:
        result.get(timeout=RESULT_TIMEOUT)
    except Exception as exc:  # noqa: BLE001 - this is what a caller sees
        print(f"examples.fail -> {result.status}: {type(exc).__name__}: {exc}")

    _show_raw_key(result.id)


def _show_raw_key(task_id: str) -> None:
    """What is actually in Redis, under the key Celery would have used."""
    client = redis.Redis.from_url(REDIS_URL)
    try:
        raw = client.get(f"{KEY_PREFIX}{task_id}")
    finally:
        client.close()

    if raw is None:
        print(f"  {KEY_PREFIX}{task_id}: missing")
        return

    envelope = json.loads(raw)
    print(f"  {KEY_PREFIX}{task_id}:")
    print(f"    status:    {envelope['status']}")
    print(f"    result:    {envelope['result']}")
    print(f"    date_done: {envelope['date_done']}")


SCENARIOS = {
    "report": successful_task,
    "fail": failing_task,
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
