# mypy: disable-error-code="untyped-decorator"
"""A real Celery application used by the integration tests.

Run as a worker subprocess by the ``celery_worker`` fixture, so it must be
importable on its own and configure itself from the environment.

Celery ships no type information, so ``@app.task`` is an untyped decorator.
"""

import json
import os
from pathlib import Path
from typing import Any

from celery import Celery

BROKER_URL = os.environ.get("CELERY_TEST_BROKER_URL", "memory://")
RESULT_BACKEND = os.environ.get("CELERY_TEST_RESULT_BACKEND", "rpc://")
LOG_PATH = os.environ.get("CELERY_TEST_LOG", "")

app = Celery(
    "faststream_celery_tests",
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
)
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_always_eager=False,
    broker_connection_retry_on_startup=True,
    # Keep the worker from prefetching the whole queue in a solo pool.
    worker_prefetch_multiplier=1,
)


def record(payload: Any) -> None:
    """Append a payload to the log file the test reads back."""
    if not LOG_PATH:
        return

    with Path(LOG_PATH).open("a", encoding="utf-8") as log:
        log.write(json.dumps(payload) + "\n")


@app.task(name="tests.echo")
def echo(payload: Any = None) -> Any:
    """Record and return the payload — proves the worker ran our task."""
    record({"task": "tests.echo", "payload": payload})
    return payload


@app.task(name="tests.add")
def add(x: int, y: int) -> int:
    """Return a value the caller can assert on over the result backend."""
    record({"task": "tests.add", "args": [x, y]})
    return x + y


@app.task(name="tests.fail")
def fail(message: str = "boom") -> None:
    """Always raise, so failure propagation can be tested."""
    record({"task": "tests.fail", "message": message})
    raise ValueError(message)


def read_log(path: "str | os.PathLike[str]") -> list[Any]:
    """Read every payload the worker has recorded so far."""
    log_file = Path(path)
    if not log_file.exists():
        return []

    return [
        json.loads(line)
        for line in log_file.read_text(encoding="utf-8").splitlines()
        if line
    ]
