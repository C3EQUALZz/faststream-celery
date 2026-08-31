# mypy: disable-error-code="untyped-decorator"
"""A stock Celery worker running the deferred tasks `faststream_publisher.py` sends.

Run from this directory:
    celery -A celery_worker worker --loglevel=info -Q celery
"""

import time

from celery import Celery

BROKER_URL = "amqp://guest:guest@localhost:5672//"

app = Celery("examples.worker", broker=BROKER_URL, backend="rpc://")
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    worker_prefetch_multiplier=1,
)


@app.task(name="examples.remind")
def remind(label: str = "") -> str:
    print(f"remind({label!r}) at {time.strftime('%H:%M:%S')}")
    return label
