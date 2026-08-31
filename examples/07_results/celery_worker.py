# mypy: disable-error-code="untyped-decorator"
"""A stock Celery worker on the Redis transport, answering `faststream_reader.py`.

Run from this directory:
    celery -A celery_worker worker --loglevel=info -Q celery
"""

from celery import Celery

REDIS_URL = "redis://localhost:6379/0"

# Must match every other client and worker on this bus.
VISIBILITY_TIMEOUT = 3600

app = Celery("examples.worker", broker=REDIS_URL, backend=REDIS_URL)
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    worker_prefetch_multiplier=1,
    broker_transport_options={"visibility_timeout": VISIBILITY_TIMEOUT},
)


@app.task(name="examples.add")
def add(x: int, y: int) -> int:
    print(f"add({x}, {y})")
    return x + y
