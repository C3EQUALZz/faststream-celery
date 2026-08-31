# mypy: disable-error-code="untyped-decorator"
"""The worker behind `GET /sum`: a stock Celery worker on queue `math`.

Run from this directory:
    celery -A celery_worker worker --loglevel=info -Q math
"""

from celery import Celery

BROKER_URL = "amqp://guest:guest@localhost:5672//"

app = Celery("examples.math", broker=BROKER_URL, backend="rpc://")
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    worker_prefetch_multiplier=1,
)


@app.task(name="examples.add")
def add(x: int, y: int) -> int:
    print(f"add({x}, {y})")
    return x + y
