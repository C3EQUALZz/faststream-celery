# mypy: disable-error-code="untyped-decorator"
"""The Celery ends of the canvas: the task before ours and the ones after.

Run from this directory:
    celery -A celery_worker worker --loglevel=info -Q celery
"""

from typing import Any

from celery import Celery

BROKER_URL = "amqp://guest:guest@localhost:5672//"

app = Celery("examples.canvas", broker=BROKER_URL, backend="rpc://")
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    worker_prefetch_multiplier=1,
)


@app.task(name="examples.parse")
def parse(raw: str) -> dict[str, Any]:
    """First link of the chain. Its return value is the next task's argument."""
    print(f"parse({raw!r})")
    return {"document_id": raw, "words": len(raw.split())}


@app.task(name="examples.notify")
def notify(payload: Any) -> Any:
    """Last link, and also used as a `link=` callback.

    A callback and a chain step are both called with the previous task's
    result as their first argument.
    """
    print(f"notify({payload!r})")
    return payload


@app.task(name="examples.alert")
def alert(task_id: str) -> str:
    """An errback. Celery hands it the failed task's id, not the exception."""
    print(f"alert(task_id={task_id!r})")
    return task_id
