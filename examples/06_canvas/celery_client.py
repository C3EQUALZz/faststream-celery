"""A stock Celery client starting a canvas that runs through FastStream.

Built with Celery's own `chain` and `signature`, referencing our handler by
task name and queue. Celery does not know — and does not need to know — that
`examples.enrich` runs on a FastStream service.

Run (with `celery_worker.py` and `faststream_middle.py` already running):
    python celery_client.py            # chain: parse -> enrich -> notify
    python celery_client.py callback   # enrich with a link= callback
    python celery_client.py errback    # a failing enrich, with link_error=
"""

import sys

from celery import Celery, chain, signature

BROKER_URL = "amqp://guest:guest@localhost:5672//"
CELERY_QUEUE = "celery"
OUR_QUEUE = "enrich"
RESULT_TIMEOUT = 30.0

app = Celery("examples.client", broker=BROKER_URL, backend="rpc://")
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)


def run_chain() -> None:
    """Three links, the middle one on the FastStream service."""
    workflow = chain(
        signature("examples.parse", args=["the quick brown fox"], queue=CELERY_QUEUE),
        signature("examples.enrich", queue=OUR_QUEUE),
        signature("examples.notify", queue=CELERY_QUEUE),
    )
    result = workflow.apply_async()

    # `.get()` on a chain waits for its last link, which runs on the Celery
    # worker — and returns what our handler passed to it.
    print("chain ->", result.get(timeout=RESULT_TIMEOUT))


def run_callback() -> None:
    """`link=` on our handler: the callback runs on the Celery worker."""
    task = signature(
        "examples.enrich",
        args=[{"document_id": "from-celery", "words": 20}],
        queue=OUR_QUEUE,
    )
    task.link(signature("examples.notify", queue=CELERY_QUEUE))

    result = task.apply_async()
    print("enrich + link ->", result.get(timeout=RESULT_TIMEOUT))


def run_errback() -> None:
    """`link_error=`: our handler raises, and the errback runs on Celery."""
    task = signature(
        "examples.enrich",
        args=[{"document_id": "doomed", "words": 0}],
        queue=OUR_QUEUE,
    )
    task.link_error(signature("examples.alert", queue=CELERY_QUEUE))

    result = task.apply_async()
    try:
        result.get(timeout=RESULT_TIMEOUT)
    except Exception as exc:  # noqa: BLE001 - this is what a caller sees
        print(f"enrich + link_error -> {result.status}: {type(exc).__name__}")
    print("watch the worker log for alert(task_id=...)")


SCENARIOS = {
    "chain": run_chain,
    "callback": run_callback,
    "errback": run_errback,
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
