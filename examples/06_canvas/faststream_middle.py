"""A FastStream handler in the middle of a Celery canvas.

Chains, callbacks and errbacks are carried inside the task body (Celery's
`embed` slot). This broker reads them and, once a handler finishes, publishes
whatever was waiting there — the same dispatch `celery.app.trace` does. So a
chain can run

    examples.parse (Celery)  ->  examples.enrich (here)  ->  examples.notify (Celery)

and no side needs to know where the next link runs.

The handler validates its input with a Pydantic model: in a chain, the previous
task's result arrives as the first positional argument, which is exactly the
kind of contract worth checking.

Run:
    faststream run faststream_middle.py:app       # or: python faststream_middle.py
"""

import asyncio
from typing import Any

from faststream import FastStream
from pydantic import BaseModel, NonNegativeInt

from faststream_celery import CeleryBroker, CeleryTask, signature
from faststream_celery.annotations import CeleryMessage, Logger

BROKER_URL = "amqp://guest:guest@localhost:5672//"
OUR_QUEUE = "enrich"
CELERY_QUEUE = "celery"


class ParsedDocument(BaseModel):
    """What `examples.parse` returns, and so what arrives here."""

    document_id: str
    words: NonNegativeInt


class EnrichedDocument(BaseModel):
    """What travels on to the next link."""

    document_id: str
    words: int
    reading_seconds: int


broker = CeleryBroker(BROKER_URL)
app = FastStream(broker)

WORDS_PER_SECOND = 4


@broker.subscriber(OUR_QUEUE, task="examples.enrich")
async def enrich(
    args: tuple[ParsedDocument],
    kwargs: dict[str, Any],
    message: CeleryMessage,
    logger: Logger,
) -> EnrichedDocument:
    """The middle link.

    The return value is what the next link — or the callback — is called with.
    A failure here fires the errbacks instead.
    """
    (document,) = args

    logger.info(
        "enriching %s (root_id=%s, parent_id=%s)",
        document.document_id,
        message.headers.get("root_id"),
        message.headers.get("parent_id"),
    )

    if document.words == 0:
        msg = "nothing to enrich"
        raise ValueError(msg)

    return EnrichedDocument(
        document_id=document.document_id,
        words=document.words,
        reading_seconds=document.words // WORDS_PER_SECOND,
    )


@app.after_startup
async def publish_our_own_canvas() -> None:
    """The same three-link chain, started from this side.

    Celery serializes a chain **reversed**: the next step is the last element
    and the rest travels on inside it. So `parse -> enrich -> notify` is
    written `chain=[notify, enrich]`.
    """
    await broker.publish(
        CeleryTask(
            "examples.parse",
            args=["a document from faststream"],
            chain=[
                signature("examples.notify", options={"queue": CELERY_QUEUE}),
                signature("examples.enrich", options={"queue": OUR_QUEUE}),
            ],
        ),
        queue=CELERY_QUEUE,
    )

    # A callback: it runs on the Celery worker once our handler succeeds, with
    # the handler's return value as its argument.
    await broker.publish(
        CeleryTask(
            "examples.enrich",
            args=[{"document_id": "with-a-callback", "words": 12}],
            link=[signature("examples.notify", options={"queue": CELERY_QUEUE})],
        ),
        queue=OUR_QUEUE,
    )

    # An errback: `words=0` makes the handler raise, and the errback runs with
    # the failed task's id — which is why `correlation_id` is worth setting.
    await broker.publish(
        CeleryTask(
            "examples.enrich",
            args=[{"document_id": "doomed", "words": 0}],
            link_error=[signature("examples.alert", options={"queue": CELERY_QUEUE})],
        ),
        queue=OUR_QUEUE,
        correlation_id="doomed-task-id",
    )

    # `immutable=True` is Celery's `.si()`: the step refuses the previous
    # result and is called with its own arguments only.
    await broker.publish(
        CeleryTask(
            "examples.enrich",
            args=[{"document_id": "immutable-callback", "words": 8}],
            link=[
                signature(
                    "examples.notify",
                    args=["done"],
                    options={"queue": CELERY_QUEUE},
                    immutable=True,
                ),
            ],
        ),
        queue=OUR_QUEUE,
    )


if __name__ == "__main__":
    asyncio.run(app.run())
