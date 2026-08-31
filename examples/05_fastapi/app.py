"""FastAPI and Celery in one service.

`CeleryRouter` is a `fastapi.APIRouter`, so HTTP routes and Celery task
handlers live in one app, share one dependency graph, and are validated by the
same Pydantic models. `router.lifespan_context` connects and disconnects the
broker with the app.

What this app does:

* consumes `examples.send_email` from the `emails` queue — a stock Celery
  client can call it (`celery_client.py`);
* `POST /emails` enqueues that same task, so an HTTP request becomes a
  background job;
* `GET /sum` publishes `examples.add` to the `math` queue and waits for the
  result of the stock Celery worker in `celery_worker.py`.

Requires the `fastapi` extra:
    pip install "faststream-celery[fastapi]"

Run:
    uvicorn app:app --reload
"""

from collections.abc import AsyncGenerator
from typing import Annotated, Any, Literal

from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, Field, PositiveInt

from faststream_celery import CeleryTask
from faststream_celery.fastapi import CeleryRouter, Logger

BROKER_URL = "amqp://guest:guest@localhost:5672//"
EMAILS_QUEUE = "emails"
MATH_QUEUE = "math"
REQUEST_TIMEOUT = 10.0

NoArgs = tuple[()]


class EmailOptions(BaseModel):
    """`kwargs` of `examples.send_email`, and the body of `POST /emails`."""

    user_id: PositiveInt
    urgent: bool = False
    locale: str = Field(default="en", pattern="^[a-z]{2}$")


class EmailSent(BaseModel):
    status: Literal["sent"] = "sent"
    user_id: int
    delivered_by: str


class Mailer:
    """Stands in for whatever the handler actually talks to."""

    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint

    async def send(self, options: EmailOptions) -> str:
        return f"{self.endpoint}/{options.locale}"


async def get_mailer() -> AsyncGenerator[Mailer]:
    """A FastAPI dependency with teardown, used by a task handler."""
    mailer = Mailer("https://mail.example")
    try:
        yield mailer
    finally:
        # Close the session, return the connection to the pool, ...
        pass


MailerDep = Annotated[Mailer, Depends(get_mailer)]

router = CeleryRouter(BROKER_URL)

# A long-living publisher: bound to a queue, and in the AsyncAPI schema.
emails = router.publisher(EMAILS_QUEUE, description="Outgoing email jobs.")


@router.subscriber(EMAILS_QUEUE, task="examples.send_email")
async def send_email(
    args: NoArgs,
    kwargs: EmailOptions,
    mailer: MailerDep,
    logger: Logger,
) -> EmailSent:
    """A Celery task handler with FastAPI dependencies and Pydantic input."""
    delivered_by = await mailer.send(kwargs)

    logger.info("emailed %s via %s", kwargs.user_id, delivered_by)

    return EmailSent(user_id=kwargs.user_id, delivered_by=delivered_by)


@router.post("/emails", status_code=status.HTTP_202_ACCEPTED)
async def enqueue_email(options: EmailOptions) -> dict[str, str]:
    """HTTP in, Celery task out. The handler above picks it up."""
    await emails.publish(
        CeleryTask("examples.send_email", kwargs=options.model_dump()),
    )
    return {"status": "queued"}


@router.get("/sum")
async def compute_sum(left: int, right: int) -> dict[str, Any]:
    """HTTP in, Celery task out, worker's result back in the response.

    The broker is reached through `router.broker`. The `CeleryBroker`,
    `CeleryMessage` and `Logger` annotations resolve from the FastStream
    context, which exists inside a task handler — an HTTP route has no such
    scope, and FastAPI would ask for a `context__` header instead.

    This holds the HTTP connection open for as long as the task runs, so keep
    it for work measured in milliseconds — or return a task id and poll.
    """
    response = await router.broker.request(
        CeleryTask("examples.add", args=[left, right]),
        queue=MATH_QUEUE,
        timeout=REQUEST_TIMEOUT,
    )
    envelope: Any = await response.decode()

    if envelope["status"] != "SUCCESS":
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=envelope["result"],
        )

    return {"result": envelope["result"]}


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


app = FastAPI(lifespan=router.lifespan_context)
app.include_router(router)
