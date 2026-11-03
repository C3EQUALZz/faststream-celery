# faststream-celery

[![PyPI](https://img.shields.io/pypi/v/faststream-celery.svg)](https://pypi.org/project/faststream-celery/)
[![Python](https://img.shields.io/pypi/pyversions/faststream-celery.svg)](https://pypi.org/project/faststream-celery/)
[![License](https://img.shields.io/pypi/l/faststream-celery.svg)](./LICENSE)
[![Downloads](https://img.shields.io/pypi/dm/faststream-celery.svg)](https://pypi.org/project/faststream-celery/)

A [FastStream](https://github.com/ag2ai/faststream) broker that speaks Celery.

Write async FastStream handlers that consume the tasks your Celery producers
already send, and publish tasks your Celery workers already run. Both sides keep
working while you move services across one at a time — nothing has to be
migrated in a big bang, and neither side needs to know the other changed.

```python
from faststream import FastStream
from faststream_celery import CeleryBroker

broker = CeleryBroker("amqp://guest:guest@localhost:5672//")
app = FastStream(broker)


@broker.subscriber("celery", task="proj.tasks.send_email")
async def send_email(args: list, kwargs: dict) -> str:
    return "sent"
```

That handler now serves `app.send_task("proj.tasks.send_email", ...)` from any
Celery client, result included.

## Install

```bash
pip install faststream-celery
```

Extras, all optional:

```bash
pip install "faststream-celery[redis]"       # Redis transport and result backend
pip install "faststream-celery[fastapi]"     # FastAPI router
pip install "faststream-celery[otel]"        # OpenTelemetry tracing
pip install "faststream-celery[prometheus]"  # Prometheus metrics
```

## Consuming tasks

`task=` picks the Celery task a handler serves. Several handlers share one
queue, the way Celery routes:

```python
@broker.subscriber("celery", task="proj.tasks.send_email")
async def send_email(args: list, kwargs: dict) -> None: ...


@broker.subscriber("celery", task="proj.tasks.resize")
async def resize(args: list, kwargs: dict) -> None: ...


@broker.subscriber("celery")  # anything the two above do not claim
async def fallback(args: list, kwargs: dict) -> None: ...
```

For each message, an exact `task=` match takes precedence over the fallback
without `task=`. Only one subscriber receives the message, in both production
and `TestCeleryBroker`. A custom filter rejecting the selected exact subscriber
does not retry the fallback. A task nobody claims is rejected, the way a Celery
worker rejects one it does not know.

Returning a value is enough to answer the caller: with an AMQP `reply_to` or a
result backend configured, `AsyncResult.get()` on the client side returns it. A
handler that raises comes back as `FAILURE` with a traceback.

## Publishing tasks

```python
from faststream_celery import CeleryTask

await broker.publish(
    CeleryTask("proj.tasks.send_email", args=[user_id], kwargs={"urgent": True}),
    queue="celery",
)
```

Deferred execution works as in `apply_async`:

```python
await broker.publish(
    CeleryTask("proj.tasks.digest", countdown=60, expires=3600),
    queue="celery",
)
```

Deferred tasks are held in memory until they are due, and dropped once their
`expires` has passed.

To wait for a result:

```python
response = await broker.request(CeleryTask("proj.tasks.add", args=[2, 3]), queue="celery")
result = await response.decode()  # {"status": "SUCCESS", "result": 5, ...}
```

Without a result backend that waits on a temporary AMQP reply queue. With one it
reads `celery-task-meta-<id>`, exactly as `AsyncResult.get()` does.

`publisher.request(...)` uses the same result mechanism as `broker.request(...)`.
It applies the endpoint's queue, exchange, routing key and header defaults;
arguments supplied to the call take precedence. Its configured `reply_to` is
used only by ordinary publishing: each RPC request gets its own reply queue.

`request()` waits for `SUCCESS`, `FAILURE` or `REVOKED`. Intermediate states
such as `STARTED`, `RETRY` and custom progress states keep waiting within the
same timeout. A failure or revocation is returned as its Celery result envelope.

Subscriber `no_reply=True` suppresses automatic success and failure replies.
Configured result backends still record outcomes, canvas continuations run,
and explicit publisher decorators publish normally. Requests through a result
backend can still complete; RPC requests without a reply time out.

Handlers may return a FastStream `Response`: its body becomes the Celery result
and the value passed to callbacks and chain steps. Its headers and correlation
ID remain available to ordinary FastStream publishers; Celery results keep the
incoming task ID.

Canvas continuations preserve scheduling, explicit routing, headers, retries,
group metadata and nested callbacks/errbacks. A continuation without an explicit
queue uses its subscriber's queue. A failed chain step completes the remaining
frozen task results as `FAILURE` without executing those steps. Unsupported
non-null options and composite signatures raise `SetupError` during dispatch;
priority and composite canvas execution remain pending.

Mixed chords created by Celery clients can share ordinary header tasks and
callbacks between Celery and FastStream. They require the default ordered JSON
Redis result backend, with either RabbitMQ or Redis transport. Header results
retain their order; dependency failures complete the body as `ChordError` and
run its queued errbacks. A retried completion can republish the callback before
group cleanup; callbacks should be idempotent. Saved `GroupResult` headers
are joined from stored task results in their saved order and nested list
structure; their tasks must store results. In-memory chord testing remains
unsupported. Celery dispatches composite header signatures; this broker
restores their saved result structure when its ordinary tasks complete.
Failures within linear header chains also complete the chord body as
`ChordError`, without executing the
remaining chain steps or the body.
See [the canvas contract](docs/design.md#8-canvas).

RPC replies are acknowledged before their connection closes. Returned results
have no delivery to settle: their `ack()`, `nack()` and `reject()` methods only
update local message state and cannot requeue the completed task.

FastStream delivery instructions (`AckMessage`, `NackMessage`, `RejectMessage`,
`SkipMessage`) produce no task result or canvas continuation. The same applies
when a handler explicitly calls `message.nack()` or `message.reject()`. With
`AckPolicy.NACK_ON_ERROR`, an exception leaves the result unfinished while the
delivery is requeued; a later successful attempt can complete it.

Calling `message.ack()` and then returning normally still reports the handler's
result. If processing ends with a delivery instruction and no later outcome,
clients waiting for the task result time out.

## Transports and results

```python
CeleryBroker(
    "redis://localhost:6379/0",
    result_backend="redis://localhost:6379/0",
    transport_options={"visibility_timeout": 3600},
)
```

RabbitMQ and Redis are both supported, and both are tested against a live Celery
on every commit. On Redis, `visibility_timeout` has to match across every client
and worker sharing the bus — otherwise one side redelivers what another is still
working on.

`prefetch_count=0` enables unlimited prefetch at broker or subscriber level.
`None` inherits the broker setting, then defaults to `max_workers`. Subscribers
sharing a queue use the widest requested window; zero takes precedence over
finite values. Processing concurrency is set separately by `max_workers`.

After a consumer has started, recoverable read-connection failures are retried
automatically with capped exponential backoff. `broker.ping()` returns `False`
while any queue consumer is recovering or has stopped. Errors establishing the
first consumer connection still propagate from startup.

Acknowledgements from a disconnected consumer fail rather than running on the
replacement connection. Unacknowledged tasks may be redelivered, so handlers
must tolerate repeated execution.

| | Read | Publish |
|---|---|---|
| Protocol v2 (Celery 4+) | yes | yes |
| Protocol v1 | yes | no |

Messages are JSON, as Celery does by default.

## Testing

`TestCeleryBroker` runs handlers in memory — middlewares, parsing and dependency
injection included — with no broker and no connection:

```python
from faststream_celery import CeleryTask, TestCeleryBroker


async def test_send_email() -> None:
    async with TestCeleryBroker(broker):
        await broker.publish(CeleryTask("proj.tasks.send_email", args=[1]), queue="celery")

        send_email.mock.assert_called_once_with({"args": [1], "kwargs": {}})
```

Assert inside the block: the mocks are reset on the way out.

`request()` returns the same Celery result envelope in fake mode as with a real
broker, including `FAILURE` for handler errors. An ordinary test `publish()`
still raises handler exceptions. Requests with no result time out; unfinished
request handlers are cancelled when the test broker exits.

A broker configured with `result_backend=` works there too — the backend is
swapped for an in-memory one, so results are recorded and `request()` reads
them back with no Redis to run:

```python
from faststream_celery.backend import InMemoryResultBackend


async def test_the_result_is_recorded() -> None:
    async with TestCeleryBroker(broker):
        response = await broker.request(
            CeleryTask("proj.tasks.add", args=[2, 3]),
            queue="celery",
            correlation_id="task-id-1",
        )

        envelope = await response.decode()
        assert envelope["status"] == "SUCCESS"

        backend = broker.config.broker_config.result_backend
        assert isinstance(backend, InMemoryResultBackend)
        assert backend.results["task-id-1"]["result"] == 5
```

## FastAPI

```python
from fastapi import FastAPI
from faststream_celery.fastapi import CeleryRouter

router = CeleryRouter("amqp://guest:guest@localhost:5672//")


@router.subscriber("celery", task="proj.tasks.add")
async def add(args: list[int], kwargs: dict) -> int:
    return sum(args)


app = FastAPI(lifespan=router.lifespan_context)
app.include_router(router)
```

## Observability

```python
from faststream_celery.opentelemetry import CeleryTelemetryMiddleware
from faststream_celery.prometheus import CeleryPrometheusMiddleware

broker = CeleryBroker(
    "amqp://guest:guest@localhost:5672//",
    middlewares=(
        CeleryTelemetryMiddleware(),
        CeleryPrometheusMiddleware(registry=registry),
    ),
)
```

Spans carry the Celery task name and id, so two tasks on one queue stay apart
in a trace. Metrics are labelled by queue; add the task name where you want the
two apart there too:

```python
CeleryPrometheusMiddleware(
    registry=registry,
    custom_labels={"task": lambda msg: (msg.message.headers or {}).get("task", "")},
)
```

`faststream docs gen` produces an AsyncAPI schema with the queue, exchange and
routing key of every subscriber and publisher.

## Annotations

```python
from faststream_celery.annotations import CeleryMessage, Logger


@broker.subscriber("celery", task="proj.tasks.add")
async def add(message: CeleryMessage, logger: Logger) -> None:
    logger.info("running %s", message.headers["task"])
```

## Examples

[`examples/`](./examples) has one directory per scenario, with **both sides
present** — a FastStream app and the stock Celery process it talks to — so you
can start them and watch the tasks cross:

|                                                                  |                                                                        |
|------------------------------------------------------------------|------------------------------------------------------------------------|
| [`01_celery_to_faststream/`](./examples/01_celery_to_faststream) | a Celery client's tasks served by FastStream handlers                  |
| [`02_faststream_to_celery/`](./examples/02_faststream_to_celery) | tasks we publish, run by a real `celery worker`                        |
| [`03_both_ways/`](./examples/03_both_ways)                       | one call crossing the boundary four times                              |
| [`04_payload_validation/`](./examples/04_payload_validation)     | three ways to validate a task payload with Pydantic                    |
| [`05_fastapi/`](./examples/05_fastapi)                           | HTTP routes and task handlers in one app                               |
| …                                                                | canvas, results, scheduling, routers, observability, testing, security |

## Contributing

```bash
uv sync
uv run pytest tests/unit                        # fast, no broker needed
uv run pytest tests/integration -m connected    # live RabbitMQ, Redis and Celery (needs Docker)
uv run nox                                      # every Python, both ends of the dependency ranges
uv run nox -s lint                              # ruff and mypy
```

CI tests pinned minimum and latest released FastStream/Kombu versions across
Python 3.10–3.14. The latest-release combination also runs daily at 04:23 UTC:

```bash
uv run nox -s tests -k 'faststream_latest and kombu_latest'
```

`AGENTS.md` has the development guidelines and the `justfile` the day-to-day
commands.

## License

Apache-2.0.
