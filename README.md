# faststream-celery

[![PyPI](https://img.shields.io/pypi/v/faststream-celery.svg)](https://pypi.org/project/faststream-celery/)
[![Python](https://img.shields.io/pypi/pyversions/faststream-celery.svg)](https://pypi.org/project/faststream-celery/)
[![License](https://img.shields.io/pypi/l/faststream-celery.svg)](./LICENSE)

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

Leave `task=` off and the handler takes every task on the queue. A task nobody
claims is rejected, the way a Celery worker rejects one it does not know.

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

Spans and metrics carry the Celery task name, so two tasks on one queue stay
apart. `faststream docs gen` produces an AsyncAPI schema with the queue,
exchange and routing key of every subscriber and publisher.

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

`AGENTS.md` has the development guidelines and the `justfile` the day-to-day
commands.

## License

Apache-2.0.
