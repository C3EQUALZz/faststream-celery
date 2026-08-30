# faststream-celery

A [FastStream](https://github.com/ag2ai/faststream) broker that is wire-compatible
with [Celery](https://docs.celeryq.dev/). It consumes tasks sent by Celery clients
and publishes tasks that real Celery workers execute, so an existing Celery bus can
keep running while services move to FastStream one at a time.

The user-facing API is plain FastStream — no Celery-specific extensions
(see [`docs/adr/0002-faststream-api-consistency.md`](docs/adr/0002-faststream-api-consistency.md)).
Transport is [kombu](https://github.com/celery/kombu), the same library Celery itself
is built on ([ADR-0001](docs/adr/0001-kombu-as-transport.md)).

## Install

```bash
pip install faststream-celery
```

## Consume tasks a Celery client sends

```python
from faststream import FastStream
from faststream_celery import CeleryBroker

broker = CeleryBroker("amqp://guest:guest@localhost:5672//")
app = FastStream(broker)


@broker.subscriber("celery", task="proj.tasks.send_email")
async def send_email(args: list, kwargs: dict) -> str:
    return "sent"
```

`task=` filters on the Celery `task` header; without it the handler accepts every
task on the queue. Subscribers on the same queue share a single kombu consumer, so
routing by task name works the way Celery routes it — one broker consumer per
subscriber would make the broker round-robin messages between them instead:

```python
@broker.subscriber("celery", task="proj.tasks.send_email")
async def send_email(args: list, kwargs: dict) -> None: ...


@broker.subscriber("celery", task="proj.tasks.resize")
async def resize(args: list, kwargs: dict) -> None: ...


@broker.subscriber("celery")  # everything the two above do not claim
async def fallback(args: list, kwargs: dict) -> None: ...
```

A task no subscriber claims is rejected, as a Celery worker rejects an unknown
task. Where subscribers ask for different `prefetch_count`s on one queue, the
widest window wins.

If the incoming message carries a `reply_to` (a Celery client with the `rpc://`
result backend), the return value is published back as a Celery result envelope,
so `AsyncResult.get()` works unchanged. A handler that raises is reported to the
caller as `FAILURE`.

## Publish tasks a Celery worker executes

```python
from datetime import timedelta

from faststream_celery import CeleryTask

await broker.publish(
    CeleryTask("proj.tasks.send_email", args=[user_id], kwargs={"urgent": True}),
    queue="celery",
)

# Deferred execution, as in `apply_async(countdown=..., expires=...)`.
await broker.publish(
    CeleryTask("proj.tasks.digest", countdown=60, expires=timedelta(hours=1).seconds),
    queue="celery",
)
```

To wait for the result over AMQP RPC:

```python
response = await broker.request(CeleryTask("proj.tasks.add", args=[2, 3]), queue="celery")
envelope = await response.decode()  # {"status": "SUCCESS", "result": 5, ...}
```

## Annotations

`faststream_celery.annotations` provides the usual `Annotated` shortcuts:

```python
from faststream_celery.annotations import CeleryMessage, Logger


@broker.subscriber("celery", task="proj.tasks.add")
async def handler(message: CeleryMessage, logger: Logger) -> None:
    logger.info("task %s", message.headers["task"])
```

## Testing

`TestCeleryBroker` runs the whole pipeline — middlewares, parser, dependency
injection — in memory, with no broker and no kombu connection:

```python
from faststream_celery import CeleryBroker, CeleryTask, TestCeleryBroker


async def test_send_email() -> None:
    async with TestCeleryBroker(broker):
        await broker.publish(CeleryTask("proj.tasks.send_email", args=[1]), queue="celery")
        send_email.mock.assert_called_once_with({"args": [1], "kwargs": {}})
```

Assertions belong **inside** the context manager: the handler mocks are reset on exit.

## Protocol support

| | Read | Publish |
|---|---|---|
| Protocol v2 (Celery 4+) | yes | yes |
| Protocol v1 | yes | no |

Serialization follows Celery's defaults: we publish `json` and accept `["json"]`.

## Development

Environment and day-to-day commands live in [`AGENTS.md`](AGENTS.md) and the
`justfile` (`just lint`, `just mypy`, `just static-analysis`).

```bash
uv sync
uv run pytest tests/unit          # unit suite, no broker needed
uv run pytest tests/integration -m connected   # live RabbitMQ + Celery, needs Docker
```

### Version matrix

The package inherits from the private `faststream._internal` API, so it is tested
across every supported Python, both ends of the pinned `faststream>=0.7.5,<0.8`
range and both ends of `kombu>=5.3,<6` (see `docs/design.md` §12):

```bash
uv run nox                 # the whole matrix
uv run nox -l              # list every session
uv run nox -s tests-3.12   # one Python only
uv run nox -s nightly      # unit suite against faststream@main
uv run nox -s integration  # integration suite (needs Docker)
uv run nox -s lint         # ruff + mypy
```

CI runs the `nightly` equivalent on a schedule
(`.github/workflows/nightly-faststream-main.yaml`), so breakage in the private API
surfaces before a faststream release rather than in users' issues. Every
`faststream._internal` import is confined to `src/faststream_celery/_internal.py`,
which `tests/unit/test_internal_isolation.py` enforces.

## License

Apache-2.0.
