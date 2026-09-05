# Design: a Celery-compatible broker for FastStream

Status: agreed (grilling session, 2026-08-29).

## Goal

A FastStream broker that is wire-compatible with Celery: it consumes tasks sent
by Celery clients (`send_task`) and publishes tasks that real Celery workers
execute. The primary scenario is gradual migration from Celery to FastStream
while the bus keeps running. The user-facing API is pure FastStream idiom, with
no Celery-specific extensions on top of it.

## Key decisions

### 1. Transport — kombu as a dependency

Celery itself is built on kombu (`celery/app/amqp.py` is a thin wrapper over
`kombu.Producer/Consumer`). Compatibility across all transports is guaranteed
by using the same library; hand-rolled envelopes and reverse-engineering the
virtual transports (Redis LIST + `_kombu.binding.*` + `unacked` HASH/ZSET)
were rejected as fragile. See ADR-0001.

From [taskiq-kombu](https://github.com/soapun/taskiq-kombu) we reuse only the
sync→async bridge pattern; its message format (`application/data`) is not
Celery-compatible and is not reused.

### 2. Async bridge: thread + `asyncio.Queue`

Each subscriber gets a dedicated `threading.Thread`: `kombu.Consumer` +
a `connection.drain_events(timeout=...)` loop, with messages handed over to an
`asyncio.Queue` via `loop.call_soon_threadsafe`; the async side (the FastStream
pipeline) reads from that queue.

All kombu objects live and are acked only on their own thread (kombu
transports, Redis especially, are not thread-safe). Separate connections for
reading and writing (`read_connection` / `write_connection`).

### 3. Message protocol

- We publish **protocol v2** only (headers `task`/`id`/`root_id`/...,
  body `(args, kwargs, embed)`, `correlation_id` in properties).
- We read **v1 and v2** (detected by the presence of the `task` header).

### 4. Routing by task name

`@broker.subscriber(queue="celery", task="proj.tasks.send_email")` — filtering
on `headers["task"]` via the stock FastStream filter mechanism. The `task`
parameter is optional: without it, the handler accepts everything in the queue.
The fate of filtered-out messages follows the stock FastStream filter rules.

### 5. Task results (full scope)

- **AMQP RPC** (`reply_to` / `correlation_id`) — as a worker (we publish the
  reply) and as a client (`broker.request()`).
- **Redis result backend** (`celery-task-meta-<id>`) — as a worker (we write
  status/result) and as a client (we read).

### 6. Ack and retry — the FastStream way

No Celery-specific defaults. The stock `AckPolicy`
(`ACK_FIRST` / `ACK` / `NACK_ON_ERROR` / `REJECT_ON_ERROR` / `MANUAL`);
`CeleryMessage.ack()/nack()/reject()` map onto kombu acknowledgements. See
ADR-0002.

The broker has no retry API (like every other FastStream broker): redelivery is
expressed via `AckPolicy` / `message.nack()`, and scenario-level retries via a
user middleware (the documented FastStream `RetryMiddleware` pattern). The
Celery-specific "republish with `retries+1`" remains available manually through
a regular `publish`. Celery fields (`retries`, `eta`, `timelimit`, ...) are
passed through to `StreamMessage.headers`.

### 7. ETA / countdown

An in-memory scheduler inside the subscriber (like a Celery worker): a message
with a future `eta` is held until due, then passed to `consume()`. No
persistence (Celery works the same way).

### 8. Canvas — full support

Chain, callbacks/errbacks (publishing the next signatures from `embed`). A
step's `options` travel with it: `queue`, the frozen `task_id`, and `reply_to` —
without the last one, the client waiting on a chain never sees the final
result.

Also in scope: **chord** (with group counters in the result backend,
`chord_unlock` mechanics). Chord is the heaviest item in scope and a candidate
for its own stage.

### 9. Publishing

Stock `broker.publish(...)` plus a message wrapper type `CeleryTask`:

```python
await broker.publish(
    CeleryTask("proj.tasks.send_email", args=[user_id], kwargs={"urgent": True}),
    queue="celery",
)
```

`countdown`/`eta`, `link`/`link_error` (→ `embed`), `expires`, `retries` are
attributes of the wrapper. Low-level publishing of raw data stays available.

### 10. Connection configuration

```python
CeleryBroker(
    "amqp://guest:guest@localhost:5672/",
    transport_options={"visibility_timeout": 3600},
    ssl=None,
    # + stock FastStream parameters: middlewares, logger, graceful_timeout...
)
```

A pre-built `kombu.Connection` is not accepted (can be added later without
breaking the API).

### 11. Transports and testing

- Officially supported: **RabbitMQ** (pyamqp) and **Redis** (kombu virtual
  transport). Other kombu transports — best effort, untested.
- Integration tests via testcontainers against a **live Celery**:
  Celery client → our broker, and our broker → a real `celery worker`
  (including result delivery).
- `TestCeleryBroker` (an in-memory fake modeled on
  `faststream/redis/testing.py`) for users' application tests. It swaps the
  producer *and* the result backend: fake mode connects nothing, so a broker
  configured with `result_backend=` gets an `InMemoryResultBackend` for the
  duration, and both reporting and `request()` keep working offline. A broker
  without a backend keeps having none, so `request()` stays on its reply path.

### 12. Dependencies and versioning

- `faststream>=0.7.5` — a custom broker inherits from the private
  `faststream._internal` API; the dependency on it is isolated in a single
  adapter module of the project.
- `kombu>=5.3,<6` — a public, stable API.
- Version matrix via **nox** tests the minimum and latest supported releases
  of FastStream and kombu across Python 3.10–3.14. A daily CI run tests the
  latest released versions at 04:23 UTC. Unreleased upstream code is outside
  the compatibility checks.

### 13. Serialization

Celery defaults: publish `json`, accept `["json"]`. The `serializer` / `accept`
parameters are passed through to kombu (`Producer.publish(serializer=...)`,
`Consumer(accept=...)`). Pickle — explicit opt-in only, with a warning in the
docs.

### 14. Concurrency and QoS

- `max_workers` — the stock FastStream `ConcurrentMixin`.
- A separate `prefetch_count` parameter (default = `max_workers`) → kombu QoS;
  for Redis it controls the size of the `unacked` buffer.

### 15. Time limits

`timelimit: [hard, soft]` is implemented as cooperative cancellation: soft —
a catchable exception inside the handler (a `SoftTimeLimitExceeded` analogue),
hard — cancellation of the handler call. The limitation ("hard cannot kill a
stuck synchronous call") is documented.

### 16. Celery Events / Flower

Out of scope for the first release. On the roadmap: emitting task events
(`task-received/started/succeeded/failed`) to the `celeryev` exchange behind a
`send_events=True` flag — the architecture does not preclude it (it's just one
more publisher).

### 17. Package structure

```
src/faststream_celery/
├── __init__.py        # public exports with explicit __all__
├── annotations.py     # broker-specific Annotated type aliases
├── broker/            # broker.py (BrokerUsecase subclass), router.py, registrator.py, logging.py
├── configs/           # @dataclass(kw_only=True) configs inheriting BrokerConfig
├── message.py         # StreamMessage subclass (CeleryMessage: ack/nack/reject → kombu)
├── parser.py          # message parser (kombu Message → StreamMessage, protocol v1/v2 detection)
├── publisher/         # publisher endpoint + producer.py (kombu.Producer wrapper)
├── subscriber/        # subscriber endpoint (kombu consumer thread + asyncio.Queue bridge,
│                      #  ETA scheduler, canvas; split into usecases/ if it grows)
├── response.py        # PublishCommand subclasses (CeleryPublishCommand)
├── security.py        # auth/security helpers
├── testing.py         # in-memory TestBroker (TestCeleryBroker)
└── exceptions.py      # broker-specific exceptions
```

Public exports: `from faststream_celery import CeleryBroker, CeleryRouter, TestCeleryBroker`.

AsyncAPI specifications: minimal `SubscriberSpecification`/
`PublisherSpecification` subclasses (queue/exchange/routing key) — required
for `faststream docs`.

## Risks

| Risk | Mitigation |
|---|---|
| Private `faststream._internal` breaks between releases | Minimum/latest release nox matrix, adapter module |
| Chord — complex coordination via the result backend | A dedicated implementation stage |
| Redis virtual transport (`unacked`, `visibility_timeout`) | Integration tests against live Celery from day one |
| Client and worker `visibility_timeout` must match | Document; `transport_options` are passed through |

## Roadmap (beyond the first release)

- Celery Events emission (`celeryev`) for Flower visibility.
- Accepting a pre-built `kombu.Connection` in the constructor.
- Support for additional kombu transports (SQS, etc.) on demand.
