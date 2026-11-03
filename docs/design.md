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
parameter is optional: without it, the handler is the fallback for this queue.
Exact task matches take precedence; one subscriber receives each message.
Production and fake delivery share the selection rule. A selected subscriber's
custom filter follows the stock FastStream rules without retrying the fallback.

The task-name filter dispatches within this application; it does not route
between competing worker processes. During migration, use separate queues and
`task_routes` for different executor task sets. Sharing a queue requires every
consumer to serve every task routed to it. See ADR-0003.

Publisher endpoints apply configured exchange and routing-key defaults in both
direct calls and subscriber decorators. Explicit command routing is preserved;
an explicit `exchange=""` selects the AMQP default exchange.

### 5. Task results (full scope)

- **AMQP RPC** (`reply_to` / `correlation_id`) — as a worker (we publish the
  reply) and as a client (`broker.request()`).
- **Redis result backend** (`celery-task-meta-<id>`) — as a worker (we write
  status/result) and as a client (we read).

`broker.request()` and `publisher.request()` share result selection in the
producer. It waits on the configured backend, or uses an isolated RPC reply
queue, and returns a detached payload. Both endpoints use FastStream's stock
request middleware/parser pipeline. Publisher routing/header defaults and
explicit call overrides apply to requests as they do to ordinary publishing.

Subscriber `no_reply=True` disables automatic RPC replies for success and
failure. It leaves backend result recording, callbacks/errbacks and explicit
publisher decorators active. `ignore_result` remains a separate Celery header.

For FastStream `Response` returns, result reporting and canvas use the body
normalized through FastStream's public `ensure_response()`. The middleware
returns the original value to FastStream so ordinary publishers retain response
headers and correlation ID. Result envelopes remain keyed by the incoming task ID.

### 6. Ack and retry — the FastStream way

No Celery-specific defaults. The stock `AckPolicy`
(`ACK_FIRST` / `ACK` / `NACK_ON_ERROR` / `REJECT_ON_ERROR` / `MANUAL`);
`CeleryMessage.ack()/nack()/reject()` map onto kombu acknowledgements. See
ADR-0002.

Delivery disposition and task outcome are separate (ADR-0004). The FastStream
`HandlerException` instructions produce no result or canvas continuation.
Explicit `nack()` / `reject()` also leave processing without an outcome, even
when the handler returns normally. Exceptions subject to `NACK_ON_ERROR` do
not publish terminal failure unless the delivery was already acknowledged.
Ordinary completion after `ack()` still reports a success or failure outcome.

The internal `Acknowledgement` protocol exposes `ack()` and `reject(requeue=...)`.
In-memory messages implement them synchronously; a real delivery returns an
awaitable tied to its owning consumer session. `CeleryMessage` commits its state
after settlement succeeds and serializes concurrent settlement calls. Backend
results and RPC snapshots have no delivery controller; RPC settlement finishes
on the connection's thread before returning the snapshot.

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

### 8. Canvas

Chain, callbacks/errbacks (publishing the next signatures from `embed`).
Continuations retain scheduling (`countdown`, `eta`, `expires`), routing
(`queue`, `exchange`, `routing_key`, `declare`), headers, the frozen `task_id`
and `reply_to`, retries, group id/index and nested callbacks/errbacks. A callback
or errback can carry its own chain; a chain step receives the remaining outer
chain, matching Celery's worker dispatch. Root and parent ids follow the task
that actually dispatched the continuation.

Errbacks run as queued task signatures. Mutable signatures receive the failed
task id before their own positional arguments; immutable signatures keep only
their own arguments. Both retain their keyword arguments. The broker does not
invoke Celery's inline errbacks with `(request, exc, traceback)`; those require
an application wrapper using the queued task contract.

When an ordinary chain step fails, every remaining signature with a frozen
`task_id` receives a `FAILURE` result with the original exception and traceback,
without executing the remaining tasks. Backend storage uses each signature's
id; RPC replies use its own `reply_to`, without falling back to the failed
task's reply queue. Signatures without a task id do not create new result ids.
Subscriber `no_reply` still suppresses automatic RPC replies; the failed task's
`ignore_result` suppresses both storage and replies. Delivery control and
requeued processing do not propagate terminal failure. Failure handling inside
composite canvases remains a separate requirement.

`CeleryTask` continuations serialize all their task options, including nested
canvas and lineage. Relative times are resolved when the continuation is
published. Without an explicit queue, a continuation uses the active subscriber's
queue; routing keys are not treated as queue names. Mixed applications should
set explicit continuation queues for their respective executors.

Non-null options outside this supported set raise `SetupError` when dispatching
the continuation, as do composite signatures (`subtask_type`, such as group or
chord). Priority and serializers other than the task protocol's JSON are not
implemented yet. This prevents execution with
silently discarded options; it does not provide atomic canvas dispatch or undo
an already completed handler.

Mixed chords constructed by a Celery client are supported with a Redis result
backend, over either RabbitMQ or Redis transport. The initial contract uses
flat task headers, ordinary task callbacks, default backend key prefixes,
JSON-compatible results and `result_chord_ordered=True`. Each executor records
parts in Celery's shared `.j` journal and reads `.s`/`.t` size counters. The
executor completing the group publishes the body with results in header order.
An identical journal member does not increase the count or redispatch from
our coordinator. A failed dependency produces `celery.exceptions.ChordError`
for the frozen body id and invokes its queued errbacks; the body is not executed.

Saved/nested group structures and unordered journals raise `SetupError`.
Nested headers, header-chain failure coordination, other backend settings and
`chord_unlock` backends remain follow-ups. RPC does not coordinate chords.
`TestCeleryBroker` replaces Redis with an in-memory result backend and cannot
join these distributed groups; use connected tests. Callback publication,
result storage and acknowledgement are not atomic or exactly once. See
[ADR-0005](adr/0005-share-celery-redis-chord-state.md) for the protocol boundary.

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

- `faststream>=0.7.8` — a custom broker inherits from the private
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
- A separate `prefetch_count` parameter → kombu QoS; `None` inherits the broker
  setting, then defaults to `max_workers`. An explicit `0` means unlimited
  prefetch. Shared queue consumers keep the widest requested window, with zero
  taking precedence over every finite value. Redis uses the same limit for
  its `unacked` buffer.

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
├── acknowledgement.py # delivery settlement protocol
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
for `faststream docs`. The channel label identifies the documented endpoint;
the channel address is its routing key, passed directly to FastStream's
`SubscriberSpec` and `PublisherSpec` models.

## Risks

| Risk                                                      | Mitigation                                         |
|-----------------------------------------------------------|----------------------------------------------------|
| Private `faststream._internal` breaks between releases    | Minimum/latest release nox matrix, adapter module  |
| Chord — complex coordination via the result backend       | A dedicated implementation stage                   |
| Redis virtual transport (`unacked`, `visibility_timeout`) | Integration tests against live Celery from day one |
| Client and worker `visibility_timeout` must match         | Document; `transport_options` are passed through   |

## Roadmap (beyond the first release)

- Celery Events emission (`celeryev`) for Flower visibility.
- Accepting a pre-built `kombu.Connection` in the constructor.
- Support for additional kombu transports (SQS, etc.) on demand.
