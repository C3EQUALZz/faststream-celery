# kombu as the transport layer instead of a native async implementation

Status: accepted

The broker is built on top of **kombu** (the messaging library Celery itself is
built on) rather than on native async clients (aio-pika, redis.asyncio) with a
hand-rolled Celery envelope.

The project's core value is wire compatibility with Celery, and the only way to
guarantee it across transports (RabbitMQ, Redis, and any future kombu
transport) is to use the same library Celery uses. The native path looks
cleaner but requires reverse-engineering kombu's internals — most painfully the
Redis virtual transport (LIST-based queues, `_kombu.binding.*` sets, `unacked`
HASH/ZSET with visibility timeout). Every compatibility bug there means lost
tasks in production.

## Considered options

- **kombu as transport (chosen).** Exact queue/exchange naming, serialization
  and envelope format for free, on every kombu transport. Cost: kombu is
  synchronous, so the broker bridges it into asyncio via a dedicated thread per
  subscriber (`drain_events` → `asyncio.Queue`), and kombu objects are only
  ever touched from that thread.
- **Native async clients + hand-rolled envelope.** Fully async, idiomatic
  FastStream internals. Rejected: the compatibility surface becomes our own
  reverse-engineered copy of kombu, which drifts with every kombu release.

## Consequences

- The sync→async bridge (consumer thread + `asyncio.Queue`, separate
  read/write connections) is a fixed architectural component.
- `kombu` is a hard dependency; `faststream` is pinned to a minor range
  because the broker builds on `faststream._internal`.
