# Broker API stays strictly within FastStream idioms

Status: accepted

The broker exposes no Celery-style API surface: no `retry()`/`max_retries`
helper, no Celery-specific ack defaults, no `send_task`-shaped publish method.
Publishing goes through a message wrapper (`CeleryTask`) passed to the stock
`broker.publish(...)`; retries are expressed via `AckPolicy` /
`message.nack()` or user middleware (the documented FastStream
`RetryMiddleware` pattern); ack behavior keeps the platform defaults of every
other FastStream broker.

This is a deliberate deviation from "behave like a Celery worker". FastStream
has no handler-level retry API in any broker, and matching Celery's defaults
(`acks_late`, republish-with-`retries+1`) would make this broker inconsistent
with the rest of the FastStream ecosystem — the consistency is the point of
the project. Celery protocol fields (`retries`, `eta`, `timelimit`, ...) are
exposed read-only in `StreamMessage.headers`, and manual republish semantics
remain available to users who need them.

## Consequences

- Features must be added by mirroring other FastStream brokers (Redis/Rabbit
  packages), not by porting Celery APIs.
- Users migrating Celery code that relies on `self.retry()` need a documented
  migration recipe (middleware or manual republish) — this belongs in the
  docs, not in the broker API.
