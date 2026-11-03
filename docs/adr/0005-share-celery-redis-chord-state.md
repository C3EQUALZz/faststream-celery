# Share Celery's Redis chord state

Status: proposed for review with the initial flat-chord implementation.

## Context

Mixed applications need a chord header to finish on either Celery or FastStream.
A separate counter owned by this package cannot observe completions from an
unmodified Celery worker. Importing Celery's synchronous backend would introduce
a runtime dependency and another synchronous execution boundary.
The shared format is defined by Celery's
[RedisBackend](https://docs.celeryq.dev/en/stable/_modules/celery/backends/redis.html).

## Decision

Use the existing asynchronous Redis result backend to join Celery's native
ordered JSON journal and counters: `celery-taskset-meta-<group>.j`, `.s`, `.t`.
The Celery client creates the chord, freezes task ids and sets the group size.
Kombu's JSON encoder preserves the journal member format. An optional
`ChordBackend` protocol exposes recording a part and clearing the finished group;
ordinary result backends need not provide chord coordination.

The initial contract covers flat task headers and ordinary task callbacks,
default key prefixes and `result_chord_ordered=True`. Saved/nested group
structures and unordered journals are rejected explicitly. Chord failures
complete the frozen body with Celery's `ChordError` and dispatch queued errbacks.
Transport may be RabbitMQ or Redis; the result backend must be Redis.

## Consequences

Celery remains a test dependency. Publishing still uses Kombu (ADR-0001),
through FastStream's existing API (ADR-0002). In-memory TestCeleryBroker does
not emulate distributed Redis coordination; mixed chords require live tests.

Identical journal members do not increase the count or redispatch from our
coordinator. This is not an exactly-once guarantee: legacy Celery workers do
not share a callback claim, and journal updates, callback publication and
message acknowledgement are not atomic. Conflicting duplicate results and
delivery failures require the separate delivery-guarantees work.

Nested headers, header-chain failure coordination, non-default backend
serialization/prefixes and alternative coordination modes remain follow-ups.
Full canvas support is not implied by this initial interoperability slice.
