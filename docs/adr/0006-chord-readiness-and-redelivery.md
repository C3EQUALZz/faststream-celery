# Chord readiness is independent of whether a member was newly added

Status: proposed for review after 8d0d558.
Supersedes the callback deduplication claim in ADR-0005; its shared Redis
protocol, scope and backend capability decisions remain unchanged.

## Context

A successful ZADD only proves that a part was recorded. If the subsequent
ZRANGE fails, the callback has not been dispatched. Suppressing a retry because
ZADD now returns zero leaves the frozen body result pending indefinitely.

## Decision

Use the shared count, size and adjustment keys to determine readiness, including
when the incoming member already exists. Identical members remain deduplicated
by the ZSET. A ready group is read again on redelivery, matching
[Celery's Redis backend](https://docs.celeryq.dev/en/stable/_modules/celery/backends/redis.html).
Do not add a separate callback claim protocol that unmodified Celery workers
would ignore.

## Consequences

Redelivery can recover a failed read while the group journal remains available.
Callbacks can be republished on concurrent/retried completion before cleanup;
applications must use idempotent callbacks. There is no exactly-once guarantee
or automatic promise of redelivery under every FastStream acknowledgement
policy. The live regression uses NACK_ON_ERROR to verify recovery.

All participants must agree on result_chord_ordered=True. An existing LIST can
be rejected, but an absent journal does not reveal another worker's configuration.
There is no automatic ordered/unordered-mode negotiation.
