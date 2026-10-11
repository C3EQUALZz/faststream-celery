# Isolate queues by executor during migration

Status: accepted (compatibility audit, 2026-10-11).

## Context

The project's migration scenario allows FastStream applications and Celery
workers to coexist. A broker delivers a queued task to a competing consumer
before that consumer inspects its task name. Our `task=` filter therefore
cannot direct a task to another process; an unclaimed task is rejected without
requeue. Celery also rejects tasks that its worker does not register.

## Decision

During migration, route tasks to separate queues for their intended executors
using Celery `task_routes` and explicit FastStream publishing destinations.
Sharing a queue between executor types is supported only when every consumer
of that queue can execute every task routed to it.

Mixed canvas crosses these queues through explicit signature destinations.
Task filtering within one FastStream application remains useful for dispatch
between that application's handlers.

## Consequences

Migration requires routing configuration rather than only starting an extra
worker on an existing queue. This operational cost buys predictable task
ownership and avoids discarding tasks registered only by the other executor.
Requeueing foreign tasks is not a substitute for routing: repeated deliveries
may return to the same incompatible consumer.

This clarifies the migration definition in `CONTEXT.md`; it preserves
ADR-0001's Kombu transport and ADR-0002's FastStream public API.
