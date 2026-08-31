# 11. Middlewares, dependencies, annotations

Three layers, each with its own job:

| Layer | Scope | For |
|---|---|---|
| annotations | one parameter | what the handler needs from the framework: `CeleryMessage`, `Logger`, `CeleryBroker`, `Connection` |
| dependencies | one subscriber | what the handler needs from your app, per message, with teardown |
| middlewares | every subscriber | what wraps a task whatever the handler is: audit, metrics, retry policy |

## Run

```bash
docker compose -f ../docker-compose.yaml up -d rabbitmq

faststream run faststream_app.py:app     # terminal 1
python celery_client.py                   # terminal 2
```

## Expected output

The app logs the audit lines:

```
audit: examples.charge ok in 0.001s
audit: examples.charge failed in 0.000s: PermissionError
giving up on examples.charge after 9 retries
```

and the client prints:

```
examples.charge -> {'status': 'captured', 'order_id': 1234, 'attempt': 1}
examples.charge (over limit) -> FAILURE: PermissionError: a charge over 100000 needs a review
examples.inspect -> {'task_name': 'examples.inspect', 'task_id': '...', 'retries': 0, ...}
examples.charge with retries=9 -> rejected, watch the app log
```

## What to notice

* A middleware's `consume_scope` sees the message before the handler and sees
  whatever the handler raised. Raising `RejectMessage` drops the task without
  requeueing it — the way a Celery worker rejects an unregistered task.
* `dependencies=(Depends(within_limit),)` runs a check for its side effect
  only. Pydantic covers the *shape* of a payload; a dependency covers the
  policy the shape cannot express, in one place instead of at the top of every
  handler.
* `Depends(get_ledger)` is resolved per message and torn down after it, so a
  session or a transaction has a defined lifetime.
* `CeleryMessage` is the whole Celery envelope: `headers["task"]`,
  `headers["id"]`, `retries`, `root_id` / `parent_id` for a canvas, `eta`,
  `expires`, plus `reply_to` and `correlation_id` from the AMQP properties.
* A result whose top-level dict has a `task` key is indistinguishable from a
  Celery protocol v1 task body, and a parser reading it back will treat it as
  one. Name that field something else — `examples.inspect` returns `task_name`.
