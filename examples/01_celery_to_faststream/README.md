# 1. Celery publishes → faststream-celery consumes

The direction most services migrate first: the producers stay stock Celery, the
worker becomes an async FastStream app. Neither side is configured for the
other — the Celery client only knows a queue name and a task name.

## Run

```bash
docker compose -f ../docker-compose.yaml up -d rabbitmq

# terminal 1 — the FastStream worker
faststream run faststream_consumer.py:app     # or: python faststream_consumer.py

# terminal 2 — a stock Celery client
python celery_client.py
```

## Expected output

`celery_client.py` prints:

```
examples.add(2, 3) -> 5
examples.send_email -> {'status': 'sent', 'user_id': 42, 'locale': 'ru'}
examples.resize_image -> {'upload_id': 'upload-7', 'size': [800, 600], 'task_id': '...', 'retries': 0}
examples.send_email -> queued, not waiting
examples.add in 3.0s -> 15
examples.fail -> FAILURE: RuntimeError: nope
examples.send_email(user_id=-1) -> FAILURE: Exception: <class 'pydantic_core._pydantic_core.ValidationError'>([])
examples.not_implemented -> queued, watch the consumer's warning
```

Single scenarios: `python celery_client.py bad`, `... fail`, `... unknown`.

## What makes it work

* `task=` on a subscriber matches `headers["task"]`, the name Celery puts on
  the wire (protocol v2; protocol v1 messages are read too).
* The body is normalized to `{"args": [...], "kwargs": {...}}`, so a handler
  declares `args` and `kwargs` and annotates each — a typed tuple for the
  positional half, a Pydantic model for the keyword half. Validation runs
  before the handler body, and a bad payload fails the task with
  `ValidationError` instead of an `AttributeError` halfway through.
* A handler's return value — including a Pydantic model — is published as the
  Celery result envelope, to the `reply_to` queue the client created
  (`backend="rpc://"`) or to a result backend.
* An unhandled exception becomes a `FAILURE` envelope with a traceback, so
  `AsyncResult.get()` re-raises it and `AsyncResult.status` reads `FAILURE`.
  Celery rebuilds the exception from `exc_type` / `exc_module` / `exc_message`;
  one it cannot reconstruct — a `ValidationError` takes constructor arguments
  Celery does not have — comes back as a plain `Exception` carrying the type
  name. The status and the traceback are exact either way.
* `countdown` / `eta` / `expires` from the client are honoured: the task is
  held until due, and dropped once expired.
* A task no subscriber claims is rejected, the way a Celery worker rejects an
  unregistered task — unless a `task=`-less subscriber takes the rest.
