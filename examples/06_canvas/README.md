# 6. Canvas: a FastStream handler inside a Celery workflow

A chain, a callback and an errback all travel inside the task body (Celery's
`embed` slot). This broker reads them and publishes whatever was waiting there
once a handler finishes — the same dispatch `celery.app.trace` does. So a
workflow can cross the boundary as often as it likes:

```
examples.parse (Celery)  ->  examples.enrich (FastStream)  ->  examples.notify (Celery)
```

## Run

```bash
docker compose -f ../docker-compose.yaml up -d rabbitmq

# terminal 1 — the Celery ends of the canvas
celery -A celery_worker worker --loglevel=info -Q celery

# terminal 2 — the middle link, plus canvases it starts itself
faststream run faststream_middle.py:app     # or: python faststream_middle.py

# terminal 3 — a canvas started from a stock Celery client
python celery_client.py
python celery_client.py callback
python celery_client.py errback
```

## Expected output

`celery_client.py` prints, and the two worker logs interleave:

```
$ python celery_client.py chain
chain -> {'document_id': 'the quick brown fox', 'words': 4, 'reading_seconds': 1}

# celery worker: parse('the quick brown fox')
# faststream:   enriching the quick brown fox (root_id=..., parent_id=...)
# celery worker: notify({'document_id': ..., 'words': 4, 'reading_seconds': 1})
```

```
$ python celery_client.py errback
enrich + link_error -> FAILURE: ValueError
watch the worker log for alert(task_id=...)
```

`faststream_middle.py` starts four canvases of its own at startup: the same
three-link chain, a `link=` callback, a `link_error=` errback, and an immutable
callback.

## What makes it work

* On success, the callbacks run first, then the chain steps along. Both are
  called with the handler's return value as their first argument — unless the
  signature is immutable (`immutable=True`, Celery's `.si()`).
* On failure, the errbacks run instead, and receive the failed task's **id**,
  not the exception. That is Celery's own convention
  (`Backend._call_task_errbacks`), and it is why setting `correlation_id` on a
  published task pays off.
* A step's `options={"queue": ...}` decides where it goes; without one it
  follows the queue the finished task arrived on.
* `root_id` and `parent_id` are threaded through, so a canvas stays traceable
  across both sides.
* **Chain order is reversed on the wire.** Celery serializes a chain with the
  next step last, so `parse -> enrich -> notify` published from this side is
  written `chain=[notify, enrich]`. Celery's own `chain(a, b, c)` on the
  client side does that reversal for you.

## Not yet

A chord's body is carried in the same `embed` slot but is not dispatched — the
group counters that coordinate it live in the result backend and are a separate
piece of work. Chains, callbacks and errbacks are the supported canvas.
