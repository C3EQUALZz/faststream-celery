# 2. faststream-celery publishes → Celery consumes

The other direction: the workers stay stock Celery — an established codebase of
`@app.task` functions, maybe with a C extension or a pinned dependency you do
not want to move — while the service that enqueues the work becomes async
FastStream.

## Run

```bash
docker compose -f ../docker-compose.yaml up -d rabbitmq

# terminal 1 — a stock Celery worker (run it from this directory)
celery -A celery_worker worker --loglevel=info -Q celery

# terminal 2 — publish, no results wanted
faststream run faststream_publisher.py:app     # or: python faststream_publisher.py

# terminal 2 — or publish and wait for each result
faststream run faststream_request.py:app       # or: python faststream_request.py
```

## Expected output

The Celery worker logs:

```
[...] Task examples.add[...] received
add(2, 3)
[...] Task examples.add[...] succeeded in 0.001s: 5
[...] Task examples.send_email[...] received
emailing user 42 (urgent=True)
[...] Task examples.add[order-1234-total] received
```

`examples.send_email` with `countdown=5` shows up five seconds later, and the
`expires=3600` task runs immediately (it has an hour to spare).

`faststream_request.py` logs on its own side:

```
examples.add -> SUCCESS 5
examples.send_email -> SUCCESS {'status': 'sent', 'user_id': 42, 'locale': 'en'}
examples.fail -> FAILURE ValueError: ['nope']
```

## What makes it work

* `CeleryTask` is serialized to the protocol v2 envelope `apply_async`
  produces: metadata in the headers, `(args, kwargs, embed)` as the JSON body.
* The destination follows Celery's default topology — a durable direct
  exchange named after the queue, routing key the same. Pass `exchange=` /
  `routing_key=` when your Celery side declares something else.
* `countdown`, `eta`, `expires`, `retries`, `group`, `link`, `link_error` and
  `chain` on `CeleryTask` mirror `apply_async`.
* `correlation_id` is the Celery task id: pass your own to correlate a task
  with whatever produced it, or let one be generated.
* `broker.request(...)` waits for the result the way `AsyncResult.get()` does
  and hands back the raw Celery envelope, which a Pydantic model turns into
  something typed.
* Validating the payload on both sides — one Pydantic model per task — is what
  keeps a renamed field from becoming a `TypeError` inside a worker you did
  not deploy.
