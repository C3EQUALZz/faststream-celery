# 5. FastAPI + Celery in one service

`CeleryRouter` mounts Celery task handlers on a FastAPI app: HTTP routes and
task handlers share the dependency graph, the Pydantic models and the lifespan.

## Run

```bash
docker compose -f ../docker-compose.yaml up -d rabbitmq

# terminal 1 — the worker behind GET /sum
celery -A celery_worker worker --loglevel=info -Q math

# terminal 2 — the FastAPI app (HTTP + Celery consumer + Celery publisher)
uvicorn app:app --reload

# terminal 3
curl -X POST localhost:8000/emails \
     -H 'content-type: application/json' \
     -d '{"user_id": 42, "urgent": true, "locale": "ru"}'
curl 'localhost:8000/sum?left=2&right=3'
python celery_client.py
```

## Expected output

```
$ curl -X POST localhost:8000/emails -d '{"user_id": 42}' -H 'content-type: application/json'
{"status":"queued"}
# uvicorn log: emailed 42 via https://mail.example/en

$ curl 'localhost:8000/sum?left=2&right=3'
{"result":5}
# celery worker log: add(2, 3)

$ python celery_client.py
examples.send_email -> {'status': 'sent', 'user_id': 42, 'delivered_by': 'https://mail.example/ru'}
```

An invalid HTTP body is a 422 from FastAPI; the same invalid payload sent by a
Celery client fails the task with a `ValidationError` — one model, both doors.

## What makes it work

* `router.lifespan_context` starts the broker and its subscribers with the
  ASGI app, and stops them with it.
* `CeleryBroker` (from `faststream_celery.fastapi`) is a FastAPI dependency
  resolving to the router's broker, so an HTTP route can publish or request.
* A subscriber resolves `Depends` the way a route does — `get_mailer` here has
  teardown and is used from the task handler.
* `router.publisher(...)` is a long-living publisher: bound to a queue once,
  callable from anywhere, and documented in the AsyncAPI schema.
* `GET /sum` blocks on `broker.request(...)`, so the HTTP response carries what
  the Celery worker computed. For long tasks, publish and return a task id
  instead.
