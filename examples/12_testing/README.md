# 12. Testing without a broker

`TestCeleryBroker` swaps the producer for one that routes a publish straight
into the matching subscribers. Middlewares, the parser, Pydantic validation and
dependency injection all run — in memory, in one process, with no RabbitMQ, no
Redis, no Celery and no Docker.

## Run

```bash
pip install pytest pytest-asyncio
pytest test_app.py
```

No services needed.

## Expected output

```
7 passed
```

## What you can assert

* **routing** — `handler.mock.assert_called_once_with({"args": [...],
  "kwargs": {...}})`. That dict is the normalized Celery body every handler
  sees.
* **the result** — `await (await broker.request(...)).decode()`.
* **what a handler published** — `publisher.mock`, on the publisher object.
* **failures** — the handler's exception propagates out of `broker.publish`, so
  `pytest.raises` works on it, `ValidationError` included.
* **routing misses** — a task no subscriber claims raises
  `SubscriberNotFound`, which is the in-memory stand-in for a Celery worker
  rejecting an unregistered task.
* **Celery metadata** — publish with `correlation_id=` and read
  `message.headers` in the handler.

## A broker with a result backend

`result_backend=` works in fake mode too: the configured backend is swapped for
an `InMemoryResultBackend` for the duration of the block — fake mode connects
nothing, so the real one would have no client to write through. Results are
recorded and read back the same way:

```python
from faststream_celery.backend import InMemoryResultBackend

async with TestCeleryBroker(broker):
    await broker.publish(
        CeleryTask("examples.charge", kwargs={"order_id": 1, "amount_cents": 500}),
        queue=QUEUE,
        correlation_id="task-id-1",
    )

    backend = broker.config.broker_config.result_backend
    assert isinstance(backend, InMemoryResultBackend)
    assert backend.results["task-id-1"]["status"] == "SUCCESS"
```

With a backend configured, `broker.request(...)` returns the recorded **Celery
envelope** (`status`, `result`, `traceback`), as it does in production. Without
one it returns the handler's value directly, over the reply path.

## Three things to know

* Assert **inside** the `async with` block: the mocks are reset on the way out.
* `handler.mock` records the message that was *delivered* to the subscriber,
  before validation and before the handler body. An invalid payload still
  shows up there — assert on the raised `ValidationError`, or on a downstream
  publisher's mock, to prove the body did not run.
* `persistent=False` on a subscriber registered inside a test keeps it out of
  the app once the block exits, instead of leaking into the tests that follow.

## Integration tests

For the wire itself — a live Celery client and worker, both transports —
`tests/integration/` in this repository runs RabbitMQ, Redis and a real
`celery worker` in testcontainers. `TestCeleryBroker` checks your handlers;
those check the protocol.
