# 7. Results, and the Redis transport

Two independent choices:

* the **transport** — where tasks travel. This example uses `redis://`, the
  virtual transport kombu emulates with Redis lists.
* the **result backend** — where results are kept. `result_backend=` writes the
  `celery-task-meta-<id>` keys a Celery worker writes.

On RabbitMQ, a result can also travel back on a temporary reply queue
(`backend="rpc://"`, `broker.request()`); on Redis there is no reply queue, so
a backend is how results work at all.

## Run

```bash
docker compose -f ../docker-compose.yaml up -d redis

# terminal 1 — the FastStream consumer, writing results to Redis
faststream run faststream_consumer.py:app     # or: python faststream_consumer.py

# terminal 2 — a stock Celery client, reading them
python celery_client.py
python celery_client.py fail
```

The other direction:

```bash
# terminal 1 — a stock Celery worker on the same Redis
celery -A celery_worker worker --loglevel=info -Q celery

# terminal 2 — FastStream publishes and reads the result out of the backend
faststream run faststream_reader.py:app       # or: python faststream_reader.py
```

## Expected output

```
$ python celery_client.py
examples.build_report -> {'status': 'built', 'month': '2026-08', 'rows': 128}
  celery-task-meta-<id>:
    status:    SUCCESS
    result:    {'status': 'built', 'month': '2026-08', 'rows': 128}
    date_done: 2026-08-31T...

$ python celery_client.py fail
examples.fail -> FAILURE: RuntimeError: nope
  celery-task-meta-<id>:
    status:    FAILURE
    result:    {'exc_type': 'RuntimeError', 'exc_message': ['nope'], 'exc_module': 'builtins'}
    date_done: 2026-08-31T...
```

The raw key is printed to make the point: what the FastStream side wrote is
byte-for-byte the envelope Celery writes, under the key Celery reads.

## Testing it without Redis

```bash
pytest test_faststream_consumer.py
```

`TestCeleryBroker` swaps the configured backend for an in-memory one, so the
handlers, the validation and the recorded envelopes can all be asserted with no
Redis running:

```python
from faststream_celery.backend import InMemoryResultBackend

backend = broker.config.broker_config.result_backend
assert isinstance(backend, InMemoryResultBackend)
assert backend.results["task-id-1"]["status"] == "SUCCESS"
```

Results are stored serialized, as a real backend stores them — so a handler
returning something a Celery client could not read fails in the test instead of
against a live broker.

## `visibility_timeout`

The one Redis-specific setting that matters. It is how long a
delivered-but-unacknowledged message stays invisible to other consumers; when
it lapses, the message is redelivered. **Every client and worker on the bus must
use the same value** — a shorter one on any side means it redelivers a task
another side is still working on.

```python
CeleryBroker(REDIS_URL, transport_options={"visibility_timeout": 3600})
```

```python
app.conf.broker_transport_options = {"visibility_timeout": 3600}
```

## Result expiry

Result keys are written with a TTL, one day by default, like Celery's
`result_expires`.
