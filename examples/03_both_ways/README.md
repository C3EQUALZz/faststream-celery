# 3. Both ways in one loop

One Celery call crosses the boundary four times:

```
celery_client.py   --examples.order_placed-->  faststream_service.py   (queue: orders)
faststream_service --examples.charge------->   celery_worker.py        (queue: payments)
celery_worker      --result--------------->    faststream_service
faststream_service --result--------------->    celery_client.py
```

The FastStream service is a Celery worker and a Celery client at the same time,
and the two stock Celery processes at either end cannot tell that the thing in
the middle is not a Celery worker.

## Run

```bash
docker compose -f ../docker-compose.yaml up -d rabbitmq

# terminal 1 — payments: a stock Celery worker
celery -A celery_worker worker --loglevel=info -Q payments

# terminal 2 — orders: the FastStream service
faststream run faststream_service.py:app     # or: python faststream_service.py

# terminal 3 — a stock Celery client
python celery_client.py
python celery_client.py declined
python celery_client.py cancelled
```

## Expected output

```
$ python celery_client.py
examples.order_placed -> {'order_id': 1234, 'charged': True, 'charge_id': 'ch_1234', 'error': None}

$ python celery_client.py declined
examples.order_placed (declined) -> {'order_id': 1235, 'charged': False, 'charge_id': None, 'error': 'ValueError'}
```

The payments worker logs `charging 4999 EUR for 1234`, and on `cancelled`,
`refunding order 1234`.

## What makes it work

* Consuming and publishing share one broker: `broker.request()` inside a
  handler publishes on its own short-lived connection, so waiting for the
  Celery worker does not block the subscriber's own consumer.
* The declined charge shows what a `FAILURE` envelope is good for: the
  FastStream service reads the failure as data (`status`, `exc_type`) and
  answers the original caller instead of dying with it.
* Each hop validates its own payload with its own Pydantic model. The service
  in the middle re-validates on the way out, so a bad field cannot be
  forwarded into a worker you did not deploy.
