# 8. Scheduling: countdown, eta, expires

The same three `apply_async` options, honoured in both directions.

## Run

Celery schedules, FastStream runs:

```bash
docker compose -f ../docker-compose.yaml up -d rabbitmq

faststream run faststream_consumer.py:app     # terminal 1
python celery_client.py                        # terminal 2
```

FastStream schedules, Celery runs:

```bash
celery -A celery_worker worker --loglevel=info -Q celery     # terminal 1
faststream run faststream_publisher.py:app                    # terminal 2
```

## Expected output

```
$ python celery_client.py
countdown -> {'status': 'sent', 'label': 'countdown 5.0s', 'late_by_seconds': 0.01} (waited 5.1s)
eta -> {'status': 'sent', 'label': 'eta 2026-08-31T...', 'late_by_seconds': 0.02} (waited 8.1s)
expired -> queued and dropped; the consumer logs nothing for it
```

`late_by_seconds` is how long after its `eta` the handler actually ran — the
scheduler aims at the due moment, it does not merely sleep.

## How it works

* A task with a future `eta` (or a `countdown`, which is turned into one) is
  held by an in-memory scheduler inside the subscriber and passed to the
  handler when due. Same as a Celery worker — and with the same consequence: a
  restart loses what was still waiting, on either side.
* `expires` is checked before the handler runs. An expired task is dropped and
  acknowledged, never handled.
* `countdown` and `eta` are mutually exclusive; passing both raises a
  `SetupError`.
* A naive `datetime` is read as UTC, the way Celery does with `enable_utc`.
* Hold time is not a durability mechanism. For anything that must survive a
  restart, keep the schedule in a store and publish when it is due — or use
  Celery beat on the Celery side.
