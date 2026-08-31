# 9. Routers: splitting an app by domain

`CeleryRouter` groups subscribers and publishers and prefixes their queue
names, so a module owns its queues without repeating the prefix. Routers are
included into a broker (`CeleryBroker(routers=(...))` or
`broker.include_router(...)`), or into another router.

## Run

```bash
docker compose -f ../docker-compose.yaml up -d rabbitmq

faststream run faststream_app.py:app     # terminal 1
python celery_client.py                   # terminal 2
```

## Expected output

```
imaging.make_thumbnail -> {'status': 'ready', 'upload_id': 'upload-7', 'size': 512}
  (the handler also published imaging.export to imaging.exports)
billing.issue_invoice -> {'invoice_id': 'inv_1234', 'cents': 4999}
```

The app log shows `thumbnail upload-7 at 512`, then `exporting upload-7` — the
second one from a task the first handler published.

## What to notice

* The prefix applies to **queue names**, not to task names. A Celery producer
  sends to `imaging.thumbnails` and calls `imaging.make_thumbnail`; the two
  happen to share a prefix here only because it reads well.
* A publisher created on a router inherits the prefix too, so
  `imaging.publisher("exports")` publishes to `imaging.exports`.
* Two registration styles: the `@router.subscriber(...)` decorator, and
  `CeleryRoute(handler, queue, task=...)` passed to `CeleryRouter(handlers=...)`
  when the handler function comes from elsewhere. `CeleryRoute` and
  `CeleryPublisherArgs` are importable from `faststream_celery.broker`.
* `dependencies=` and `middlewares=` on a router apply to every handler in it —
  one place for the auth check or the tracing every subscriber in a domain
  needs.
