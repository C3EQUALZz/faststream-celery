# 10. Observability: tracing, metrics, AsyncAPI

```bash
pip install "faststream-celery[otel,prometheus]"
docker compose -f ../docker-compose.yaml up -d rabbitmq

faststream run faststream_app.py:app     # terminal 1
python celery_client.py                   # terminal 2
curl -s localhost:9000/metrics | grep faststream
python asyncapi.py > asyncapi.json
```

## Tracing

`CeleryTelemetryMiddleware` produces a consume span per task, linked to the
producer's span when the Celery client propagated a trace context. The span
carries the **Celery task name**, so `examples.charge` and `examples.refund` —
which share one queue — are separate operations in a trace instead of one
`celery` blob.

The example prints spans with the console exporter; swap it for an OTLP
exporter to send them to a collector.

## Metrics

`CeleryPrometheusMiddleware` exposes the standard FastStream metrics —
received/processed counters, processing duration, message size, exceptions.
The stock labels stop at the **queue** (`handler`), which on Celery is not
enough: two task names share one queue and would be one series. The example
adds the task name with `custom_labels`:

```python
CeleryPrometheusMiddleware(
    registry=registry,
    app_name="payments",
    custom_labels={"task": task_label},   # reads headers["task"]
)
```

```
faststream_received_messages_total{app_name="payments",broker="celery",handler="celery",task="examples.charge"} 2.0
faststream_received_messages_total{app_name="payments",broker="celery",handler="celery",task="examples.refund"} 2.0
```

Without that label the two tasks add up into one `handler="celery"` series.
One series per task name is cheap for a fixed set of tasks — think twice if a
task name carries an id.

`registry=` is required, so the metrics land in a registry you own (and can
mount wherever your app already serves `/metrics`).

## AsyncAPI

`asyncapi.py` builds the schema from the broker, no connection needed. Each
subscriber and publisher becomes a channel with its queue, exchange and routing
key. With the CLI installed:

```bash
faststream docs gen faststream_app:app
faststream docs serve faststream_app:app
```

Subscribers take their description from the handler docstring; `title=`,
`description=` and `include_in_schema=` on `subscriber()` / `publisher()`
override that.
