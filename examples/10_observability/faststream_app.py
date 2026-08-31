"""Tracing and metrics for Celery tasks.

Both middlewares label their output with the Celery task name, so two tasks
sharing one queue stay apart in a trace and in a metric — which a queue-only
label would not give you.

* spans go to stdout through the OpenTelemetry console exporter;
* metrics are served on http://localhost:9000/metrics.

Requires the extras:
    pip install "faststream-celery[otel,prometheus]"

Run:
    faststream run faststream_app.py:app          # or: python faststream_app.py
"""

import asyncio
from typing import Literal

from faststream import FastStream
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from prometheus_client import CollectorRegistry, start_http_server
from pydantic import BaseModel, PositiveInt

from faststream_celery import CeleryBroker
from faststream_celery.annotations import Logger
from faststream_celery.message import ConsumerMessage
from faststream_celery.opentelemetry import CeleryTelemetryMiddleware
from faststream_celery.prometheus import CeleryPrometheusMiddleware

BROKER_URL = "amqp://guest:guest@localhost:5672//"
QUEUE = "celery"
METRICS_PORT = 9000

NoArgs = tuple[()]


class Charge(BaseModel):
    """`kwargs` of `examples.charge`."""

    order_id: PositiveInt
    amount_cents: PositiveInt


class Charged(BaseModel):
    status: Literal["captured"] = "captured"
    order_id: int


def task_label(msg: ConsumerMessage | None) -> str:
    """The Celery task name of a message, as a metric label.

    The stock labels stop at the queue, so `examples.charge` and
    `examples.refund` — which share one queue — would be a single series.
    `None` is the publish side, where there is no incoming message.

    One label per task name is one series per task name: fine for a fixed set
    of tasks, worth thinking about if task names carry an id.
    """
    if msg is None:
        return ""

    return str((msg.message.headers or {}).get("task", ""))


def make_tracer_provider() -> TracerProvider:
    """A tracer provider printing spans to stdout.

    Swap `ConsoleSpanExporter` for an OTLP exporter to send them somewhere.
    """
    provider = TracerProvider(
        resource=Resource.create({"service.name": "faststream-celery-example"}),
    )
    provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    return provider


registry = CollectorRegistry()

broker = CeleryBroker(
    BROKER_URL,
    middlewares=(
        CeleryTelemetryMiddleware(tracer_provider=make_tracer_provider()),
        CeleryPrometheusMiddleware(
            registry=registry,
            app_name="payments",
            custom_labels={"task": task_label},
        ),
    ),
)
app = FastStream(broker)


@broker.subscriber(QUEUE, task="examples.charge")
async def charge(args: NoArgs, kwargs: Charge, task_logger: Logger) -> Charged:
    task_logger.info("charging order %s", kwargs.order_id)
    return Charged(order_id=kwargs.order_id)


@broker.subscriber(QUEUE, task="examples.refund")
async def refund(args: tuple[PositiveInt], kwargs: dict[str, str]) -> str:
    """A second task on the same queue, told apart by the task-name label."""
    (order_id,) = args
    return f"rf_{order_id}"


@app.after_startup
async def serve_metrics() -> None:
    start_http_server(METRICS_PORT, registry=registry)
    print(f"metrics on http://localhost:{METRICS_PORT}/metrics")


if __name__ == "__main__":
    asyncio.run(app.run())
