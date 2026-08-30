from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from faststream.prometheus.middleware import PrometheusMiddleware

from faststream_celery._internal import EMPTY
from faststream_celery.message import ConsumerMessage
from faststream_celery.prometheus.provider import CeleryMetricsSettingsProvider
from faststream_celery.response import CeleryPublishCommand

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry


class CeleryPrometheusMiddleware(
    PrometheusMiddleware[ConsumerMessage, CeleryPublishCommand],
):
    def __init__(
        self,
        *,
        registry: "CollectorRegistry",
        app_name: str = EMPTY,
        metrics_prefix: str = "faststream",
        received_messages_size_buckets: Sequence[float] | None = None,
        custom_labels: dict[str, str | Callable[[Any], str]] | None = None,
    ) -> None:
        super().__init__(
            settings_provider_factory=lambda _: CeleryMetricsSettingsProvider(),
            registry=registry,
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            received_messages_size_buckets=received_messages_size_buckets,
            custom_labels=custom_labels,
        )
