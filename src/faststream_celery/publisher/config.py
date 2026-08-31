from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from faststream_celery._internal import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)

if TYPE_CHECKING:
    from faststream_celery.configs import CeleryBrokerConfig


@dataclass(kw_only=True)
class CeleryPublisherSpecificationConfig(PublisherSpecificationConfig):
    queue: str
    exchange: str | None = None
    routing_key: str | None = None
    reply_to: str = ""


@dataclass(kw_only=True)
class CeleryPublisherConfig(PublisherUsecaseConfig):
    _outer_config: "CeleryBrokerConfig"

    queue: str
    exchange: str | None = None
    routing_key: str | None = None
    headers: dict[str, Any] | None = None
    reply_to: str = ""
