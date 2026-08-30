from typing import TYPE_CHECKING, Any

from .config import CeleryPublisherConfig, CeleryPublisherSpecificationConfig
from .specification import CeleryPublisherSpecification
from .usecase import CeleryPublisher

if TYPE_CHECKING:
    from faststream_celery.configs import CeleryBrokerConfig


def create_publisher(  # ruff: ignore[too-many-arguments]
    *,
    queue: str,
    exchange: str | None,
    routing_key: str | None,
    headers: dict[str, Any] | None,
    reply_to: str,
    config: "CeleryBrokerConfig",
    # AsyncAPI args
    title_: str | None,
    description_: str | None,
    schema_: Any | None,
    include_in_schema: bool,
) -> CeleryPublisher:
    publisher_config = CeleryPublisherConfig(
        queue=queue,
        exchange=exchange,
        routing_key=routing_key,
        headers=headers,
        reply_to=reply_to,
        _outer_config=config,
    )

    specification = CeleryPublisherSpecification(
        config,
        CeleryPublisherSpecificationConfig(
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            title_=title_,
            description_=description_,
            schema_=schema_,
            include_in_schema=include_in_schema,
        ),
    )

    return CeleryPublisher(publisher_config, specification)
