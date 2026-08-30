from typing import TYPE_CHECKING, Any, TypeAlias

from faststream_celery._internal import CallsCollection

from .config import CelerySubscriberConfig, CelerySubscriberSpecificationConfig
from .specification import CelerySubscriberSpecification
from .usecase import CeleryConcurrentSubscriber, CelerySubscriber

if TYPE_CHECKING:
    from faststream.middlewares import AckPolicy

    from faststream_celery.configs import CeleryBrokerConfig

SubscriberType: TypeAlias = CelerySubscriber


def create_subscriber(  # ruff: ignore[too-many-arguments]
    *,
    queue: str,
    task: str | None,
    # Subscriber args
    max_workers: int | None,
    prefetch_count: int | None,
    ack_policy: "AckPolicy",
    no_reply: bool,
    config: "CeleryBrokerConfig",
    # AsyncAPI args
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> SubscriberType:
    workers = max_workers or config.max_workers
    prefetch = prefetch_count or config.prefetch_count or workers

    subscriber_config = CelerySubscriberConfig(
        queue=queue,
        task=task,
        prefetch_count=prefetch,
        no_reply=no_reply,
        _outer_config=config,
        _ack_policy=ack_policy,
    )

    calls: CallsCollection[Any] = CallsCollection()

    specification = CelerySubscriberSpecification(
        config,
        CelerySubscriberSpecificationConfig(
            queue=queue,
            task=task,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        ),
        calls,
    )

    if workers > 1:
        return CeleryConcurrentSubscriber(
            subscriber_config,
            specification,
            calls,
            max_workers=workers,
        )

    return CelerySubscriber(subscriber_config, specification, calls)
