from dataclasses import dataclass
from typing import TYPE_CHECKING

from faststream.middlewares import AckPolicy

from faststream_celery._internal import (
    EMPTY,
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)

if TYPE_CHECKING:
    from faststream_celery.configs import CeleryBrokerConfig


@dataclass(kw_only=True)
class CelerySubscriberSpecificationConfig(SubscriberSpecificationConfig):
    queue: str
    task: str | None = None


@dataclass(kw_only=True)
class CelerySubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "CeleryBrokerConfig"

    queue: str
    task: str | None = None
    prefetch_count: int = 1

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            if self._outer_config.ack_policy is not EMPTY:
                return self._outer_config.ack_policy
            return AckPolicy.REJECT_ON_ERROR

        return self._ack_policy
