from typing import Annotated

from faststream_celery.broker import (
    CeleryBroker as CB,  # ruff: ignore[camelcase-imported-as-acronym]
)
from faststream_celery.fastapi._internal import Context
from faststream_celery.message import (
    CeleryMessage as CM,  # ruff: ignore[camelcase-imported-as-acronym]
)
from faststream_celery.publisher.producer import CeleryFastProducer

__all__ = (
    "CeleryBroker",
    "CeleryMessage",
    "CeleryProducer",
)

CeleryMessage = Annotated[CM, Context("message")]
CeleryBroker = Annotated[CB, Context("broker")]
CeleryProducer = Annotated[CeleryFastProducer, Context("broker.config.producer")]
