from typing import Annotated

from faststream.annotations import ContextRepo, Logger
from faststream.params import NoCast
from kombu import Connection as KombuConnection

from faststream_celery._internal import Context
from faststream_celery.broker import (
    CeleryBroker as CB,  # ruff: ignore[camelcase-imported-as-acronym]
)
from faststream_celery.message import (
    CeleryMessage as CM,  # ruff: ignore[camelcase-imported-as-acronym]
)
from faststream_celery.publisher.producer import CeleryFastProducer

__all__ = (
    "CeleryBroker",
    "CeleryMessage",
    "CeleryProducer",
    "Connection",
    "ContextRepo",
    "Logger",
    "NoCast",
)

CeleryMessage = Annotated[CM, Context("message")]
CeleryBroker = Annotated[CB, Context("broker")]
CeleryProducer = Annotated[CeleryFastProducer, Context("broker.config.producer")]

# The kombu connection used for publishing; each subscriber reads on its own.
Connection = Annotated[KombuConnection, Context("broker._connection")]
