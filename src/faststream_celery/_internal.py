"""Adapter over the private ``faststream._internal`` API.

Every import from ``faststream._internal`` in this package goes through this
module (see ``docs/design.md`` §12), so a breaking change in the private API
has a single point of repair.
"""

from typing import TYPE_CHECKING

from faststream._internal._compat import dump_json
from faststream._internal.basic_types import (
    AsyncFuncAny,
    DecodedMessage,
    LoggerProto,
    SendableMessage,
)
from faststream._internal.broker import BrokerRouter, BrokerUsecase
from faststream._internal.broker.registrator import Registrator
from faststream._internal.broker.router import ArgsContainer, SubscriberRoute
from faststream._internal.configs import (
    BrokerConfig,
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream._internal.context import Context
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper
from faststream._internal.endpoint.publisher import (
    PublisherProto,
    PublisherSpecification,
    PublisherUsecase,
)
from faststream._internal.endpoint.publisher.fake import FakePublisher
from faststream._internal.endpoint.subscriber import (
    SubscriberSpecification,
    SubscriberUsecase,
)
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream._internal.endpoint.subscriber.mixins import (
    ConcurrentMixin,
    TasksMixin,
)
from faststream._internal.endpoint.subscriber.utils import default_filter
from faststream._internal.endpoint.utils import ParserComposition, process_msg
from faststream._internal.logger import (
    DefaultLoggerStorage,
    LoggerState,
    make_logger_state,
)
from faststream._internal.logger.logging import get_broker_logger
from faststream._internal.parser import CodecProto, DefaultCodec
from faststream._internal.producer import ProducerProto
from faststream._internal.testing.broker import (
    EnterType,
    TestBroker,
    change_producer,
)
from faststream._internal.types import (
    BrokerMiddleware,
    CustomCallable,
    Filter,
    IdGenerator,
    P_HandlerParams,
    PublisherMiddleware,
    T_HandlerReturn,
)

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

__all__ = (
    "EMPTY",
    "ArgsContainer",
    "AsyncFuncAny",
    "BrokerConfig",
    "BrokerMiddleware",
    "BrokerRouter",
    "BrokerUsecase",
    "CallsCollection",
    "CodecProto",
    "ConcurrentMixin",
    "Context",
    "ContextRepo",
    "CustomCallable",
    "DecodedMessage",
    "DefaultCodec",
    "DefaultLoggerStorage",
    "EnterType",
    "FakePublisher",
    "FastDependsConfig",
    "Filter",
    "HandlerCallWrapper",
    "IdGenerator",
    "LoggerProto",
    "LoggerState",
    "P_HandlerParams",
    "ParserComposition",
    "ProducerProto",
    "PublisherMiddleware",
    "PublisherProto",
    "PublisherSpecification",
    "PublisherSpecificationConfig",
    "PublisherUsecase",
    "PublisherUsecaseConfig",
    "Registrator",
    "SendableMessage",
    "SubscriberRoute",
    "SubscriberSpecification",
    "SubscriberSpecificationConfig",
    "SubscriberUsecase",
    "SubscriberUsecaseConfig",
    "T_HandlerReturn",
    "TasksMixin",
    "TestBroker",
    "change_producer",
    "default_filter",
    "dump_json",
    "get_broker_logger",
    "make_logger_state",
    "process_msg",
    "resolve_serializer",
)


def resolve_serializer(fd_config: FastDependsConfig) -> "SerializerProto | None":
    """The serializer a config actually uses, with its ``EMPTY`` default applied.

    ``FastDependsConfig.serializer`` may still hold the ``EMPTY`` sentinel;
    only the private property resolves it to the real default serializer.
    """
    return fd_config._serializer
