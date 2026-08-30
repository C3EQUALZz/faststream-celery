import logging
from collections.abc import Iterable
from functools import partial
from typing import TYPE_CHECKING, Any, Optional, cast
from urllib.parse import urlparse

import anyio
from fast_depends import dependency_provider
from faststream.message import gen_cor_id
from faststream.middlewares import AckPolicy
from faststream.response import PublishType
from faststream.specification.schema import BrokerSpec
from kombu import (
    Connection,
    exceptions as kombu_exceptions,
)
from typing_extensions import override

from faststream_celery._internal import (
    EMPTY,
    BrokerUsecase,
    ContextRepo,
    FastDependsConfig,
)
from faststream_celery.configs import CeleryBrokerConfig
from faststream_celery.message import ConsumerMessage
from faststream_celery.middlewares import CeleryResultMiddleware
from faststream_celery.publisher.producer import CeleryFastProducer
from faststream_celery.response import CeleryPublishCommand
from faststream_celery.schemas.task import CelerySendableMessage
from faststream_celery.types import MutableHeaders

from .logging import make_celery_logger_state
from .registrator import CeleryRegistrator

DEFAULT_PING_TIMEOUT = 3.0

if TYPE_CHECKING:
    from collections.abc import Sequence
    from types import TracebackType

    from fast_depends import Provider
    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto
    from faststream.security import BaseSecurity
    from faststream.specification.schema import Tag, TagDict

    from faststream_celery._internal import (
        BrokerMiddleware,
        CodecProto,
        CustomCallable,
        IdGenerator,
    )
    from faststream_celery.message import CeleryMessage


class CeleryBroker(
    CeleryRegistrator,
    BrokerUsecase[ConsumerMessage, Connection],
):
    """A FastStream broker wire-compatible with Celery (over kombu)."""

    def __init__(  # ruff: ignore[too-many-arguments]
        self,
        url: str = "amqp://guest:guest@localhost:5672//",
        *,
        transport_options: dict[str, Any] | None = None,
        ssl: bool | dict[str, Any] | None = None,
        security: Optional["BaseSecurity"] = None,
        max_workers: int = 1,
        prefetch_count: int | None = None,
        # stock FastStream parameters
        middlewares: "Sequence[BrokerMiddleware[Any, Any]]" = (),
        logger: Any = EMPTY,
        log_level: int = logging.INFO,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        serializer: Optional["SerializerProto"] = None,
        dependencies: Iterable["Dependant"] = (),
        graceful_timeout: float | None = 15.0,
        ack_policy: AckPolicy = EMPTY,
        apply_types: bool = True,
        provider: Optional["Provider"] = None,
        context: Optional["ContextRepo"] = None,
        id_generator: "IdGenerator" = gen_cor_id,
        routers: Iterable[CeleryRegistrator] = (),
        # AsyncAPI information
        description: str | None = None,
        tags: "Iterable[Tag | TagDict]" = (),
        protocol_version: str | None = None,
    ) -> None:
        """Initialize the CeleryBroker.

        Args:
            url: kombu connection url, e.g. ``amqp://guest:guest@localhost:5672//``.
            transport_options: kombu transport options (e.g. ``visibility_timeout``
                for the Redis transport).
            ssl: kombu ``ssl`` connection option (bool or a dict of ssl options).
            security: FastStream security object (SSL context, SASL credentials).
            max_workers: Default number of workers processing messages concurrently.
            prefetch_count: kombu QoS prefetch count (`max_workers` by default).
            middlewares: Broker middlewares to apply to all subscribers/publishers.
            logger: Custom logger object.
            log_level: Log level for the broker logger.
            parser: Custom parser to map kombu Message to a FastStream one.
            decoder: Custom decoder to decode the FastStream msg body.
            codec: Custom codec object.
            serializer: Custom serializer object.
            dependencies: Dependencies list (`[Depends(),]`) to apply to all subscribers.
            graceful_timeout: Graceful shutdown timeout.
            ack_policy: Default acknowledgement policy for all subscribers.
            apply_types: Whether to use FastDepends type casting or not.
            provider: Custom dependency provider.
            context: Custom context object.
            id_generator: Custom message id generator.
            routers: Routers to include.
            description: AsyncAPI broker description.
            tags: AsyncAPI broker tags.
            protocol_version: AsyncAPI protocol version.
        """
        producer = CeleryFastProducer(
            parser=parser,
            decoder=decoder,
            id_generator=id_generator,
        )

        config = CeleryBrokerConfig(
            producer=producer,
            url=url,
            transport_options=transport_options,
            ssl=ssl,
            security=security,
            max_workers=max_workers,
            prefetch_count=prefetch_count,
            broker_middlewares=middlewares,
            broker_parser=parser,
            broker_decoder=decoder,
            broker_codec=codec,
            logger=make_celery_logger_state(
                logger=None if logger is EMPTY else logger,
                log_level=log_level,
            ),
            fd_config=FastDependsConfig(
                use_fastdepends=apply_types,
                serializer=serializer or EMPTY,
                provider=provider or dependency_provider,
                context=context or ContextRepo(),
            ),
            broker_dependencies=dependencies,
            graceful_timeout=graceful_timeout,
            ack_policy=ack_policy,
            id_generator=id_generator,
            extra_context={"broker": self},
        )
        config.insert_middleware(
            cast(
                "BrokerMiddleware[ConsumerMessage]",
                partial(CeleryResultMiddleware, config=config),
            ),
        )

        super().__init__(
            routers=routers,
            config=config,
            specification=BrokerSpec(
                description=description,
                url=[url],
                protocol=urlparse(url).scheme or "amqp",
                protocol_version=protocol_version or "custom",
                security=security,
                tags=tags,
            ),
        )

    @override
    async def _connect(self) -> Connection:
        connection = self.config.broker_config.make_connection()
        await self.config.broker_config.connect(connection)
        return connection

    @override
    async def start(self) -> None:
        await self.connect()
        await super().start()

    @override
    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await super().stop(exc_type, exc_val, exc_tb)
        await self.config.disconnect()
        self._connection = None

    @override
    async def publish(
        self,
        message: CelerySendableMessage = None,
        queue: str = "",
        *,
        exchange: str | None = None,
        routing_key: str | None = None,
        headers: MutableHeaders | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
    ) -> None:
        """Publish a Celery task (or a raw message) to a queue.

        Args:
            message: Message body to send. Pass a `CeleryTask` to publish a
                Celery protocol v2 task message a Celery worker can execute.
            queue: Celery queue name to publish to.
            exchange: Exchange name to publish to (the queue name by default,
                mirroring Celery's default queue declaration).
            routing_key: Routing key to publish with (the queue name by default).
            headers: Message headers to store meta-information.
            correlation_id: Manual message correlation_id setter (used as the
                Celery task id for `CeleryTask` messages).
            reply_to: Reply message destination queue name.
        """
        cmd = CeleryPublishCommand(
            message,
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            headers=headers,
            correlation_id=correlation_id or self.config.id_generator(),
            reply_to=reply_to,
            _publish_type=PublishType.PUBLISH,
        )

        await super()._basic_publish(
            cmd,
            producer=self.config.producer,
        )

    @override
    async def request(  # type: ignore[override]
        self,
        message: CelerySendableMessage,
        queue: str = "",
        *,
        exchange: str | None = None,
        routing_key: str | None = None,
        correlation_id: str | None = None,
        headers: MutableHeaders | None = None,
        timeout: float | None = 30.0,
    ) -> "CeleryMessage":
        """Publish a Celery task and wait for its result (AMQP RPC).

        The task is published with a `reply_to` pointing at a temporary
        exclusive queue; the Celery result envelope that a worker publishes
        there is decoded and returned.

        Args:
            message: Message body to send, usually a `CeleryTask`.
            queue: Celery queue name to publish to.
            exchange: Exchange name to publish to (the queue name by default).
            routing_key: Routing key to publish with (the queue name by default).
            correlation_id: Manual message correlation_id setter (used as the
                Celery task id for `CeleryTask` messages).
            headers: Message headers to store meta-information.
            timeout: Seconds to wait for the reply before raising `TimeoutError`.

        Returns:
            CeleryMessage: The reply message; `await msg.decode()` gives the
                Celery result envelope.
        """
        cmd = CeleryPublishCommand(
            message,
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            headers=headers,
            correlation_id=correlation_id or self.config.id_generator(),
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
        )

        msg: CeleryMessage = await super()._basic_request(
            cmd,
            producer=self.config.producer,
        )
        return msg

    @override
    async def ping(self, timeout: float | None = 3) -> bool:
        connection = self._connection
        if connection is None:
            return False

        try:
            with anyio.fail_after(timeout or DEFAULT_PING_TIMEOUT):
                return await self.config.broker_config.is_alive(connection)

        except (OSError, kombu_exceptions.OperationalError, TimeoutError):
            return False
