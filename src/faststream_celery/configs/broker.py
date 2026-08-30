from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import anyio.to_thread
from faststream.exceptions import IncorrectState
from kombu import Connection

from faststream_celery._internal import BrokerConfig, DefaultCodec, resolve_serializer
from faststream_celery.security import parse_security
from faststream_celery.subscriber.consumer import ConsumerRegistry

if TYPE_CHECKING:
    from faststream.security import BaseSecurity

    from faststream_celery.backend import ResultBackend
    from faststream_celery.publisher.producer import CeleryFastProducer


@dataclass(kw_only=True)
class CeleryBrokerConfig(BrokerConfig):
    producer: "CeleryFastProducer"

    url: str
    transport_options: dict[str, Any] | None = None
    ssl: bool | dict[str, Any] | None = None
    security: "BaseSecurity | None" = None

    # Subscriber defaults; a subscriber-level value takes precedence.
    max_workers: int = 1
    prefetch_count: int | None = None

    # One kombu consumer per queue, shared by the subscribers on it.
    consumers: ConsumerRegistry = field(default_factory=ConsumerRegistry)

    # Where task results are stored; `None` leaves them on the broker.
    result_backend: "ResultBackend | None" = None

    @property
    def virtual_host(self) -> str:
        """AMQP virtual host from the connection url (``/`` by default)."""
        return urlparse(self.url).path.lstrip("/") or "/"

    def make_connection(self) -> Connection:
        """Build a fresh kombu connection (each consumer thread gets its own)."""
        security_options = parse_security(self.security)

        # `ssl=` is the kombu-level escape hatch, so it wins over whatever the
        # security object asked for.
        security_ssl = security_options.pop("ssl", None)
        ssl = self.ssl if self.ssl is not None else security_ssl

        return Connection(
            self.url,
            transport_options=self.transport_options,
            ssl=ssl,
            **security_options,
        )

    async def connect(self, connection: Connection) -> None:
        """Establish the write connection and wire the producer to it."""
        await anyio.to_thread.run_sync(connection.connect)
        self.producer.connect(
            connection,
            connection_factory=self.make_connection,
            serializer=resolve_serializer(self.fd_config),
            codec=self.broker_codec or DefaultCodec(),
        )

        if self.result_backend is not None:
            await self.result_backend.connect()

    async def disconnect(self) -> None:
        await self.producer.disconnect()

        if self.result_backend is not None:
            await self.result_backend.disconnect()

    async def is_alive(self, connection: Connection) -> bool:
        """Whether the connection still reaches the broker."""
        return await anyio.to_thread.run_sync(_check_connection, connection)


def _check_connection(connection: Connection) -> bool:
    """Runs on a worker thread — kombu connections are not thread-safe."""
    connection.ensure_connection(max_retries=0)
    return bool(connection.connected)


@dataclass(kw_only=True)
class CeleryRouterConfig(BrokerConfig):
    max_workers: int = 1
    prefetch_count: int | None = None

    def make_connection(self) -> Connection:
        raise IncorrectState
