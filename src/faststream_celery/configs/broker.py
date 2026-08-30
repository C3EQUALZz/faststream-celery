from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import anyio.to_thread
from faststream.exceptions import IncorrectState
from kombu import Connection

from faststream_celery._internal import BrokerConfig, DefaultCodec
from faststream_celery.security import parse_security

if TYPE_CHECKING:

    from faststream.security import BaseSecurity

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

    def make_connection(self) -> Connection:
        """Build a fresh kombu connection (each consumer thread gets its own)."""
        security_options = parse_security(self.security)
        ssl = self.ssl if self.ssl is not None else security_options.pop("ssl", None)
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
            serializer=self.fd_config._serializer,  # ruff: ignore[private-member-access]
            codec=self.broker_codec or DefaultCodec(),
        )

    async def disconnect(self) -> None:
        await self.producer.disconnect()


@dataclass(kw_only=True)
class CeleryRouterConfig(BrokerConfig):
    max_workers: int = 1
    prefetch_count: int | None = None

    def make_connection(self) -> Connection:
        raise IncorrectState
