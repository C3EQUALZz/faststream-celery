from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Union

from faststream.exceptions import FeatureNotSupportedException
from faststream.response import PublishCommand, PublishType
from typing_extensions import override

from faststream_celery._internal import PublisherUsecase
from faststream_celery.response import CeleryPublishCommand
from faststream_celery.schemas.task import CelerySendableMessage
from faststream_celery.types import MutableHeaders

if TYPE_CHECKING:
    from faststream_celery._internal import (
        PublisherMiddleware,
        PublisherSpecification,
    )
    from faststream_celery.message import CeleryMessage

    from .config import CeleryPublisherConfig


class CeleryPublisher(PublisherUsecase):
    """A long-living, AsyncAPI-documented publisher of Celery tasks."""

    def __init__(
        self,
        config: "CeleryPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specification)

        self.config = config

        self.exchange = config.exchange
        self.routing_key = config.routing_key
        self.headers = config.headers or {}
        self.reply_to = config.reply_to

    @property
    def queue(self) -> str:
        return f"{self._outer_config.prefix}{self.config.queue}"

    @override
    async def publish(
        self,
        message: CelerySendableMessage = None,
        queue: str | None = None,
        *,
        exchange: str | None = None,
        routing_key: str | None = None,
        headers: MutableHeaders | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
    ) -> None:
        """Publish a Celery task (or raw message) to a queue."""
        cmd = CeleryPublishCommand(
            message,
            queue=queue or self.queue,
            exchange=exchange or self.exchange,
            routing_key=routing_key or self.routing_key,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or self._outer_config.id_generator(),
            reply_to=reply_to or self.reply_to,
            _publish_type=PublishType.PUBLISH,
        )
        await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=(),
        )

    @override
    async def _publish(
        self,
        cmd: Union["CeleryPublishCommand", "PublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """This method should be called in subscriber flow only."""
        cmd = CeleryPublishCommand.from_cmd(cmd, queue=self.queue)

        cmd.add_headers(self.headers, override=False)
        cmd.reply_to = cmd.reply_to or self.reply_to

        await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=_extra_middlewares,
        )

    @override
    async def request(
        self,
        message: CelerySendableMessage = None,
        queue: str | None = None,
        *,
        correlation_id: str | None = None,
        headers: MutableHeaders | None = None,
        timeout: float | None = 30.0,
    ) -> "CeleryMessage":
        msg = "CeleryBroker doesn't support RPC requests yet."
        raise FeatureNotSupportedException(msg)
