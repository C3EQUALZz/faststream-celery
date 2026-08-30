from typing import Any

from faststream.exceptions import SetupError
from faststream.response import PublishCommand, PublishType


class CeleryPublishCommand(PublishCommand):
    """DTO carrying everything needed to publish a Celery task message."""

    def __init__(  # ruff: ignore[too-many-arguments]
        self,
        message: Any = None,
        /,
        *,
        _publish_type: PublishType,
        correlation_id: str | None = None,
        queue: str = "",
        exchange: str | None = None,
        routing_key: str | None = None,
        declare: bool = True,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        timeout: float | None = 30.0,
    ) -> None:
        if not queue:
            msg = "You should specify `queue` to publish a Celery task to."
            raise SetupError(msg)

        super().__init__(
            message,
            _publish_type=_publish_type,
            correlation_id=correlation_id,
            destination=queue,
            reply_to=reply_to,
            headers=headers,
        )

        # An empty exchange name is meaningful (the AMQP default exchange,
        # used for RPC replies), so None is the "not set" marker here.
        self.exchange = exchange
        self.routing_key = routing_key
        self.declare = declare

        # Request option (reserved for RPC, see ticket-3)
        self.timeout = timeout

    @property
    def queue(self) -> str:
        return self.destination

    @classmethod
    def from_cmd(
        cls,
        cmd: "PublishCommand",
        *,
        queue: str = "",
    ) -> "CeleryPublishCommand":
        if isinstance(cmd, CeleryPublishCommand):
            return cmd

        return cls(
            cmd.body,
            queue=queue or cmd.destination,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
            reply_to=cmd.reply_to,
            _publish_type=cmd.publish_type,
        )
