import asyncio
from typing import Any, NamedTuple

from faststream.message import AckStatus, StreamMessage
from faststream.message.source_type import SourceType
from kombu import Message
from typing_extensions import override

from faststream_celery.acknowledgement import Acknowledgement
from faststream_celery.schemas.constants import CONTENT_ENCODING


class ConsumerMessage(NamedTuple):
    """A payload with its delivery controller, or no controller for a result."""

    message: Message
    acknowledgement: Acknowledgement | None = None

    async def settle(self, status: AckStatus) -> None:
        """Wait for transport settlement, or complete a local delivery directly."""
        acknowledgement = self.acknowledgement
        if acknowledgement is None:
            return
        pending = (
            acknowledgement.ack()
            if status is AckStatus.ACKED
            else acknowledgement.reject(requeue=status is AckStatus.NACKED)
        )
        if pending is not None:
            await pending


def local_message(
    body: bytes,
    *,
    content_type: str,
    correlation_id: str,
) -> ConsumerMessage:
    """Shape a payload we produced ourselves like a received message.

    A result read out of a backend never came off a queue, but the pipeline
    still expects the broker's own message type.
    """
    return ConsumerMessage(
        Message(
            body=body,  # type: ignore[arg-type]
            content_type=content_type,
            content_encoding=CONTENT_ENCODING,
            properties={"correlation_id": correlation_id},
        ),
    )


class CeleryMessage(StreamMessage[ConsumerMessage]):
    """A message received from a Celery-compatible queue.

    ``ack()`` / ``nack()`` / ``reject()`` map onto kombu acknowledgements,
    executed on the consumer thread that owns the message's channel.
    """

    def __init__(  # ruff: ignore[too-many-arguments]
        self,
        raw_message: ConsumerMessage,
        body: bytes | Any,
        *,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        content_type: str | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
        source_type: SourceType = SourceType.CONSUME,
    ) -> None:
        super().__init__(
            raw_message=raw_message,
            body=body,
            headers=headers,
            reply_to=reply_to,
            content_type=content_type,
            correlation_id=correlation_id,
            message_id=message_id,
            source_type=source_type,
        )
        self._settlement_lock = asyncio.Lock()

    @property
    def kombu_message(self) -> Message:
        """The kombu message this one was parsed from."""
        return self.raw_message.message

    @override
    async def ack(self) -> None:
        """Acknowledge the message (kombu ``basic_ack``)."""
        await self.settle(AckStatus.ACKED)

    @override
    async def nack(self) -> None:
        """Requeue the same delivery without changing Celery retries or ETA."""
        await self.settle(AckStatus.NACKED)

    @override
    async def reject(self) -> None:
        """Reject the message without requeue."""
        await self.settle(AckStatus.REJECTED)

    async def settle(self, status: AckStatus) -> None:
        """Commit successful settlement once, serializing competing calls."""
        async with self._settlement_lock:
            if self.committed is None:
                await self.raw_message.settle(status)
                self.committed = status
