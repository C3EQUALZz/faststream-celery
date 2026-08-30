from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any, NamedTuple, TypeAlias

from faststream.message import StreamMessage
from kombu import Message
from typing_extensions import override

# Runs a kombu acknowledgement callable on the consumer thread that owns
# the message's channel (kombu transports are not thread-safe).
AckExecutor: TypeAlias = Callable[[Callable[[], None]], Awaitable[None]]


class ConsumerMessage(NamedTuple):
    """A kombu message handed over from the consumer thread to the event loop.

    Carries the ack executor alongside the raw message so that
    ``CeleryMessage`` can marshal acknowledgements back to the thread owning
    the kombu channel.
    """

    message: Message
    executor: AckExecutor


async def run_inline(action: Callable[[], None]) -> None:  # ruff: ignore[unused-async]
    """Ack executor for messages with no consumer thread to hop onto.

    Used for RPC replies and by the in-memory test broker, where the
    kombu object is either short-lived or a stand-in.
    """
    action()


class CeleryMessage(StreamMessage[Message]):
    """A message received from a Celery-compatible queue.

    ``ack()`` / ``nack()`` / ``reject()`` map onto kombu acknowledgements,
    executed on the consumer thread that owns the message's channel.
    """

    def __init__(  # ruff: ignore[too-many-arguments]
        self,
        raw_message: Message,
        body: bytes | Any,
        *,
        ack_executor: AckExecutor,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        content_type: str | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
    ) -> None:
        super().__init__(
            raw_message=raw_message,
            body=body,
            headers=headers,
            reply_to=reply_to,
            content_type=content_type,
            correlation_id=correlation_id,
            message_id=message_id,
        )
        self._ack_executor = ack_executor

    @override
    async def ack(self) -> None:
        """Acknowledge the message (kombu ``basic_ack``)."""
        if self.committed is None:
            await super().ack()
            await self._ack_executor(self.raw_message.ack)

    @override
    async def nack(self) -> None:
        """Reject the message with requeue (Celery task retry semantics)."""
        if self.committed is None:
            await super().nack()
            await self._ack_executor(partial(self.raw_message.reject, requeue=True))

    @override
    async def reject(self) -> None:
        """Reject the message without requeue."""
        if self.committed is None:
            await super().reject()
            await self._ack_executor(partial(self.raw_message.reject, requeue=False))
