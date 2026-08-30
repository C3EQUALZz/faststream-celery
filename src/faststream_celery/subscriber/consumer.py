"""One kombu consumer per queue, shared by every subscriber on it.

Celery routes by task name *inside* a queue, so several subscribers normally
share one. Giving each its own broker consumer would make the broker
round-robin messages between them and a subscriber would drop whatever it
cannot handle, so a queue is read once and each message goes to the
subscriber whose ``task=`` claims it.
"""

import asyncio
import logging
from collections.abc import Callable
from contextlib import suppress
from functools import partial
from typing import TYPE_CHECKING, Protocol

from faststream.exceptions import IncorrectState

from faststream_celery.exceptions import DECODE_ERRORS, SETTLE_ERRORS
from faststream_celery.message import ConsumerMessage
from faststream_celery.parser import read_headers

from .bridge import ConsumerBridge

if TYPE_CHECKING:
    from kombu import Connection

    from faststream_celery._internal import LoggerState


class QueueSubscriber(Protocol):
    """What a shared consumer needs to know about a subscriber."""

    @property
    def task(self) -> str | None:
        """Celery task name to claim, or ``None`` to accept every task."""
        ...

    async def dispatch(self, msg: ConsumerMessage) -> None:
        """Handle one message from the queue."""
        ...


class SharedConsumer:
    """The single kombu consumer standing behind one queue."""

    def __init__(self, bridge: ConsumerBridge, logger: "LoggerState") -> None:
        self.bridge = bridge

        self._logger = logger
        self._subscribers: list[QueueSubscriber] = []
        self._reader: asyncio.Task[None] | None = None

    def register(self, subscriber: QueueSubscriber) -> None:
        """Add a subscriber to the fan-out."""
        if subscriber not in self._subscribers:
            self._subscribers.append(subscriber)

    async def start(self) -> None:
        """Start the kombu consumer, and the routing task once anyone is registered."""
        await self.bridge.start()

        if self._reader is None and self._subscribers:
            self._reader = asyncio.create_task(self._route_forever())

    async def unregister(self, subscriber: QueueSubscriber) -> bool:
        """Detach a subscriber, reporting whether the consumer has now stopped."""
        with suppress(ValueError):
            self._subscribers.remove(subscriber)

        if self._subscribers:
            return False

        await self.stop()
        return True

    async def stop(self) -> None:
        """Stop routing and close the kombu consumer."""
        reader, self._reader = self._reader, None
        if reader is not None:
            reader.cancel()
            with suppress(asyncio.CancelledError):
                await reader

        await self.bridge.stop()

    async def _route_forever(self) -> None:
        while True:
            msg = await self.bridge.get()

            try:
                await self._route(msg)
            except Exception as exc:
                # The one deliberate catch-all in the package: this task is
                # the queue's pump, and letting it die would stop the queue
                # with nothing said about it.
                self._logger.log(
                    f"Failed to route a message: {exc!r}",
                    logging.ERROR,
                    exc_info=exc,
                )

    async def _route(self, msg: ConsumerMessage) -> None:
        subscriber = self._claimant(msg)

        if subscriber is None:
            await self._drop(msg)
            return

        await subscriber.dispatch(msg)

    def _claimant(self, msg: ConsumerMessage) -> QueueSubscriber | None:
        task_name = _task_name(msg)

        for subscriber in self._subscribers:
            if subscriber.task == task_name:
                return subscriber

        return next((s for s in self._subscribers if s.task is None), None)

    async def _drop(self, msg: ConsumerMessage) -> None:
        """Reject a task no subscriber claims.

        Nothing would ever settle it otherwise, and an unsettled message
        holds a slot in the prefetch window. A Celery worker drops an
        unknown task the same way.
        """
        with suppress(*SETTLE_ERRORS, IncorrectState):
            await msg.executor(partial(msg.message.reject, requeue=False))


class ConsumerRegistry:
    """A broker's shared consumers, keyed by queue name."""

    def __init__(self) -> None:
        self._consumers: dict[str, SharedConsumer] = {}

    def acquire(
        self,
        *,
        queue: str,
        connection_factory: Callable[[], "Connection"],
        accept: list[str],
        prefetch_count: int,
        logger: "LoggerState",
    ) -> SharedConsumer:
        """The consumer for ``queue``, creating it on first use.

        Subscribers sharing a queue may ask for different prefetch counts;
        the widest window wins, since a narrower one would throttle the
        others.
        """
        if (consumer := self._consumers.get(queue)) is not None:
            consumer.bridge.raise_prefetch(prefetch_count)
            return consumer

        consumer = SharedConsumer(
            ConsumerBridge(
                connection_factory=connection_factory,
                queue_name=queue,
                accept=accept,
                prefetch_count=prefetch_count,
            ),
            logger,
        )
        self._consumers[queue] = consumer
        return consumer

    async def release(self, queue: str, subscriber: QueueSubscriber) -> None:
        """Detach a subscriber, closing the consumer once the last one leaves."""
        consumer = self._consumers.get(queue)
        if consumer is None:
            return

        if await consumer.unregister(subscriber):
            del self._consumers[queue]


def _task_name(msg: ConsumerMessage) -> str | None:
    try:
        name = read_headers(msg.message).get("task")
    except DECODE_ERRORS:
        # An unreadable envelope is not a routing decision. Hand it to a
        # catch-all subscriber so the regular pipeline reports the parsing
        # error under the user's ack policy.
        return None

    return str(name) if name is not None else None
