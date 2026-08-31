"""Sync-to-async bridge between kombu and the FastStream pipeline.

A dedicated ``threading.Thread`` runs a ``kombu.Consumer`` with a
``connection.drain_events`` loop. Messages cross to the event loop through
an ``asyncio.Queue`` (via ``loop.call_soon_threadsafe``); acknowledgements
cross back through an action queue drained by the consumer thread, because
kombu objects must only be touched by the thread that owns their channel.
"""

import asyncio
import queue
import threading
from collections.abc import Callable
from contextlib import suppress
from typing import TYPE_CHECKING, Final

from faststream.exceptions import IncorrectState
from kombu import Connection, Consumer, Exchange, Queue

from faststream_celery.exceptions import CONNECTION_ERRORS, SETTLE_ERRORS
from faststream_celery.message import ConsumerMessage

if TYPE_CHECKING:
    from kombu import Message

_PendingAction = tuple[Callable[[], None], "asyncio.Future[None]"]

# Blocking read while a delivery is unsettled: a queued ack runs only once
# `drain_events` returns, and with the QoS window full nothing arrives to end
# it early.
SETTLE_DRAIN_TIMEOUT: Final[float] = 0.01


def _resolve_future(future: "asyncio.Future[None]") -> None:
    if not future.done():
        future.set_result(None)


def _resolve_future_error(future: "asyncio.Future[None]", exc: Exception) -> None:
    if not future.done():
        future.set_exception(exc)


class ConsumerBridge:
    """Owns the kombu read connection and the consumer thread."""

    def __init__(
        self,
        *,
        connection_factory: Callable[[], Connection],
        queue_name: str,
        accept: list[str],
        prefetch_count: int,
        drain_timeout: float = 1.0,
    ) -> None:
        self._connection_factory = connection_factory
        self._queue_name = queue_name
        self._accept = accept
        self._prefetch_count = prefetch_count
        self._drain_timeout = drain_timeout

        self._messages: asyncio.Queue[ConsumerMessage] = asyncio.Queue()
        self._actions: queue.Queue[_PendingAction] = queue.Queue()

        self._stop_event = threading.Event()
        self._started = threading.Event()
        self._prefetch_changed = threading.Event()
        self._error: BaseException | None = None
        self._consuming = False

        # Deliveries not settled yet; the consumer thread owns this.
        self._unsettled = 0
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    async def start(self) -> None:
        """Spawn the consumer thread and wait until it is consuming."""
        if self._thread is not None:
            return

        self._loop = asyncio.get_running_loop()
        self._thread = threading.Thread(
            target=self._run,
            name=f"faststream-celery-{self._queue_name}",
            daemon=True,
        )
        self._thread.start()

        await asyncio.to_thread(self._started.wait)

        if self._error is not None:
            error = self._error
            self._thread = None
            raise error

        if not self._consuming:
            self._thread = None
            msg = f"The consumer of {self._queue_name!r} stopped before consuming."
            raise IncorrectState(msg)

    async def stop(self) -> None:
        """Stop the consumer thread and close the read connection."""
        self._stop_event.set()

        thread = self._thread
        if thread is not None:
            await asyncio.to_thread(thread.join)
            self._thread = None

        self._abandon_actions()

    def raise_prefetch(self, count: int) -> None:
        """Widen the QoS window; the consumer thread applies it."""
        if count > self._prefetch_count:
            self._prefetch_count = count
            self._prefetch_changed.set()

    async def get(self) -> ConsumerMessage:
        """Take the next message received by the consumer thread."""
        return await self._messages.get()

    async def execute(self, action: Callable[[], None]) -> None:
        """Run an ack callable on the consumer thread and await its completion."""
        if self._loop is None or self._stop_event.is_set():
            msg = "Consumer is not running."
            raise IncorrectState(msg)

        future = self._loop.create_future()
        self._actions.put((action, future))
        await future

    def _on_message(self, message: "Message") -> None:
        """Consumer-thread callback: hand the message to the event loop."""
        if self._loop is not None:
            self._unsettled += 1
            self._loop.call_soon_threadsafe(
                self._messages.put_nowait,
                ConsumerMessage(message, self.execute),
            )

    def _run(self) -> None:
        connection = self._connection_factory()
        try:
            self._consume_loop(connection)

        except CONNECTION_ERRORS as exc:
            self._error = exc

        finally:
            # `start()` waits on this, so it must be set on every path —
            # `_consuming` tells the two apart.
            self._started.set()
            self._abandon_actions()

            with suppress(*CONNECTION_ERRORS):
                connection.close()

    def _consume_loop(self, connection: Connection) -> None:
        channel = connection.channel()

        # Celery's default queue declaration: a direct exchange named
        # after the queue, routing key equal to the queue name.
        exchange = Exchange(self._queue_name, type="direct", durable=True)
        celery_queue = Queue(
            self._queue_name,
            exchange=exchange,
            routing_key=self._queue_name,
            durable=True,
        )

        consumer = Consumer(
            channel,
            queues=[celery_queue],
            accept=self._accept,
            on_message=self._on_message,
            prefetch_count=self._prefetch_count,
        )
        consumer.consume()

        self._consuming = True
        self._started.set()

        while not self._stop_event.is_set():
            self._run_actions()
            self._apply_prefetch(consumer)
            try:
                connection.drain_events(timeout=self._next_timeout())
            except TimeoutError:
                continue

    def _next_timeout(self) -> float:
        """How long to block on the socket before running queued actions."""
        if self._unsettled:
            return min(self._drain_timeout, SETTLE_DRAIN_TIMEOUT)

        return self._drain_timeout

    def _apply_prefetch(self, consumer: Consumer) -> None:
        if self._prefetch_changed.is_set():
            self._prefetch_changed.clear()
            consumer.qos(prefetch_count=self._prefetch_count)

    def _run_actions(self) -> None:
        loop = self._loop

        while True:
            try:
                action, future = self._actions.get_nowait()
            except queue.Empty:
                return

            self._unsettled = max(0, self._unsettled - 1)

            try:
                action()
            except SETTLE_ERRORS as exc:
                if loop is not None:
                    loop.call_soon_threadsafe(_resolve_future_error, future, exc)
            else:
                if loop is not None:
                    loop.call_soon_threadsafe(_resolve_future, future)

    def _abandon_actions(self) -> None:
        """Fail every ack still queued, so no caller waits on a dead thread."""
        self._unsettled = 0

        while True:
            try:
                _, future = self._actions.get_nowait()
            except queue.Empty:
                return

            error = IncorrectState("Consumer is stopped.")
            if self._loop is None:
                _resolve_future_error(future, error)
            else:
                self._loop.call_soon_threadsafe(_resolve_future_error, future, error)
