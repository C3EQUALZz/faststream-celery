"""Sync-to-async bridge between kombu and the FastStream pipeline.

A dedicated ``threading.Thread`` runs a ``kombu.Consumer`` with a
``connection.drain_events`` loop. Messages cross to the event loop through
an ``asyncio.Queue`` (via ``loop.call_soon_threadsafe``); acknowledgements
cross back through an action queue drained by the consumer thread, because
kombu objects must only be touched by the thread that owns their channel.
"""

import asyncio
import logging
import threading
from collections.abc import Callable
from contextlib import suppress
from functools import partial
from typing import TYPE_CHECKING, Final

from faststream.exceptions import IncorrectState
from kombu import Connection, Consumer, Exchange, Queue

from faststream_celery.exceptions import CONNECTION_ERRORS
from faststream_celery.message import ConsumerMessage

from .session import ConsumerSession

if TYPE_CHECKING:
    from faststream_celery._internal import LoggerState

# Blocking read while a delivery is unsettled: a queued ack runs only once
# `drain_events` returns, and with the QoS window full nothing arrives to end
# it early.
SETTLE_DRAIN_TIMEOUT: Final[float] = 0.01
RECONNECT_DELAY: Final[float] = 0.1
MAX_RECONNECT_DELAY: Final[float] = 5.0


class ConsumerBridge:
    """Owns the kombu read connection and the consumer thread."""

    def __init__(  # ruff: ignore[too-many-arguments]
        self,
        *,
        connection_factory: Callable[[], Connection],
        queue_name: str,
        accept: list[str],
        prefetch_count: int,
        drain_timeout: float = 1.0,
        logger: "LoggerState | None" = None,
    ) -> None:
        self._connection_factory = connection_factory
        self._queue_name = queue_name
        self._accept = accept
        self._prefetch_count = prefetch_count
        self._drain_timeout = drain_timeout
        self._logger = logger

        self._messages: asyncio.Queue[
            tuple[ConsumerSession, ConsumerMessage] | Exception
        ] = asyncio.Queue()
        self._session: ConsumerSession | None = None

        self._stop_event = threading.Event()
        self._started = threading.Event()
        self._prefetch_changed = threading.Event()
        self._error: Exception | None = None
        self._consuming = False

        self._thread: threading.Thread | None = None

    @property
    def is_alive(self) -> bool:
        """Whether the read thread is currently consuming, including after recovery."""
        thread = self._thread
        return (
            self._consuming
            and not self._stop_event.is_set()
            and thread is not None
            and thread.is_alive()
        )

    async def start(self) -> None:
        """Spawn the consumer thread and wait until it is consuming."""
        if self._thread is not None:
            if self._error is not None:
                raise self._error
            return

        loop = asyncio.get_running_loop()
        self._stop_event.clear()
        self._started.clear()
        self._error = None
        self._messages = asyncio.Queue()
        self._thread = threading.Thread(
            target=partial(self._run, loop),
            name=f"faststream-celery-{self._queue_name}",
            daemon=True,
        )
        self._thread.start()

        await asyncio.to_thread(self._started.wait)

        if self._error is not None:
            error = self._error
            await asyncio.to_thread(self._thread.join)
            self._thread = None
            raise error

        if self._stop_event.is_set():
            msg = f"The consumer of {self._queue_name!r} stopped before consuming."
            raise IncorrectState(msg)

    async def stop(self) -> None:
        """Stop the consumer thread and close the read connection."""
        self._stop_event.set()
        if self._session is not None:
            self._session.close()

        thread = self._thread
        if thread is not None:
            await asyncio.to_thread(thread.join)
            self._thread = None
        else:
            self._messages.put_nowait(IncorrectState("Consumer is stopped."))

    def raise_prefetch(self, count: int) -> None:
        """Widen the QoS window; the consumer thread applies it."""
        if count > self._prefetch_count:
            self._prefetch_count = count
            self._prefetch_changed.set()

    async def get(self) -> ConsumerMessage:
        """Wait through recovery, discarding buffered deliveries from closed sessions."""
        messages = self._messages
        while True:
            delivery = await messages.get()
            if isinstance(delivery, Exception):
                # Keep the failure available to every current and future
                # reader, rather than waking just one waiter.
                messages.put_nowait(delivery)
                msg = "Consumer is stopped."
                raise IncorrectState(msg) from delivery
            session, message = delivery
            if not session.closed:
                return message

    def _run(self, loop: asyncio.AbstractEventLoop) -> None:
        try:
            self._run_connections(loop)
        except Exception as exc:  # ruff: ignore[blind-except]
            # This is the thread boundary: fatal failures must reach async
            # readers and startup callers instead of only threading.excepthook.
            self._error = exc
            self._log(f"Consumer stopped: {exc!r}", logging.ERROR)
        finally:
            self._consuming = False
            self._started.set()
            loop.call_soon_threadsafe(
                self._messages.put_nowait,
                self._error or IncorrectState("Consumer is stopped."),
            )

    def _run_connections(self, loop: asyncio.AbstractEventLoop) -> None:
        delay = RECONNECT_DELAY
        while not self._stop_event.is_set():
            connection = self._connection_factory()
            session = ConsumerSession(loop, self._messages)
            self._session = session
            recoverable: tuple[type[BaseException], ...] = (OSError,)
            try:
                recoverable = (
                    OSError,
                    *connection.recoverable_connection_errors,
                    *connection.recoverable_channel_errors,
                )
                self._consume_loop(connection, session)
            except recoverable as exc:
                # Keep the existing startup contract: the first connection
                # failure reaches start(); recovery applies once we consumed.
                if not self._started.is_set():
                    raise
                if self._consuming:
                    delay = RECONNECT_DELAY
                self._log(
                    f"Consumer connection lost; retrying in {delay}s: {exc!r}",
                    logging.WARNING,
                )
            finally:
                self._consuming = False
                session.close()
                with suppress(*CONNECTION_ERRORS, *recoverable):
                    connection.close()

            if self._stop_event.wait(delay):
                return
            delay = min(delay * 2, MAX_RECONNECT_DELAY)

    def _consume_loop(self, connection: Connection, session: ConsumerSession) -> None:
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
            on_message=session.on_message,
            prefetch_count=self._prefetch_count,
        )
        consumer.consume()

        self._consuming = True
        if self._started.is_set():
            self._log("Consumer connection restored.", logging.INFO)
        self._started.set()

        while not self._stop_event.is_set():
            session.run_actions()
            self._apply_prefetch(consumer)
            try:
                connection.drain_events(timeout=self._next_timeout())
            except TimeoutError:
                continue

    def _next_timeout(self) -> float:
        """How long to block on the socket before running queued actions."""
        if self._session is not None and self._session.unsettled:
            return min(self._drain_timeout, SETTLE_DRAIN_TIMEOUT)

        return self._drain_timeout

    def _apply_prefetch(self, consumer: Consumer) -> None:
        if self._prefetch_changed.is_set():
            self._prefetch_changed.clear()
            consumer.qos(prefetch_count=self._prefetch_count)

    def _log(self, message: str, level: int) -> None:
        if self._logger is not None:
            self._logger.log(f"{self._queue_name}: {message}", level)
