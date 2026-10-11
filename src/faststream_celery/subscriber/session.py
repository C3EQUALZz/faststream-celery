"""Deliveries and acknowledgements belonging to one Kombu read connection."""

import asyncio
import queue
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING

from faststream.exceptions import IncorrectState
from kombu.exceptions import MessageStateError

from faststream_celery.message import ConsumerMessage

if TYPE_CHECKING:
    from kombu import Message


def complete_future(
    future: asyncio.Future[None],
    error: Exception | None = None,
) -> None:
    if future.done():
        return
    if error is None:
        future.set_result(None)
    else:
        future.set_exception(error)


class ConsumerSession:
    """Keep old deliveries from submitting acknowledgements after reconnect."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        messages: "asyncio.Queue[tuple[ConsumerSession, ConsumerMessage] | Exception]",
    ) -> None:
        self._loop = loop
        self._messages = messages
        self._actions: queue.Queue[tuple[Callable[[], None], asyncio.Future[None]]] = (
            queue.Queue()
        )
        self._lock = threading.Lock()
        self._closed = threading.Event()
        self.unsettled = 0

    @property
    def closed(self) -> bool:
        return self._closed.is_set()

    def on_message(self, message: "Message") -> None:
        """Hand a delivery and its own session's executor to the event loop."""
        self.unsettled += 1
        self._loop.call_soon_threadsafe(
            self._messages.put_nowait,
            (self, ConsumerMessage(message, self.execute)),
        )

    async def execute(self, action: Callable[[], None]) -> None:
        """Submit an ack atomically with respect to closing this session."""
        future = self._loop.create_future()
        with self._lock:
            if self.closed:
                msg = "Consumer connection is closed."
                raise IncorrectState(msg)
            self._actions.put((action, future))
        await future

    def run_actions(self) -> None:
        """Execute queued acks on the thread owning their connection."""
        while True:
            try:
                action, future = self._actions.get_nowait()
            except queue.Empty:
                return

            self.unsettled = max(0, self.unsettled - 1)
            try:
                action()
            except Exception as exc:
                # Every submitted action must resolve its future, even when
                # an unexpected error terminates the consumer thread.
                self._loop.call_soon_threadsafe(complete_future, future, exc)
                if not isinstance(exc, MessageStateError):
                    raise
            else:
                self._loop.call_soon_threadsafe(complete_future, future)

    def close(self) -> None:
        """Refuse new actions and fail pending ones before another session starts."""
        with self._lock:
            self._closed.set()
            while True:
                try:
                    _, future = self._actions.get_nowait()
                except queue.Empty:
                    return
                self._loop.call_soon_threadsafe(
                    complete_future,
                    future,
                    IncorrectState("Consumer connection is closed."),
                )
