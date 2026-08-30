"""In-memory ETA scheduler for deferred Celery tasks.

A Celery worker holds a task with a future ``eta`` in memory until it is due
rather than leaving it on the broker; this mirrors that behaviour. There is
no persistence: if the process dies before the due time, the message comes
back only because it was never acked (per the subscriber's ack policy).
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Generic, TypeVar

T = TypeVar("T")


class EtaScheduler(Generic[T]):
    """Delays messages until their due time, then hands them to ``dispatch``."""

    def __init__(self, dispatch: Callable[[T], Awaitable[None]]) -> None:
        self._dispatch = dispatch
        self._pending: set[asyncio.Task[None]] = set()

    @property
    def pending(self) -> int:
        """Number of messages currently waiting for their due time."""
        return len(self._pending)

    def schedule(self, msg: T, delay: float) -> None:
        """Dispatch ``msg`` after ``delay`` seconds."""
        task = asyncio.create_task(self._wait(msg, delay))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def stop(self) -> None:
        """Cancel every pending message and wait for the tasks to unwind."""
        pending, self._pending = self._pending, set()

        for task in pending:
            task.cancel()

        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def _wait(self, msg: T, delay: float) -> None:
        await asyncio.sleep(delay)
        await self._dispatch(msg)
