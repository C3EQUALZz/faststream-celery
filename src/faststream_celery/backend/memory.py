"""An in-process result backend.

``TestCeleryBroker`` puts this in place of whatever backend the broker was
configured with: in fake mode nothing is connected, so the real backend has no
client to write through — while ``CeleryResultMiddleware`` still reports every
outcome to a backend, exactly as it does in production.

Results are kept in a dict, so a test can wait on one with
``broker.request(...)`` or read it straight from ``results``.
"""

import json
from typing import TYPE_CHECKING, cast

import anyio

from faststream_celery._internal import dump_json

if TYPE_CHECKING:
    from faststream_celery.schemas.result import TaskResult


class InMemoryResultBackend:
    """Keeps Celery result envelopes in a dict, keyed by task id."""

    def __init__(self) -> None:
        self.results: dict[str, TaskResult] = {}

        # One event per task id, so `wait()` returns as soon as the result is
        # recorded instead of polling for it.
        self._arrived: dict[str, anyio.Event] = {}

    async def connect(self) -> None:
        """Nothing to connect to."""

    async def disconnect(self) -> None:
        """Forget everything recorded, so one test cannot see another's."""
        self.results.clear()
        self._arrived.clear()

    async def store(self, task_id: str, result: "TaskResult") -> None:
        """Record the outcome of a task we executed.

        Serialized and read back, the way a real backend stores it: a test
        then sees the envelope a Celery client would, and a result that cannot
        be serialized fails here rather than against a live broker.
        """
        stored = cast("TaskResult", json.loads(dump_json(result)))

        self.results[task_id] = stored
        self._event_for(task_id).set()

    async def load(self, task_id: str) -> "TaskResult | None":
        """Read a recorded outcome, or ``None`` while the task is pending."""
        return self.results.get(task_id)

    async def wait(self, task_id: str, *, timeout: float) -> "TaskResult":
        """Block until the task has an outcome, or raise ``TimeoutError``."""
        with anyio.move_on_after(timeout):
            await self._event_for(task_id).wait()
            return self.results[task_id]

        msg = f"No Celery result for task {task_id!r} within {timeout} seconds."
        raise TimeoutError(msg)

    def _event_for(self, task_id: str) -> anyio.Event:
        if (event := self._arrived.get(task_id)) is None:
            event = self._arrived[task_id] = anyio.Event()

        return event
