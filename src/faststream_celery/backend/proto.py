"""What the broker needs from a Celery result backend."""

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from faststream_celery.schemas.result import ChordResult, TaskResult


@runtime_checkable
class ChordBackend(Protocol):
    """Optional backend capability for joining a mixed Celery chord."""

    async def record_chord_part(
        self,
        group_id: str,
        group_index: int | None,
        result: "TaskResult",
    ) -> "list[ChordResult] | None":
        """Record a part and return ordered results when the group is complete."""
        ...

    async def clear_chord(self, group_id: str) -> None:
        """Forget the completed group's coordination keys."""
        ...


class ResultBackend(Protocol):
    """Stores and reads task results the way a Celery client expects."""

    async def connect(self) -> None:
        """Open the connection the backend reads and writes through."""
        ...

    async def disconnect(self) -> None:
        """Close it again."""
        ...

    async def store(self, task_id: str, result: "TaskResult") -> None:
        """Record the outcome of a task we executed."""
        ...

    async def load(self, task_id: str) -> "TaskResult | None":
        """Read a recorded outcome, or ``None`` while the task is pending."""
        ...

    async def wait(
        self,
        task_id: str,
        *,
        timeout: float,
    ) -> "TaskResult":
        """Block until a task has an outcome, or raise ``TimeoutError``."""
        ...
