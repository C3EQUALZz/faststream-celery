"""The result structure persisted by Celery's GroupResult.as_tuple()."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from faststream.exceptions import SetupError

from faststream_celery.schemas.result import SUCCESS, ChordResult

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping, Sequence
    from typing import Any


@dataclass
class ResultGroup:
    """A saved group tree, excluding parent links that are not header results."""

    group_id: str
    children: list["str | ResultGroup"]

    @classmethod
    def from_tuple(cls, value: "Sequence[Any]") -> "ResultGroup":
        (group_id, _), members = value
        if members is None:
            error = "A saved chord header must be a GroupResult."
            raise SetupError(error)
        children: list[str | ResultGroup] = []
        for child in members:
            if child[1] is None:
                children.append(child[0][0])
            else:
                children.append(cls.from_tuple(child))
        return cls(group_id, children)

    def task_ids(self) -> "Generator[str]":
        """Yield terminal task ids in saved order, without parent or group ids."""
        for child in self.children:
            if isinstance(child, str):
                yield child
            else:
                yield from child.task_ids()

    def assemble(self, results: "Mapping[str, ChordResult]") -> list[ChordResult]:
        """Rebuild successful group values from their collected task results."""
        parts = []
        for child in self.children:
            if isinstance(child, str):
                parts.append(results[child])
            else:
                parts.append(
                    ChordResult(
                        child.group_id,
                        SUCCESS,
                        [part.result for part in child.assemble(results)],
                    ),
                )
        return parts
