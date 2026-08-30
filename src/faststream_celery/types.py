"""Types describing the Celery wire format.

Mirrors ``faststream/rabbit/types.py``: broker-specific aliases layered on the
FastStream sendable/decoded types, plus ``TypedDict``s for the protocol v2
envelope produced by ``celery.app.amqp.as_task_v2``.
"""

from collections.abc import Mapping, Sequence
from typing import Any, TypeAlias

from typing_extensions import TypedDict

from faststream_celery._internal import DecodedMessage, SendableMessage

# Positional and keyword arguments of a Celery task, as they travel on the wire.
TaskArgs: TypeAlias = Sequence[SendableMessage]
TaskKwargs: TypeAlias = Mapping[str, SendableMessage]

# Celery headers carry arbitrary JSON put there by other producers.
HeadersType: TypeAlias = Mapping[str, Any]
MutableHeaders: TypeAlias = dict[str, Any]


class TaskSignature(TypedDict, total=False):
    """A serialized Celery signature (``celery.canvas.Signature.__json__``).

    Signatures reach us inside :class:`TaskEmbed` as canvas continuations. We
    carry them verbatim, so every key is optional.
    """

    task: str
    args: list[SendableMessage]
    kwargs: dict[str, SendableMessage]
    options: dict[str, Any]
    subtask_type: str | None
    immutable: bool
    chord_size: int | None


class TaskEmbed(TypedDict):
    """The third slot of a v2 body: what to run once this task is done."""

    callbacks: list[TaskSignature] | None
    errbacks: list[TaskSignature] | None
    chain: list[TaskSignature] | None
    chord: TaskSignature | None


class TaskHeaders(TypedDict):
    """Protocol v2 message headers (``celery.app.amqp.as_task_v2``)."""

    lang: str
    task: str
    id: str
    shadow: str | None
    eta: str | None
    expires: str | None
    group: str | None
    group_index: int | None
    retries: int
    timelimit: list[float | None]
    root_id: str | None
    parent_id: str | None
    argsrepr: str
    kwargsrepr: str
    origin: str | None
    ignore_result: bool
    replaced_task_nesting: int
    stamped_headers: list[str] | None
    stamps: dict[str, DecodedMessage]


# The v2 body triple: `(args, kwargs, embed)`.
TaskBody: TypeAlias = tuple[
    list[SendableMessage],
    dict[str, SendableMessage],
    TaskEmbed,
]
