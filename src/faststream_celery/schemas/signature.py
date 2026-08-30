"""Celery signatures — the serialized calls a canvas is made of.

A signature is one task plus the arguments it is to be called with; Celery
carries them inside a task body's ``embed`` slot so a worker knows what to
run next (``celery.canvas.Signature``).
"""

from collections.abc import Mapping, Sequence
from typing import Any

from faststream_celery._internal import SendableMessage
from faststream_celery.types import TaskArgs, TaskKwargs, TaskSignature


def signature(
    task: str,
    *,
    args: TaskArgs = (),
    kwargs: TaskKwargs | None = None,
    options: Mapping[str, Any] | None = None,
    immutable: bool = False,
) -> TaskSignature:
    """Describe a call for a callback, an errback or a chain step.

    Args:
        task: Celery task name to call.
        args: Positional arguments of the call.
        kwargs: Keyword arguments of the call.
        options: Celery apply options, such as ``{"queue": "other"}``.
        immutable: Whether the call refuses the previous task's result.
            Celery spells this ``.si()``, as against ``.s()``.
    """
    return TaskSignature(
        task=task,
        args=list(args),
        kwargs=dict(kwargs or {}),
        options=dict(options or {}),
        subtask_type=None,
        immutable=immutable,
    )


def call_args(sig: TaskSignature, result: SendableMessage) -> list[SendableMessage]:
    """The arguments a continuation is called with.

    The previous task's result is prepended, unless the signature is
    immutable — ``celery.canvas.Signature._merge``.
    """
    args = list(sig.get("args") or [])

    if sig.get("immutable"):
        return args

    return [result, *args]


def queue_of(sig: TaskSignature, default: str) -> str:
    """Where a continuation goes: its own ``queue`` option, else ours."""
    options = sig.get("options") or {}
    queue = options.get("queue")

    return str(queue) if queue else default


def as_signatures(
    values: Sequence[TaskSignature] | None,
) -> list[TaskSignature]:
    """Normalize the ``embed`` slots, which are ``None`` when unused."""
    return list(values or [])
