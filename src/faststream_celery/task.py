from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import NamedTuple, TypeAlias

from faststream.exceptions import SetupError

from faststream_celery._internal import SendableMessage
from faststream_celery.types import (
    TaskArgs,
    TaskBody,
    TaskEmbed,
    TaskHeaders,
    TaskKwargs,
)


@dataclass
class CeleryTask:
    """Wrapper to publish a Celery task via ``broker.publish(...)``.

    Example:
        ```python
        await broker.publish(
            CeleryTask("proj.tasks.send_email", args=[user_id], kwargs={"urgent": True}),
            queue="celery",
        )
        ```
    """

    task: str
    args: TaskArgs = ()
    kwargs: TaskKwargs = field(default_factory=dict)

    # Scheduling options, mirroring `Task.apply_async`.
    countdown: float | None = None
    eta: datetime | None = None
    expires: datetime | float | None = None

    retries: int = 0

    def __post_init__(self) -> None:
        if self.countdown is not None and self.eta is not None:
            msg = "`countdown` and `eta` are mutually exclusive."
            raise SetupError(msg)

    def resolve_eta(self, now: datetime) -> datetime | None:
        """Absolute due time of the task, or ``None`` for "run immediately"."""
        if self.eta is not None:
            return ensure_aware(self.eta)

        if self.countdown is not None:
            return now + timedelta(seconds=self.countdown)

        return None

    def resolve_expires(self, now: datetime) -> datetime | None:
        """Absolute expiration time, or ``None`` when the task never expires."""
        if isinstance(self.expires, datetime):
            return ensure_aware(self.expires)

        if self.expires is not None:
            return now + timedelta(seconds=self.expires)

        return None


# Anything `broker.publish()` accepts: a Celery task or a raw FastStream payload.
CelerySendableMessage: TypeAlias = CeleryTask | SendableMessage


class TaskEnvelope(NamedTuple):
    """A Celery protocol v2 envelope: kombu body plus headers."""

    body: TaskBody
    headers: TaskHeaders


def ensure_aware(value: datetime) -> datetime:
    """Treat a naive datetime as UTC, as Celery does with ``enable_utc``."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def build_task_envelope(
    task: CeleryTask,
    *,
    task_id: str,
    now: datetime | None = None,
) -> TaskEnvelope:
    """Assemble a Celery protocol v2 envelope.

    Mirrors ``celery.app.amqp.as_task_v2``: metadata lives in the headers,
    the body is the ``(args, kwargs, embed)`` triple.

    Args:
        task: Task to publish.
        task_id: Celery task id (also the message ``correlation_id``).
        now: Reference time for ``countdown`` and a relative ``expires``
            (the current UTC time by default).
    """
    moment = now or datetime.now(timezone.utc)

    args = list(task.args)
    kwargs = dict(task.kwargs)

    eta = task.resolve_eta(moment)
    expires = task.resolve_expires(moment)

    headers = TaskHeaders(
        lang="py",
        task=task.task,
        id=task_id,
        shadow=None,
        eta=eta.isoformat() if eta is not None else None,
        expires=expires.isoformat() if expires is not None else None,
        group=None,
        group_index=None,
        retries=task.retries,
        timelimit=[None, None],
        root_id=None,
        parent_id=None,
        argsrepr=repr(args),
        kwargsrepr=repr(kwargs),
        origin=None,
        ignore_result=False,
        replaced_task_nesting=0,
        stamped_headers=None,
        stamps={},
    )

    body: TaskBody = (
        args,
        kwargs,
        TaskEmbed(callbacks=None, errbacks=None, chain=None, chord=None),
    )

    return TaskEnvelope(body=body, headers=headers)
