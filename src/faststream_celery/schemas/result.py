"""Celery task result envelopes.

A Celery client reading a result — over AMQP RPC or from a result backend —
expects this exact shape (``celery.backends.base.Backend._get_result_meta``).
"""

import traceback
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Final, Literal, NamedTuple, TypeAlias

from typing_extensions import TypedDict

from faststream_celery._internal import SendableMessage

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

    from faststream_celery.types import TaskSignature

SUCCESS: Literal["SUCCESS"] = "SUCCESS"
FAILURE: Literal["FAILURE"] = "FAILURE"
REVOKED: Literal["REVOKED"] = "REVOKED"

# Celery's READY_STATES; progress and RETRY must keep a result waiter open.
READY_STATES: Final[frozenset[str]] = frozenset((SUCCESS, FAILURE, REVOKED))

# Celery workers may also report arbitrary application progress states.
TaskStatus: TypeAlias = str


class ExceptionInfo(TypedDict):
    """The ``result`` field of a FAILURE envelope.

    Celery rebuilds the original exception from it on the client side.
    """

    exc_type: str
    exc_message: list[SendableMessage]
    exc_module: str


class TaskResult(TypedDict):
    """A Celery result message."""

    task_id: str
    status: TaskStatus
    result: SendableMessage
    traceback: str | None
    children: list[SendableMessage]
    date_done: str


class ChordResult(NamedTuple):
    """One terminal result from Celery's ordered Redis chord journal."""

    task_id: str
    status: str
    result: SendableMessage


def build_success(task_id: str, result: SendableMessage) -> TaskResult:
    """Envelope a handler's return value as a successful Celery result."""
    return TaskResult(
        task_id=task_id,
        status=SUCCESS,
        result=result,
        traceback=None,
        children=[],
        date_done=datetime.now(timezone.utc).isoformat(),
    )


def build_failure(task_id: str, exc: BaseException) -> TaskResult:
    """Envelope an exception the way a Celery worker reports a failed task."""
    return TaskResult(
        task_id=task_id,
        status=FAILURE,
        result=ExceptionInfo(
            exc_type=type(exc).__name__,
            exc_message=list(exc.args),
            exc_module=type(exc).__module__,
        ),
        traceback="".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__),
        ),
        children=[],
        date_done=datetime.now(timezone.utc).isoformat(),
    )


def build_chord_failure(task_id: str, cause: ChordResult | Exception) -> TaskResult:
    """Report a chord dependency or dispatch failure to a Celery client."""
    if isinstance(cause, ChordResult):
        value = cause.result
        if isinstance(value, dict):
            message_args = value.get("exc_message") or ()
            args = (
                message_args
                if isinstance(message_args, (list, tuple))
                else (message_args,)
            )
            detail = f"{value.get('exc_type', 'Exception')}({', '.join(map(repr, args))})"
        else:
            detail = repr(value)
        message = f"Dependency {cause.task_id} raised {detail}"
    else:
        message = f"Callback error: {cause!r}"

    return TaskResult(
        task_id=task_id,
        status=FAILURE,
        result=ExceptionInfo(
            exc_type="ChordError",
            exc_message=[message],
            exc_module="celery.exceptions",
        ),
        traceback=None,
        children=[],
        date_done=datetime.now(timezone.utc).isoformat(),
    )


def failed_chain_results(
    chain: "Sequence[TaskSignature] | None",
    failure: TaskResult,
) -> "Generator[tuple[TaskSignature, TaskResult]]":
    """Associate the original failure with each frozen downstream task id."""
    for step in chain or ():
        task_id = (step.get("options") or {}).get("task_id")
        if task_id is not None:
            yield step, {**failure, "task_id": task_id}
