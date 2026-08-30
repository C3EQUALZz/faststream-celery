"""Celery task result envelopes.

A Celery client reading a result — over AMQP RPC or from a result backend —
expects this exact shape (``celery.backends.base.Backend._get_result_meta``).
"""

import traceback
from datetime import datetime, timezone
from typing import Literal, TypeAlias

from typing_extensions import TypedDict

from faststream_celery._internal import SendableMessage

SUCCESS: Literal["SUCCESS"] = "SUCCESS"
FAILURE: Literal["FAILURE"] = "FAILURE"

TaskStatus: TypeAlias = Literal["SUCCESS", "FAILURE"]


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


def build_success(task_id: str, result: SendableMessage) -> TaskResult:
    """Envelope a handler's return value as a successful Celery result."""
    return TaskResult(
        task_id=task_id,
        status=SUCCESS,
        result=result,
        traceback=None,
        children=[],
        date_done=_now(),
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
        date_done=_now(),
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
