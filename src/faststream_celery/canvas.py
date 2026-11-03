"""Running the Celery canvas: chains, callbacks and errbacks.

A task carries what should happen after it in the ``embed`` slot of its body.
Once a handler finishes, whatever is waiting there is published as ordinary
tasks, the same way ``celery.app.trace`` dispatches them.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from faststream.exceptions import IncorrectState, SetupError
from faststream.response import PublishType

from faststream_celery.backend.proto import ChordBackend
from faststream_celery.exceptions import PUBLISH_ERRORS
from faststream_celery.response import CeleryPublishCommand
from faststream_celery.schemas.constants import SERIALIZER
from faststream_celery.schemas.result import (
    FAILURE,
    REVOKED,
    build_chord_failure,
    failed_chain_results,
)
from faststream_celery.schemas.signature import as_signatures, call_args, queue_of
from faststream_celery.schemas.task import CeleryTask
from faststream_celery.types import TaskEmbed, TaskSignature

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import Any

    from faststream.message import StreamMessage

    from faststream_celery._internal import ProducerProto, SendableMessage
    from faststream_celery.backend.proto import ResultBackend
    from faststream_celery.message import ConsumerMessage
    from faststream_celery.schemas.result import TaskResult


CONTINUATION_OPTIONS = (
    "task_id",
    "reply_to",
    "queue",
    "exchange",
    "routing_key",
    "declare",
    "headers",
    "serializer",
    "countdown",
    "eta",
    "expires",
    "retries",
    "link",
    "link_error",
    "chain",
    "root_id",
    "parent_id",
    "group_id",
    "group_index",
    "chord",
)


class CanvasDispatcher:
    """Publishes the continuations a finished task left behind."""

    def __init__(
        self,
        producer: "ProducerProto[CeleryPublishCommand]",
        backend: "ResultBackend | None" = None,
    ) -> None:
        self._producer = producer
        self._backend = backend

    async def on_chord_part(
        self,
        msg: "StreamMessage[ConsumerMessage]",
        part_options: "Mapping[str, Any]",
        result: "TaskResult",
        *,
        queue: str,
    ) -> None:
        """Report a terminal part using its own Celery signature metadata."""
        callback = part_options.get("chord")
        if callback is None:
            return
        validate_signature(callback)
        options = callback.get("options") or {}
        callback_id = options.get("task_id")
        group_id = part_options.get("group_id")
        if not group_id or not callback_id:
            error = "A chord requires a group id and a frozen callback task id."
            raise SetupError(error)
        backend = self._backend
        if not isinstance(backend, ChordBackend):
            error = "Mixed chords require a Redis result backend."
            raise SetupError(error)

        parts = await backend.record_chord_part(
            group_id,
            part_options.get("group_index"),
            result,
        )
        if parts is None:
            return

        failed = next((part for part in parts if part.status in {FAILURE, REVOKED}), None)
        if failed is not None:
            failure = build_chord_failure(callback_id, failed)
            await self._fail_chord(msg, callback, failure, queue=queue)
        else:
            values = [part.result for part in parts]
            try:
                await self._publish(
                    msg,
                    callback,
                    args=call_args(callback, values),
                    queue=queue,
                )
            except PUBLISH_ERRORS as exc:
                failure = build_chord_failure(callback_id, exc)
                await self._fail_chord(msg, callback, failure, queue=queue)
        await backend.clear_chord(group_id)

    async def _fail_chord(
        self,
        msg: "StreamMessage[ConsumerMessage]",
        callback: TaskSignature,
        failure: "TaskResult",
        *,
        queue: str,
    ) -> None:
        """Finish a failed chord body and dispatch its queued errbacks."""
        backend = self._backend
        if backend is None:
            error = "A failed chord needs its result backend to remain connected."
            raise IncorrectState(error)
        await backend.store(failure["task_id"], failure)
        options = callback.get("options") or {}
        for _, downstream in failed_chain_results(options.get("chain"), failure):
            await backend.store(downstream["task_id"], downstream)
        for errback in as_signatures(options.get("link_error")):
            await self._publish(
                msg,
                errback,
                args=call_args(errback, failure["task_id"]),
                queue=queue,
                origin_id=failure["task_id"],
            )

    async def on_success(
        self,
        msg: "StreamMessage[ConsumerMessage]",
        embed: TaskEmbed,
        result: "SendableMessage",
        *,
        queue: str,
    ) -> None:
        """Fire the callbacks, then step the chain along.

        Callbacks and the next chain link both receive the result as their
        first argument, unless their signature is immutable.
        """
        for callback in embed.get("callbacks") or ():
            await self._publish(
                msg,
                callback,
                args=call_args(callback, result),
                queue=queue,
            )

        chain = list(embed.get("chain") or ())
        if not chain:
            return

        # Celery keeps a chain reversed: the next step is the last element,
        # and the rest travels on inside it.
        following, remaining = chain[-1], chain[:-1]

        await self._publish(
            msg,
            following,
            args=call_args(following, result),
            chain=remaining,
            queue=queue,
        )

    async def on_failure(
        self,
        msg: "StreamMessage[ConsumerMessage]",
        embed: TaskEmbed,
        task_id: str,
        *,
        queue: str,
    ) -> None:
        """Publish queued errbacks using their signature arguments.

        Mutable signatures receive the failed task's id before their own args;
        immutable signatures retain only their own args.
        """
        for errback in embed.get("errbacks") or ():
            await self._publish(
                msg,
                errback,
                args=call_args(errback, task_id),
                queue=queue,
            )

    async def _publish(  # ruff: ignore[too-many-arguments]
        self,
        msg: "StreamMessage[ConsumerMessage]",
        step: TaskSignature,
        *,
        args: "list[SendableMessage]",
        queue: str,
        chain: list[TaskSignature] | None = None,
        origin_id: str | None = None,
    ) -> None:
        """Publish a supported task signature without discarding its options."""
        task_id = origin_id or msg.headers.get("id") or msg.correlation_id
        options = step.get("options") or {}
        validate_signature(step)

        eta = options.get("eta")
        expires = options.get("expires")

        await self._producer.publish(
            CeleryPublishCommand(
                CeleryTask(
                    step["task"],
                    args=args,
                    kwargs=step.get("kwargs") or {},
                    countdown=options.get("countdown"),
                    eta=(
                        datetime.fromisoformat(eta.replace("Z", "+00:00"))
                        if isinstance(eta, str)
                        else eta
                    ),
                    expires=(
                        datetime.fromisoformat(expires.replace("Z", "+00:00"))
                        if isinstance(expires, str)
                        else expires
                    ),
                    retries=options.get("retries", 0),
                    link=as_signatures(options.get("link")),
                    link_error=as_signatures(options.get("link_error")),
                    chain=chain if chain is not None else options.get("chain") or (),
                    root_id=(
                        origin_id
                        if origin_id is not None
                        else msg.headers.get("root_id") or task_id
                    ),
                    parent_id=task_id,
                    group=options.get("group_id"),
                    group_index=options.get("group_index"),
                    chord=options.get("chord"),
                ),
                queue=queue_of(step, queue),
                exchange=options.get("exchange"),
                routing_key=options.get("routing_key"),
                declare=options.get("declare", True),
                headers=options.get("headers"),
                correlation_id=options.get("task_id"),
                reply_to=str(options.get("reply_to") or ""),
                _publish_type=PublishType.PUBLISH,
            ),
        )


def validate_signature(step: TaskSignature) -> None:
    """Reject continuation options and canvas types the dispatcher cannot execute."""
    options = step.get("options") or {}
    unsupported = {
        key
        for key, value in options.items()
        if value is not None and key not in CONTINUATION_OPTIONS
    }
    if options.get("serializer") not in {None, SERIALIZER}:
        unsupported.add("serializer")
    if unsupported or step.get("subtask_type"):
        reason = step.get("subtask_type") or ", ".join(sorted(unsupported))
        error = f"Unsupported Celery continuation options or type: {reason}."
        raise SetupError(error)
