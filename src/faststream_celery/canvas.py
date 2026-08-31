"""Running the Celery canvas: chains, callbacks and errbacks.

A task carries what should happen after it in the ``embed`` slot of its body.
Once a handler finishes, whatever is waiting there is published as ordinary
tasks, the same way ``celery.app.trace`` dispatches them.
"""

from typing import TYPE_CHECKING

from faststream.response import PublishType

from faststream_celery.response import CeleryPublishCommand
from faststream_celery.schemas.signature import call_args, queue_of
from faststream_celery.schemas.task import CeleryTask
from faststream_celery.types import TaskEmbed, TaskSignature

if TYPE_CHECKING:
    from faststream.message import StreamMessage

    from faststream_celery._internal import ProducerProto, SendableMessage
    from faststream_celery.message import ConsumerMessage


class CanvasDispatcher:
    """Publishes the continuations a finished task left behind."""

    def __init__(self, producer: "ProducerProto[CeleryPublishCommand]") -> None:
        self._producer = producer

    async def on_success(
        self,
        msg: "StreamMessage[ConsumerMessage]",
        embed: TaskEmbed,
        result: "SendableMessage",
    ) -> None:
        """Fire the callbacks, then step the chain along.

        Callbacks and the next chain link both receive the result as their
        first argument, unless their signature is immutable.
        """
        for callback in embed.get("callbacks") or ():
            await self._publish(msg, callback, args=call_args(callback, result))

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
        )

    async def on_failure(
        self,
        msg: "StreamMessage[ConsumerMessage]",
        embed: TaskEmbed,
        task_id: str,
    ) -> None:
        """Fire the errbacks.

        An errback serialized by another worker takes the failed task's id,
        not the exception — ``Backend._call_task_errbacks``.
        """
        for errback in embed.get("errbacks") or ():
            await self._publish(msg, errback, args=[task_id])

    async def _publish(
        self,
        msg: "StreamMessage[ConsumerMessage]",
        step: TaskSignature,
        *,
        args: "list[SendableMessage]",
        chain: list[TaskSignature] | None = None,
    ) -> None:
        task_id = msg.headers.get("id") or msg.correlation_id
        options = step.get("options") or {}

        await self._producer.publish(
            CeleryPublishCommand(
                CeleryTask(
                    step["task"],
                    args=args,
                    kwargs=step.get("kwargs") or {},
                    chain=chain or (),
                    root_id=msg.headers.get("root_id") or task_id,
                    parent_id=task_id,
                ),
                queue=queue_of(step, _incoming_queue(msg)),
                correlation_id=options.get("task_id"),
                # A canvas built by a Celery client freezes each step's task
                # id and reply queue into its options. Dropping `reply_to`
                # here would leave the caller of a chain waiting forever for
                # the last step's result.
                reply_to=str(options.get("reply_to") or ""),
                _publish_type=PublishType.PUBLISH,
            ),
        )


def _incoming_queue(msg: "StreamMessage[ConsumerMessage]") -> str:
    """Where a continuation goes when its signature does not say.

    Celery names a queue, its exchange and its routing key alike, so the
    routing key of the task we just ran is the queue it arrived on.
    """
    routing_key = msg.raw_message.message.delivery_info.get("routing_key")

    return str(routing_key or "")
