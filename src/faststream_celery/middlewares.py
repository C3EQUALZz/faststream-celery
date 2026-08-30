"""Broker middlewares installed by ``CeleryBroker`` itself."""

from typing import TYPE_CHECKING, Any

from faststream.message.source_type import SourceType
from faststream.middlewares import BaseMiddleware
from faststream.response import PublishType
from typing_extensions import override

from faststream_celery.canvas import CanvasDispatcher
from faststream_celery.message import ConsumerMessage
from faststream_celery.parser import read_embed
from faststream_celery.response import CeleryPublishCommand
from faststream_celery.schemas.result import TaskResult, build_failure, build_success

if TYPE_CHECKING:
    from faststream.message import StreamMessage

    from faststream_celery._internal import AsyncFuncAny, ContextRepo
    from faststream_celery.configs import CeleryBrokerConfig


class CeleryResultMiddleware(BaseMiddleware[CeleryPublishCommand, ConsumerMessage]):
    """Reports what a handler did back to whoever sent the task.

    Two channels, and a task may use either or both:

    - a ``reply_to`` queue, the AMQP RPC path. Success travels the stock
      FastStream reply path (``CeleryFakePublisher``), which never runs when
      the handler raises, so the ``FAILURE`` envelope is published here.
    - a result backend, where both outcomes are recorded under
      ``celery-task-meta-<id>``.

    It also runs the canvas the task carries: callbacks and the next chain
    link on success, errbacks on failure.
    """

    def __init__(
        self,
        msg: ConsumerMessage | None,
        /,
        *,
        context: "ContextRepo",
        config: "CeleryBrokerConfig",
    ) -> None:
        super().__init__(msg, context=context)
        self._config = config
        self._canvas = CanvasDispatcher(config.producer)

    @override
    async def consume_scope(
        self,
        call_next: "AsyncFuncAny",
        msg: "StreamMessage[Any]",
    ) -> Any:
        if msg.source_type is not SourceType.CONSUME:
            # A reply travelling back to `request()` runs this stack too, and
            # a reply is not a task outcome to report.
            return await call_next(msg)

        task_id: str = msg.headers.get("id") or msg.correlation_id

        embed = read_embed(msg.raw_message.message)

        try:
            result = await call_next(msg)

        # A handler may raise anything; the exception is reported to the
        # Celery caller and then re-raised untouched.
        except Exception as exc:
            await self._report(msg, build_failure(task_id, exc), reply=True)
            await self._canvas.on_failure(msg, embed, task_id)
            raise

        await self._report(msg, build_success(task_id, result), reply=False)
        await self._canvas.on_success(msg, embed, result)
        return result

    async def _report(
        self,
        msg: "StreamMessage[Any]",
        envelope: TaskResult,
        *,
        reply: bool,
    ) -> None:
        if msg.headers.get("ignore_result"):
            return

        if (backend := self._config.result_backend) is not None:
            await backend.store(envelope["task_id"], envelope)

        if reply and msg.reply_to:
            await self._publish_reply(msg.reply_to, envelope)

    async def _publish_reply(self, reply_to: str, envelope: TaskResult) -> None:
        await self._config.producer.publish(
            CeleryPublishCommand(
                envelope,
                queue=reply_to,
                # Celery replies go to the default exchange, and the reply
                # queue belongs to the client — we must not declare it.
                exchange="",
                declare=False,
                correlation_id=envelope["task_id"],
                _publish_type=PublishType.REPLY,
            ),
        )
