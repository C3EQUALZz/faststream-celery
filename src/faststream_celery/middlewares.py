"""Broker middlewares installed by ``CeleryBroker`` itself."""

from typing import TYPE_CHECKING, Any

from faststream.middlewares import BaseMiddleware
from faststream.response import PublishType
from typing_extensions import override

from faststream_celery.message import ConsumerMessage
from faststream_celery.response import CeleryPublishCommand
from faststream_celery.result import build_failure

if TYPE_CHECKING:
    from faststream.message import StreamMessage

    from faststream_celery._internal import AsyncFuncAny, ContextRepo
    from faststream_celery.configs import CeleryBrokerConfig


class CeleryResultMiddleware(BaseMiddleware[CeleryPublishCommand, ConsumerMessage]):
    """Reports a failed handler back to the Celery caller.

    A successful result travels the stock FastStream reply path (see
    ``CeleryFakePublisher``), which never runs when the handler raises — so
    the ``FAILURE`` envelope is published from here instead.
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

    @override
    async def consume_scope(
        self,
        call_next: "AsyncFuncAny",
        msg: "StreamMessage[Any]",
    ) -> Any:
        try:
            return await call_next(msg)

        except Exception as exc:
            await self._publish_failure(msg, exc)
            raise

    async def _publish_failure(
        self,
        msg: "StreamMessage[Any]",
        exc: Exception,
    ) -> None:
        if not msg.reply_to or msg.headers.get("ignore_result"):
            return

        task_id = msg.headers.get("id") or msg.correlation_id

        await self._config.producer.publish(
            CeleryPublishCommand(
                build_failure(task_id, exc),
                queue=msg.reply_to,
                # Celery replies go to the default exchange, and the reply
                # queue belongs to the client — we must not declare it.
                exchange="",
                declare=False,
                correlation_id=task_id,
                _publish_type=PublishType.REPLY,
            ),
        )
