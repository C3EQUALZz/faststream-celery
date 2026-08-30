from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from functools import partial
from typing import TYPE_CHECKING, Any, Optional, Union, cast

import anyio
from faststream.exceptions import IncorrectState
from faststream.message import StreamMessage
from typing_extensions import overload, override

from faststream_celery._internal import (
    ConcurrentMixin,
    SubscriberUsecase,
    TasksMixin,
    default_filter,
    process_msg,
)
from faststream_celery.message import ConsumerMessage
from faststream_celery.parser import CeleryParser
from faststream_celery.publisher.fake import CeleryFakePublisher

from .bridge import ConsumerBridge

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream_celery._internal import (
        CallsCollection,
        CustomCallable,
        Filter,
        HandlerCallWrapper,
        P_HandlerParams,
        PublisherProto,
        SubscriberSpecification,
        T_HandlerReturn,
    )
    from faststream_celery.configs import CeleryBrokerConfig
    from faststream_celery.message import CeleryMessage

    from .config import CelerySubscriberConfig

SERIALIZATION_ACCEPT = ["json"]


def _task_filter(task: str, msg: StreamMessage[Any]) -> bool:
    return msg.headers.get("task") == task


class CelerySubscriber(TasksMixin, SubscriberUsecase[ConsumerMessage]):
    """Consumes Celery tasks from a queue via a kombu consumer thread."""

    _outer_config: "CeleryBrokerConfig"

    def __init__(
        self,
        config: "CelerySubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        parser = CeleryParser()
        config.decoder = parser.decode_message
        config.parser = parser.parse_message
        super().__init__(config, specification, calls)

        self.config = config

        self._bridge: ConsumerBridge | None = None

    @property
    def queue(self) -> str:
        return f"{self._outer_config.prefix}{self.config.queue}"

    @overload
    def __call__(
        self,
        func: Callable["P_HandlerParams", "T_HandlerReturn"],
        *,
        filter: "Filter[Any]" = default_filter,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
    ) -> "HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]": ...

    @overload
    def __call__(
        self,
        func: None = None,
        *,
        filter: "Filter[Any]" = default_filter,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
    ) -> Callable[
        [Callable["P_HandlerParams", "T_HandlerReturn"]],
        "HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]",
    ]: ...

    @override
    def __call__(
        self,
        func: Callable["P_HandlerParams", "T_HandlerReturn"] | None = None,
        *,
        filter: "Filter[Any]" = default_filter,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
    ) -> Union[
        "HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]",
        Callable[
            [Callable["P_HandlerParams", "T_HandlerReturn"]],
            "HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]",
        ],
    ]:
        task_filter: Filter[Any] = filter
        if self.config.task is not None and filter is default_filter:
            task_filter = partial(_task_filter, self.config.task)

        return super().__call__(
            func,
            filter=task_filter,
            parser=parser,
            decoder=decoder,
            dependencies=dependencies,
        )

    @override
    async def start(self) -> None:
        await super().start()
        self._post_start()

        bridge = ConsumerBridge(
            connection_factory=self._outer_config.make_connection,
            queue_name=self.queue,
            accept=SERIALIZATION_ACCEPT,
            prefetch_count=self.config.prefetch_count,
        )
        self._bridge = bridge
        await bridge.start()

        if self.calls:
            self.add_task(self._consume)

    @override
    async def stop(self) -> None:
        await super().stop()

        if self._bridge is not None:
            await self._bridge.stop()
            self._bridge = None

    async def _consume(self) -> None:
        bridge = self._bridge
        if bridge is None:  # pragma: no cover
            return

        while self.running:
            await self.consume_one(await bridge.get())

    async def consume_one(self, msg: ConsumerMessage) -> None:
        await self.consume(msg)

    @override
    async def get_one(self, *, timeout: float = 5.0) -> Optional["CeleryMessage"]:
        if self._bridge is None:
            msg = "You should start subscriber at first."
            raise IncorrectState(msg)
        if self.calls:
            msg = "You can't use `get_one` method if subscriber has registered handlers."
            raise IncorrectState(msg)

        raw: ConsumerMessage | None = None
        with anyio.move_on_after(timeout):
            raw = await self._bridge.get()

        if raw is None:
            return None

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        return cast(
            "CeleryMessage",
            await process_msg(
                msg=raw,
                middlewares=(
                    m(raw, context=context) for m in self._broker_middlewares
                ),
                parser=async_parser,
                decoder=async_decoder,
            ),
        )

    @override
    async def __aiter__(self) -> AsyncIterator["CeleryMessage"]:  # type: ignore[override]
        if self._bridge is None:
            msg = "You should start subscriber at first."
            raise IncorrectState(msg)
        if self.calls:
            msg = "You can't use iterator if subscriber has registered handlers."
            raise IncorrectState(msg)

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        while True:
            raw = await self._bridge.get()

            yield cast(
                "CeleryMessage",
                await process_msg(
                    msg=raw,
                    middlewares=(
                        m(raw, context=context) for m in self._broker_middlewares
                    ),
                    parser=async_parser,
                    decoder=async_decoder,
                ),
            )

    def _make_response_publisher(
        self,
        message: "StreamMessage[Any]",
    ) -> Sequence["PublisherProto"]:
        return (
            CeleryFakePublisher(
                self._outer_config.producer,
                queue=message.reply_to,
            ),
        )

    def get_log_context(
        self,
        message: Optional["StreamMessage[Any]"],
    ) -> dict[str, str]:
        return {
            "queue": self.queue,
            "message_id": getattr(message, "message_id", ""),
        }


class CeleryConcurrentSubscriber(
    ConcurrentMixin[ConsumerMessage],
    CelerySubscriber,
):
    def __init__(
        self,
        config: "CelerySubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
        max_workers: int,
    ) -> None:
        super().__init__(config, specification, calls, max_workers=max_workers)

    @override
    async def start(self) -> None:
        await super().start()
        self.start_consume_task()

    @override
    async def consume_one(self, msg: ConsumerMessage) -> None:
        await self._put_msg(msg)
