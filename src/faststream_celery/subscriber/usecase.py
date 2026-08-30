import logging
from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from contextlib import suppress
from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Any, Optional, Union, cast

import anyio
from faststream.exceptions import IncorrectState
from faststream.message import StreamMessage
from faststream.middlewares import AckPolicy
from typing_extensions import overload, override

from faststream_celery._internal import (
    ConcurrentMixin,
    SubscriberUsecase,
    TasksMixin,
    default_filter,
    process_msg,
)
from faststream_celery.exceptions import DECODE_ERRORS, SETTLE_ERRORS
from faststream_celery.message import ConsumerMessage
from faststream_celery.parser import (
    CeleryParser,
    Schedule,
    extract_schedule,
    read_headers,
)
from faststream_celery.publisher.fake import CeleryFakePublisher
from faststream_celery.schemas.constants import SERIALIZATION_ACCEPT

from .scheduler import EtaScheduler

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from faststream.response import Response

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
    from .consumer import SharedConsumer

_NO_SCHEDULE = Schedule(eta=None, expires=None)


def _task_filter(task: str, msg: StreamMessage[Any]) -> bool:
    return msg.headers.get("task") == task


def _read_schedule(msg: ConsumerMessage) -> Schedule:
    try:
        return extract_schedule(read_headers(msg.message))
    except DECODE_ERRORS:
        # A body we cannot read is not a scheduling problem. Let the regular
        # pipeline surface the parsing error under the user's ack policy.
        return _NO_SCHEDULE


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

        self._consumer: SharedConsumer | None = None
        self._scheduler: EtaScheduler[ConsumerMessage] = EtaScheduler(self.dispatch)

    @property
    def queue(self) -> str:
        return f"{self._outer_config.prefix}{self.config.queue}"

    @property
    def task(self) -> str | None:
        return self.config.task

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

        consumer = self._outer_config.consumers.acquire(
            queue=self.queue,
            connection_factory=self._outer_config.make_connection,
            accept=SERIALIZATION_ACCEPT,
            prefetch_count=self.config.prefetch_count,
            logger=self._outer_config.logger,
        )
        self._consumer = consumer

        if self.calls:
            consumer.register(self)

        await consumer.start()

    @override
    async def stop(self) -> None:
        await super().stop()

        # Deferred tasks were never acked, so the broker redelivers them,
        # as Celery's own eta tasks do on worker shutdown.
        await self._scheduler.stop()

        if self._consumer is not None:
            self._consumer = None
            await self._outer_config.consumers.release(self.queue, self)

    async def dispatch(self, msg: ConsumerMessage) -> None:
        """Route a received message: drop, defer, or consume it now."""
        schedule = _read_schedule(msg)
        now = datetime.now(timezone.utc)

        if schedule.expires is not None and schedule.expires <= now:
            await self._drop_expired(msg, schedule.expires)
            return

        if schedule.eta is not None:
            delay = (schedule.eta - now).total_seconds()
            if delay > 0:
                self._scheduler.schedule(msg, delay)
                return

        await self.consume_one(msg)

    async def _drop_expired(self, msg: ConsumerMessage, expires: datetime) -> None:
        """Discard a task whose ``expires`` has already passed, as Celery does."""
        self._log(
            logging.WARNING,
            f"Dropping expired task (expired at {expires.isoformat()})",
            extra=self.get_log_context(None),
        )

        with suppress(*SETTLE_ERRORS, IncorrectState):
            await msg.executor(msg.message.ack)

    @override
    async def consume(self, msg: ConsumerMessage) -> Optional["Response"]:
        result: Response | None = await super().consume(msg)
        await self._settle_unhandled(msg)
        return result

    async def _settle_unhandled(self, msg: ConsumerMessage) -> None:
        """Reject a message that never reached a handler.

        FastStream's acknowledgement middleware only settles messages a
        handler accepted, so one dropped by a `task=` filter would stay
        unacked — and with a prefetch window that stalls the consumer for
        good. Celery drops unknown tasks the same way.
        """
        if self.ack_policy is AckPolicy.MANUAL or msg.message.acknowledged:
            return

        with suppress(*SETTLE_ERRORS, IncorrectState):
            await msg.executor(partial(msg.message.reject, requeue=False))

    async def consume_one(self, msg: ConsumerMessage) -> None:
        await self.consume(msg)

    @override
    async def get_one(self, *, timeout: float = 5.0) -> Optional["CeleryMessage"]:
        if self._consumer is None:
            msg = "You should start subscriber at first."
            raise IncorrectState(msg)
        if self.calls:
            msg = "You can't use `get_one` method if subscriber has registered handlers."
            raise IncorrectState(msg)

        raw: ConsumerMessage | None = None
        with anyio.move_on_after(timeout):
            raw = await self._consumer.bridge.get()

        if raw is None:
            return None

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        return cast(
            "CeleryMessage",
            await process_msg(
                msg=raw,
                middlewares=(m(raw, context=context) for m in self._broker_middlewares),
                parser=async_parser,
                decoder=async_decoder,
            ),
        )

    @override
    async def __aiter__(self) -> AsyncIterator["CeleryMessage"]:  # type: ignore[override]
        if self._consumer is None:
            msg = "You should start subscriber at first."
            raise IncorrectState(msg)
        if self.calls:
            msg = "You can't use iterator if subscriber has registered handlers."
            raise IncorrectState(msg)

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        while True:
            raw = await self._consumer.bridge.get()

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

    @override
    def _make_response_publisher(
        self,
        message: "StreamMessage[Any]",
    ) -> Sequence["PublisherProto"]:
        if message.headers.get("ignore_result"):
            # The caller told us it will never read the result.
            return ()

        return (
            CeleryFakePublisher(
                self._outer_config.producer,
                queue=message.reply_to,
                task_id=message.headers.get("id") or message.correlation_id,
            ),
        )

    @override
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
