"""In-memory ``TestCeleryBroker`` for application tests.

Modeled on ``faststream/rabbit/testing.py``: the broker's producer is swapped
for a fake that routes a publish straight into the matching subscribers'
``process_message()``, so middlewares, the parser and DI all run without a
kombu connection or a live Celery.
"""

import asyncio
from collections.abc import AsyncGenerator, Generator, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager
from copy import copy
from typing import TYPE_CHECKING, Any, NamedTuple, Optional, cast

import anyio
from faststream.exceptions import SubscriberNotFound
from faststream.message import gen_cor_id
from faststream.response import PublishType
from kombu import Message
from typing_extensions import overload, override

from faststream_celery._internal import (
    CallsCollection,
    DefaultCodec,
    EnterType,
    TestBroker,
    change_producer,
    dump_json,
    resolve_serializer,
)
from faststream_celery.backend import InMemoryResultBackend
from faststream_celery.broker import CeleryBroker
from faststream_celery.configs import CeleryBrokerConfig
from faststream_celery.exceptions import DECODE_ERRORS
from faststream_celery.message import ConsumerMessage
from faststream_celery.publisher.producer import (
    DEFAULT_REQUEST_TIMEOUT,
    CeleryFastProducer,
)
from faststream_celery.response import CeleryPublishCommand
from faststream_celery.schemas.constants import CONTENT_ENCODING, CONTENT_TYPE
from faststream_celery.schemas.result import READY_STATES
from faststream_celery.schemas.task import (
    CelerySendableMessage,
    CeleryTask,
    build_task_envelope,
)
from faststream_celery.subscriber import CelerySubscriber
from faststream_celery.subscriber.consumer import select_subscriber
from faststream_celery.types import MutableHeaders

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream_celery._internal import CodecProto
    from faststream_celery.publisher import CeleryPublisher

__all__ = ("TestCeleryBroker",)


class PendingReply(NamedTuple):
    task_id: str
    future: asyncio.Future[ConsumerMessage]


class PatchedMessage(Message):
    """A kombu message with no channel behind it.

    Acknowledgements update only local state; there is no transport to settle.
    """

    @override
    def ack(self, multiple: bool = False) -> None:
        """Acknowledge the in-memory delivery."""
        self._state = "ACK"

    @override
    def reject(self, requeue: bool = False) -> None:
        """Reject the in-memory delivery."""
        self._state = "REJECTED"

    @override
    def requeue(self) -> None:
        """Mark the in-memory delivery as requeued."""
        self._state = "REQUEUED"


async def build_message(  # ruff: ignore[too-many-arguments]
    message: CelerySendableMessage,
    queue: str,
    *,
    correlation_id: str,
    exchange: str | None = None,
    routing_key: str | None = None,
    headers: MutableHeaders | None = None,
    reply_to: str = "",
    serializer: Optional["SerializerProto"] = None,
    codec: Optional["CodecProto"] = None,
) -> ConsumerMessage:
    """Build the message a Celery consumer would have received for ``message``."""
    body: bytes
    content_type: str | None
    message_headers: MutableHeaders

    if isinstance(message, CeleryTask):
        envelope = build_task_envelope(message, task_id=correlation_id)
        body = dump_json(envelope.body)
        content_type = CONTENT_TYPE
        message_headers = dict(envelope.headers)
        message_headers.update(headers or {})

    else:
        body, content_type = await (codec or DefaultCodec()).encode(message, serializer)
        message_headers = dict(headers or {})

    raw = PatchedMessage(
        # Bytes, the way every real transport delivers a body; the kombu
        # stubs describe the narrower `str` a producer may pass instead.
        body=body,  # type: ignore[arg-type]
        content_type=content_type,
        content_encoding=CONTENT_ENCODING,
        headers=message_headers,
        properties={
            "correlation_id": correlation_id,
            "reply_to": reply_to or None,
        },
        delivery_info={
            "exchange": exchange if exchange is not None else queue,
            "routing_key": routing_key or queue,
        },
        delivery_tag=correlation_id,
    )

    return ConsumerMessage(raw, raw)


@contextmanager
def change_result_backend(config: CeleryBrokerConfig) -> Generator[None]:
    """Swap a configured result backend for an in-process one.

    A broker built with ``result_backend=`` reports every outcome to it, and
    ``request()`` reads the result back from it — but fake mode never connects,
    so the real backend has no client to write through. The in-memory stand-in
    keeps both paths working with no service behind them.

    A broker without a backend keeps having none: `request()` must stay on its
    reply path, which is what the fake producer answers.
    """
    if config.result_backend is None:
        yield
        return

    original, config.result_backend = config.result_backend, InMemoryResultBackend()
    try:
        yield
    finally:
        config.result_backend = original


class FakeProducer(CeleryFastProducer):
    """Delivers published messages to in-process subscribers."""

    def __init__(
        self,
        broker: CeleryBroker,
        brokers: Sequence[CeleryBroker],
        *,
        reply_queues: dict[str, PendingReply],
        request_tasks: set[asyncio.Task[Any]],
    ) -> None:
        super().__init__(
            parser=broker.config.broker_parser,
            decoder=broker.config.broker_decoder,
            id_generator=broker.config.id_generator,
        )
        self.broker = broker
        self.brokers = brokers
        self._reply_queues = reply_queues
        self._request_tasks = request_tasks

        self.codec = broker.config.broker_codec or DefaultCodec()
        self.serializer = resolve_serializer(broker.config.fd_config)
        self._result_backend = broker.config.broker_config.result_backend

    @property
    def subscribers(self) -> Iterator[CelerySubscriber]:
        return (cast("CelerySubscriber", s) for b in self.brokers for s in b.subscribers)

    @override
    async def publish(self, cmd: CeleryPublishCommand) -> None:
        incoming = await self._build_message(cmd)

        if pending := self._reply_queues.get(cmd.queue):
            if (
                not pending.future.done()
                and incoming.message.properties.get("correlation_id") == pending.task_id
            ):
                try:
                    result = incoming.message.decode()
                except DECODE_ERRORS as exc:
                    pending.future.set_exception(exc)
                    return
                if (
                    isinstance(result, Mapping)
                    and result.get("task_id") == pending.task_id
                    and result.get("status") in READY_STATES
                ):
                    pending.future.set_result(ConsumerMessage(incoming.message))
            return

        handler = self.find_handler(cmd, incoming)
        if cmd.publish_type is PublishType.REQUEST:
            if handler is not None:
                task = asyncio.create_task(handler.consume(incoming))
                self._request_tasks.add(task)
                task.add_done_callback(self._request_tasks.discard)
            return

        if handler is not None:
            await self._execute_handler(incoming, handler)
        elif cmd.publish_type is not PublishType.REPLY:
            # A reply queue belongs to the (absent) client, so an unconsumed
            # reply is expected; an unconsumed task is a user mistake.
            raise SubscriberNotFound

    @override
    async def request_via_rpc(self, cmd: CeleryPublishCommand) -> ConsumerMessage:
        timeout = DEFAULT_REQUEST_TIMEOUT if cmd.timeout is None else cmd.timeout
        task_id = cmd.correlation_id or self._id_generator()
        reply_queue = f"faststream-celery.reply.{gen_cor_id()}"
        future: asyncio.Future[ConsumerMessage] = (
            asyncio.get_running_loop().create_future()
        )
        self._reply_queues[reply_queue] = PendingReply(task_id, future)
        request = copy(cmd)
        request.correlation_id = task_id
        request.reply_to = reply_queue
        try:
            with anyio.move_on_after(timeout):
                await self.publish(request)
                return await future
        finally:
            self._reply_queues.pop(reply_queue, None)

        msg = f"No Celery reply for task {task_id!r} within {timeout} seconds."
        raise TimeoutError(msg)

    async def _build_message(self, cmd: CeleryPublishCommand) -> ConsumerMessage:
        return await build_message(
            cmd.body,
            cmd.queue,
            exchange=cmd.exchange,
            routing_key=cmd.routing_key,
            correlation_id=cmd.correlation_id or self._id_generator(),
            headers=cmd.headers,
            reply_to=cmd.reply_to,
            serializer=self.serializer,
            codec=self.codec,
        )

    def find_handler(
        self,
        cmd: CeleryPublishCommand,
        incoming: ConsumerMessage,
    ) -> CelerySubscriber | None:
        """Choose one claimant from the handlers registered for this queue."""
        handlers = (
            handler
            for handler in self.subscribers
            if handler.queue == cmd.queue and handler.calls
        )
        return select_subscriber(handlers, incoming)

    async def _execute_handler(
        self,
        msg: ConsumerMessage,
        handler: CelerySubscriber,
    ) -> ConsumerMessage:
        result = await handler.process_message(msg)

        return await build_message(
            result.body,
            msg.message.delivery_info.get("routing_key", ""),
            correlation_id=result.correlation_id or self._id_generator(),
            headers=result.headers,
            serializer=self.serializer,
            codec=self.codec,
        )


class TestCeleryBroker(TestBroker[CeleryBroker, EnterType]):
    """A class to test Celery brokers."""

    @overload
    def __init__(
        self: "TestCeleryBroker[CeleryBroker]",
        broker: CeleryBroker,
        /,
        *,
        with_real: bool = False,
        connect_only: bool | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: "TestCeleryBroker[tuple[CeleryBroker, ...]]",
        *brokers: CeleryBroker,
        with_real: bool = False,
        connect_only: bool | None = None,
    ) -> None: ...

    def __init__(
        self,
        *brokers: CeleryBroker,
        with_real: bool = False,
        connect_only: bool | None = None,
    ) -> None:
        self._reply_queues: dict[str, PendingReply] = {}
        self._request_tasks: set[asyncio.Task[Any]] = set()
        super().__init__(
            *brokers,
            with_real=with_real,
            connect_only=connect_only,
        )

    @override
    @asynccontextmanager
    async def _create_ctx(self) -> AsyncGenerator[list[CeleryBroker]]:
        async with super()._create_ctx() as brokers:
            try:
                yield brokers
            finally:
                tasks = tuple(self._request_tasks)
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self._request_tasks.clear()
                for pending in self._reply_queues.values():
                    pending.future.cancel()
                self._reply_queues.clear()

    @override
    @contextmanager
    def _patch_producer(self, broker: CeleryBroker) -> Generator[None]:
        config = broker.config.broker_config

        with (
            change_result_backend(config),
            change_producer(
                config,
                FakeProducer(
                    broker,
                    self.brokers,
                    reply_queues=self._reply_queues,
                    request_tasks=self._request_tasks,
                ),
            ),
        ):
            yield

    @staticmethod
    @override
    async def _fake_connect(
        broker: CeleryBroker,
        *args: object,
        **kwargs: object,
    ) -> None:
        """No connection is needed — the fake producer never touches kombu."""

    @override
    def create_publisher_fake_subscriber(
        self,
        broker: CeleryBroker,
        publisher: "CeleryPublisher",
    ) -> tuple[CelerySubscriber, bool]:
        subscribers: list[CelerySubscriber] = []
        for handler in (s for owner in self.brokers for s in owner.subscribers):
            subscriber = cast("CelerySubscriber", handler)
            if subscriber.queue == publisher.queue and subscriber.calls:
                subscribers.append(subscriber)
        if subscribers:
            # The hook accepts one endpoint; a queue can have several task handlers.
            # Copy the endpoint so collecting their calls leaves real routing intact.
            subscriber = copy(subscribers[0])
            subscriber.calls = CallsCollection(
                call for candidate in subscribers for call in candidate.calls
            )
            return subscriber, True

        return broker.subscriber(queue=publisher.queue, persistent=False), False
