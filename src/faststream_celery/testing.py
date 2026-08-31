"""In-memory ``TestCeleryBroker`` for application tests.

Modeled on ``faststream/rabbit/testing.py``: the broker's producer is swapped
for a fake that routes a publish straight into the matching subscribers'
``process_message()``, so middlewares, the parser and DI all run without a
kombu connection or a live Celery.
"""

from collections.abc import Generator, Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional, cast

import anyio
from faststream.exceptions import SubscriberNotFound
from faststream.response import PublishType
from kombu import Message
from typing_extensions import overload, override

from faststream_celery._internal import (
    DefaultCodec,
    EnterType,
    ParserComposition,
    TestBroker,
    change_producer,
    dump_json,
    resolve_serializer,
)
from faststream_celery.backend import InMemoryResultBackend
from faststream_celery.broker import CeleryBroker
from faststream_celery.configs import CeleryBrokerConfig
from faststream_celery.message import ConsumerMessage, run_inline
from faststream_celery.parser import CeleryParser, read_headers
from faststream_celery.publisher.producer import CeleryFastProducer
from faststream_celery.response import CeleryPublishCommand
from faststream_celery.schemas.constants import CONTENT_ENCODING, CONTENT_TYPE
from faststream_celery.schemas.task import (
    CelerySendableMessage,
    CeleryTask,
    build_task_envelope,
)
from faststream_celery.subscriber import CelerySubscriber
from faststream_celery.types import MutableHeaders

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream_celery._internal import CodecProto
    from faststream_celery.publisher import CeleryPublisher

__all__ = ("TestCeleryBroker",)


class PatchedMessage(Message):
    """A kombu message with no channel behind it.

    Acknowledgements are no-ops: in fake mode nothing was delivered by a
    broker, so there is nothing to settle.
    """

    @override
    def ack(self, multiple: bool = False) -> None:
        """Acknowledge the message (a no-op without a real channel)."""

    @override
    def reject(self, requeue: bool = False) -> None:
        """Reject the message (a no-op without a real channel)."""

    @override
    def requeue(self) -> None:
        """Requeue the message (a no-op without a real channel)."""


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

    return ConsumerMessage(raw, run_inline)


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
    ) -> None:
        self.broker = broker
        self.brokers = brokers

        default = CeleryParser()
        self._parser = ParserComposition(
            broker.config.broker_parser,
            default.parse_message,
        )
        self._decoder = ParserComposition(
            broker.config.broker_decoder,
            default.decode_message,
        )
        self._id_generator = broker.config.id_generator

        self.codec = broker.config.broker_codec or DefaultCodec()
        self.serializer = resolve_serializer(broker.config.fd_config)

    @property
    def subscribers(self) -> Iterator[CelerySubscriber]:
        return (cast("CelerySubscriber", s) for b in self.brokers for s in b.subscribers)

    @override
    async def publish(self, cmd: CeleryPublishCommand) -> None:
        incoming = await self._build_message(cmd)

        called = False
        for handler in self._find_handlers(cmd, incoming):
            called = True
            await self._execute_handler(incoming, handler)

        if not called and cmd.publish_type is not PublishType.REPLY:
            # A reply queue belongs to the (absent) client, so an unconsumed
            # reply is expected; an unconsumed task is a user mistake.
            raise SubscriberNotFound

    @override
    async def request(self, cmd: CeleryPublishCommand) -> ConsumerMessage:
        incoming = await self._build_message(cmd)

        for handler in self._find_handlers(cmd, incoming):
            with anyio.fail_after(cmd.timeout):
                return await self._execute_handler(incoming, handler)

        raise SubscriberNotFound

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

    def _find_handlers(
        self,
        cmd: CeleryPublishCommand,
        incoming: ConsumerMessage,
    ) -> Iterator[CelerySubscriber]:
        """Subscribers on ``cmd.queue`` whose ``task=`` filter lets this through."""
        task_name = read_headers(incoming.message).get("task")

        for handler in self.subscribers:
            if handler.queue != cmd.queue:
                continue

            expected_task = handler.config.task
            if expected_task is not None and expected_task != task_name:
                continue

            yield handler

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
        super().__init__(
            *brokers,
            with_real=with_real,
            connect_only=connect_only,
        )

    @override
    @contextmanager
    def _patch_producer(self, broker: CeleryBroker) -> Generator[None]:
        config = broker.config.broker_config

        with (
            change_producer(config, FakeProducer(broker, self.brokers)),
            change_result_backend(config),
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
        for handler in (s for b in self.brokers for s in b.subscribers):
            subscriber = cast("CelerySubscriber", handler)
            if subscriber.queue == publisher.queue:
                return subscriber, True

        return broker.subscriber(queue=publisher.queue, persistent=False), False
