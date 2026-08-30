from collections.abc import Callable
from functools import partial
from time import monotonic
from typing import TYPE_CHECKING, NamedTuple, Optional, cast

import anyio
import anyio.to_thread
from faststream.exceptions import FeatureNotSupportedException, IncorrectState
from faststream.message import gen_cor_id
from kombu import Connection, Consumer, Exchange, Producer, Queue
from typing_extensions import override

from faststream_celery._internal import (
    CodecProto,
    DefaultCodec,
    IdGenerator,
    ParserComposition,
    ProducerProto,
)
from faststream_celery.message import ConsumerMessage, run_inline
from faststream_celery.parser import CeleryParser
from faststream_celery.response import CeleryPublishCommand
from faststream_celery.schemas.constants import (
    PERSISTENT_DELIVERY_MODE,
    SERIALIZATION_ACCEPT,
    SERIALIZER,
)
from faststream_celery.schemas.task import CeleryTask, build_task_envelope
from faststream_celery.types import MutableHeaders, TaskBody

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto
    from kombu import Message

    from faststream_celery._internal import CustomCallable

DEFAULT_REQUEST_TIMEOUT = 30.0

ConnectionFactory = Callable[[], Connection]


class Destination(NamedTuple):
    """Where a message goes on the wire."""

    exchange: Exchange
    routing_key: str
    declare: list[Queue] | None


class Payload(NamedTuple):
    """What goes on the wire, independent of the connection used to send it."""

    body: TaskBody | bytes
    serializer: str | None
    content_type: str | None
    headers: MutableHeaders | None
    correlation_id: str | None
    delivery_mode: int | None


class CeleryFastProducer(ProducerProto[CeleryPublishCommand]):
    """Publishes Celery-compatible messages over a kombu write connection.

    kombu is synchronous, so the actual socket write is offloaded to a
    thread; a lock keeps concurrent publishes from touching the connection
    at once (kombu connections are not thread-safe).
    """

    def __init__(
        self,
        *,
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
        id_generator: IdGenerator = gen_cor_id,
    ) -> None:
        default = CeleryParser()
        self._parser = ParserComposition(parser, default.parse_message)
        self._decoder = ParserComposition(decoder, default.decode_message)
        self._id_generator = id_generator

        self.serializer: SerializerProto | None = None
        self.codec: CodecProto = DefaultCodec()

        self._connection: Connection | None = None
        self._connection_factory: ConnectionFactory | None = None
        self._producer: Producer | None = None
        self._lock: anyio.Lock | None = None

    @property
    def parser(self) -> ParserComposition:
        """The parser a reply travels through."""
        return self._parser

    @property
    def decoder(self) -> ParserComposition:
        """The decoder a reply travels through."""
        return self._decoder

    def connect(
        self,
        connection: Connection,
        *,
        connection_factory: ConnectionFactory,
        serializer: Optional["SerializerProto"] = None,
        codec: CodecProto | None = None,
    ) -> None:
        self._connection = connection
        self._connection_factory = connection_factory
        self._producer = Producer(connection)
        self._lock = anyio.Lock()
        self.serializer = serializer
        self.codec = codec or DefaultCodec()

    async def disconnect(self) -> None:
        connection, self._connection = self._connection, None
        self._producer = None
        self._connection_factory = None
        if connection is not None:
            await anyio.to_thread.run_sync(connection.close)

    @override
    async def publish(self, cmd: CeleryPublishCommand) -> None:
        producer = self._producer
        if producer is None or self._lock is None:
            msg = "You should connect the broker at first."
            raise IncorrectState(msg)

        payload = await self._build_payload(cmd)
        destination = _destination_of(cmd)

        async with self._lock:
            await anyio.to_thread.run_sync(
                partial(
                    publish_sync,
                    producer,
                    payload,
                    destination,
                    reply_to=cmd.reply_to or None,
                ),
            )

    @override
    async def request(self, cmd: CeleryPublishCommand) -> ConsumerMessage:
        """Publish a task and wait for its Celery reply over AMQP RPC.

        The request runs on its own short-lived connection: it declares an
        exclusive reply queue and blocks on ``drain_events`` until the reply
        arrives, which must not interleave with the shared write connection.
        """
        factory = self._connection_factory
        if factory is None:
            msg = "You should connect the broker at first."
            raise IncorrectState(msg)

        payload = await self._build_payload(cmd)
        destination = _destination_of(cmd)

        raw = await anyio.to_thread.run_sync(
            partial(
                request_sync,
                factory,
                payload,
                destination,
                timeout=cmd.timeout or DEFAULT_REQUEST_TIMEOUT,
            ),
        )

        return ConsumerMessage(raw, run_inline)

    @override
    async def publish_batch(self, cmd: CeleryPublishCommand) -> None:
        msg = "CeleryBroker doesn't support publishing in batches."
        raise FeatureNotSupportedException(msg)

    async def _build_payload(self, cmd: CeleryPublishCommand) -> Payload:
        if isinstance(cmd.body, CeleryTask):
            return self._build_task_payload(cmd)
        return await self._build_raw_payload(cmd)

    def _build_task_payload(self, cmd: CeleryPublishCommand) -> Payload:
        task = cast("CeleryTask", cmd.body)
        task_id = cmd.correlation_id or self._id_generator()
        envelope = build_task_envelope(task, task_id=task_id)

        headers: dict[str, object] = dict(envelope.headers)
        headers.update(cmd.headers or {})

        return Payload(
            body=envelope.body,
            serializer=SERIALIZER,
            content_type=None,
            headers=headers,
            correlation_id=task_id,
            delivery_mode=PERSISTENT_DELIVERY_MODE,
        )

    async def _build_raw_payload(self, cmd: CeleryPublishCommand) -> Payload:
        data, content_type = await self.codec.encode(cmd.body, self.serializer)

        return Payload(
            body=data,
            serializer=None,
            content_type=content_type,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            delivery_mode=None,
        )


def _destination_of(cmd: CeleryPublishCommand) -> Destination:
    """Celery's default topology: a direct exchange named after the queue."""
    exchange = Exchange(
        cmd.exchange if cmd.exchange is not None else cmd.queue,
        type="direct",
        durable=True,
    )
    routing_key = cmd.routing_key or cmd.queue

    declare = (
        [Queue(cmd.queue, exchange=exchange, routing_key=routing_key, durable=True)]
        if cmd.declare
        else None
    )

    return Destination(exchange=exchange, routing_key=routing_key, declare=declare)


def publish_sync(
    producer: Producer,
    payload: Payload,
    destination: Destination,
    *,
    reply_to: str | None,
) -> None:
    """Synchronous publish — always runs on a worker thread."""
    producer.publish(
        payload.body,
        serializer=payload.serializer,
        content_type=payload.content_type,
        headers=payload.headers,
        correlation_id=payload.correlation_id,
        reply_to=reply_to,
        exchange=destination.exchange,
        routing_key=destination.routing_key,
        declare=destination.declare,
        delivery_mode=payload.delivery_mode,
    )


def request_sync(
    connection_factory: ConnectionFactory,
    payload: Payload,
    destination: Destination,
    *,
    timeout: float,
) -> "Message":
    """Publish and block until the matching reply lands on a temporary queue."""
    replies: list[Message] = []

    with connection_factory() as connection:
        channel = connection.channel()

        # An empty name asks the broker for a generated, exclusive queue —
        # exactly what a Celery AMQP-RPC client uses for its replies.
        reply_queue = Queue(
            name="",
            exclusive=True,
            auto_delete=True,
            durable=False,
        )(channel)
        reply_queue.declare()

        publish_sync(
            Producer(channel),
            payload,
            destination,
            reply_to=reply_queue.name,
        )

        consumer = Consumer(
            channel,
            queues=[reply_queue],
            accept=SERIALIZATION_ACCEPT,
            on_message=replies.append,
            # `amq.gen-*` is a reserved name the broker just gave us; only
            # the broker may declare it.
            auto_declare=False,
        )
        consumer.consume()

        deadline = monotonic() + timeout
        while not replies:
            remaining = deadline - monotonic()
            if remaining <= 0:
                msg = (
                    f"No Celery reply for task {payload.correlation_id!r} "
                    f"within {timeout} seconds."
                )
                raise TimeoutError(msg)

            try:
                connection.drain_events(timeout=remaining)
            except TimeoutError:
                continue

    return replies[0]
