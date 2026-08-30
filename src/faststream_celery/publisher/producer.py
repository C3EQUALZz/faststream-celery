from collections.abc import Callable
from functools import partial
from typing import TYPE_CHECKING, Any, Optional, cast

import anyio
import anyio.to_thread
from faststream.exceptions import FeatureNotSupportedException, IncorrectState
from faststream.message import gen_cor_id
from kombu import Connection, Exchange, Producer, Queue
from typing_extensions import override

from faststream_celery._internal import (
    CodecProto,
    DefaultCodec,
    IdGenerator,
    ParserComposition,
    ProducerProto,
)
from faststream_celery.parser import CeleryParser
from faststream_celery.response import CeleryPublishCommand
from faststream_celery.task import CeleryTask, build_task_envelope

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream_celery._internal import CustomCallable

# Celery's default `task_default_delivery_mode` — persistent messages.
PERSISTENT_DELIVERY_MODE = 2


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
        self._producer: Producer | None = None
        self._lock: anyio.Lock | None = None

    def connect(
        self,
        connection: Connection,
        *,
        serializer: Optional["SerializerProto"] = None,
        codec: CodecProto | None = None,
    ) -> None:
        self._connection = connection
        self._producer = Producer(connection)
        self._lock = anyio.Lock()
        self.serializer = serializer
        self.codec = codec or DefaultCodec()

    async def disconnect(self) -> None:
        connection, self._connection = self._connection, None
        self._producer = None
        if connection is not None:
            await anyio.to_thread.run_sync(connection.close)

    @override
    async def publish(self, cmd: CeleryPublishCommand) -> None:
        if self._producer is None or self._lock is None:
            msg = "You should connect the broker at first."
            raise IncorrectState(msg)

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

        if isinstance(cmd.body, CeleryTask):
            publish = self._build_task_publish(cmd, exchange, routing_key, declare)
        else:
            publish = await self._build_raw_publish(cmd, exchange, routing_key, declare)

        async with self._lock:
            await anyio.to_thread.run_sync(publish)

    @override
    async def request(self, cmd: CeleryPublishCommand) -> Any:
        msg = "CeleryBroker doesn't support RPC requests yet."
        raise FeatureNotSupportedException(msg)

    @override
    async def publish_batch(self, cmd: CeleryPublishCommand) -> Any:
        msg = "CeleryBroker doesn't support publishing in batches."
        raise FeatureNotSupportedException(msg)

    def _build_task_publish(
        self,
        cmd: CeleryPublishCommand,
        exchange: Exchange,
        routing_key: str,
        declare: list[Queue] | None,
    ) -> Callable[[], None]:
        task = cast("CeleryTask", cmd.body)
        task_id = cmd.correlation_id or self._id_generator()
        envelope = build_task_envelope(task, task_id=task_id)

        return partial(
            self._publish,
            envelope.body,
            serializer="json",
            headers=envelope.headers,
            correlation_id=task_id,
            reply_to=cmd.reply_to or None,
            exchange=exchange,
            routing_key=routing_key,
            declare=declare,
            delivery_mode=PERSISTENT_DELIVERY_MODE,
        )

    async def _build_raw_publish(
        self,
        cmd: CeleryPublishCommand,
        exchange: Exchange,
        routing_key: str,
        declare: list[Queue] | None,
    ) -> Callable[[], None]:
        data, content_type = await self.codec.encode(cmd.body, self.serializer)

        return partial(
            self._publish,
            data,
            serializer=None,
            content_type=content_type,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            reply_to=cmd.reply_to or None,
            exchange=exchange,
            routing_key=routing_key,
            declare=declare,
            delivery_mode=None,
        )

    def _publish(  # ruff: ignore[too-many-arguments]
        self,
        body: Any,
        *,
        serializer: str | None,
        headers: dict[str, Any] | None,
        correlation_id: str | None,
        reply_to: str | None,
        exchange: Exchange,
        routing_key: str,
        declare: list[Queue] | None,
        delivery_mode: int | None,
        content_type: str | None = None,
    ) -> None:
        """Synchronous publish — always runs on a worker thread under the lock."""
        producer = self._producer
        if producer is None:  # pragma: no cover
            msg = "You should connect the broker at first."
            raise IncorrectState(msg)

        producer.publish(
            body,
            serializer=serializer,
            content_type=content_type,
            headers=headers,
            correlation_id=correlation_id,
            reply_to=reply_to,
            exchange=exchange,
            routing_key=routing_key,
            declare=declare,
            delivery_mode=delivery_mode,
        )
