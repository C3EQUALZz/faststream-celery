"""The kombu thread bridge, driven over kombu's in-process transport.

`memory://` gives a real `Consumer`, real `drain_events` and real acks, so
the sync-to-async boundary is exercised for real without a broker to run.
"""

import asyncio
import json
from collections.abc import Callable
from typing import Any

import pytest
from faststream.exceptions import IncorrectState
from kombu import Connection, Exchange, Producer, Queue

from faststream_celery.subscriber.bridge import ConsumerBridge
from tests.helpers import MEMORY_URL

DRAIN_TIMEOUT = 0.05
GET_TIMEOUT = 5.0


def connection_factory(url: str = MEMORY_URL) -> Callable[[], Connection]:
    return lambda: Connection(url)


def publish(queue: str, body: Any, *, headers: dict[str, Any] | None = None) -> None:
    """Put a message on the queue the way a Celery client would."""
    exchange = Exchange(queue, type="direct", durable=True)

    with Connection(MEMORY_URL) as connection:
        Producer(connection).publish(
            body,
            serializer="json",
            headers=headers or {},
            exchange=exchange,
            routing_key=queue,
            declare=[
                Queue(queue, exchange=exchange, routing_key=queue, durable=True),
            ],
        )


@pytest.fixture()
def bridge(queue: str) -> ConsumerBridge:
    return ConsumerBridge(
        connection_factory=connection_factory(),
        queue_name=queue,
        accept=["json"],
        prefetch_count=1,
        drain_timeout=DRAIN_TIMEOUT,
    )


class TestLifecycle:
    @pytest.mark.asyncio()
    async def test_start_then_stop(self, bridge: ConsumerBridge) -> None:
        await bridge.start()
        assert bridge._thread is not None

        await bridge.stop()
        assert bridge._thread is None

    @pytest.mark.asyncio()
    async def test_start_is_idempotent(self, bridge: ConsumerBridge) -> None:
        await bridge.start()
        thread = bridge._thread

        await bridge.start()

        assert bridge._thread is thread
        await bridge.stop()

    @pytest.mark.asyncio()
    async def test_stop_without_start_is_safe(self, bridge: ConsumerBridge) -> None:
        await bridge.stop()

    @pytest.mark.asyncio()
    async def test_a_bad_url_is_reported_to_the_caller(self, queue: str) -> None:
        """The consumer thread's failure surfaces where `start()` was awaited."""
        bridge = ConsumerBridge(
            connection_factory=connection_factory("amqp://127.0.0.1:1/"),
            queue_name=queue,
            accept=["json"],
            prefetch_count=1,
            drain_timeout=DRAIN_TIMEOUT,
        )

        with pytest.raises(OSError):  # ruff: ignore[pytest-raises-too-broad]
            await bridge.start()

        assert bridge._thread is None


class TestMessageFlow:
    @pytest.mark.asyncio()
    async def test_a_published_message_crosses_to_the_loop(
        self,
        bridge: ConsumerBridge,
        queue: str,
    ) -> None:
        await bridge.start()

        publish(queue, [[1, 2], {}, {}], headers={"task": "proj.tasks.add", "id": "1"})
        msg = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)

        assert msg.message.headers["task"] == "proj.tasks.add"
        assert msg.message.decode() == [[1, 2], {}, {}]

        await bridge.stop()

    @pytest.mark.asyncio()
    async def test_messages_keep_their_order(
        self,
        bridge: ConsumerBridge,
        queue: str,
    ) -> None:
        await bridge.start()

        for index in range(3):
            publish(queue, [[index], {}, {}], headers={"task": "t", "id": str(index)})

        received = []
        for _ in range(3):
            msg = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)
            received.append(msg.message.headers["id"])
            await msg.executor(msg.message.ack)

        assert received == ["0", "1", "2"]

        await bridge.stop()

    @pytest.mark.asyncio()
    async def test_ack_runs_on_the_consumer_thread(
        self,
        bridge: ConsumerBridge,
        queue: str,
    ) -> None:
        """The ack hops back to the thread that owns the channel."""
        await bridge.start()

        publish(queue, [[], {}, {}], headers={"task": "t", "id": "1"})
        msg = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)

        await msg.executor(msg.message.ack)

        assert msg.message.acknowledged

        await bridge.stop()

    @pytest.mark.asyncio()
    async def test_a_failing_ack_reaches_the_caller(
        self,
        bridge: ConsumerBridge,
        queue: str,
    ) -> None:
        await bridge.start()

        publish(queue, [[], {}, {}], headers={"task": "t", "id": "1"})
        msg = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)
        await msg.executor(msg.message.ack)

        # Acking twice is a kombu MessageStateError, raised on the thread.
        with pytest.raises(Exception, match="already acknowledged"):
            await msg.executor(msg.message.ack)

        await bridge.stop()


class TestAckExecutor:
    @pytest.mark.asyncio()
    async def test_execute_before_start_is_rejected(
        self,
        bridge: ConsumerBridge,
    ) -> None:
        with pytest.raises(IncorrectState, match="not running"):
            await bridge.execute(lambda: None)

    @pytest.mark.asyncio()
    async def test_execute_after_stop_is_rejected(
        self,
        bridge: ConsumerBridge,
    ) -> None:
        await bridge.start()
        await bridge.stop()

        with pytest.raises(IncorrectState, match="not running"):
            await bridge.execute(lambda: None)


class TestPrefetch:
    def test_raise_prefetch_widens_the_window(self, bridge: ConsumerBridge) -> None:
        bridge.raise_prefetch(10)

        assert bridge._prefetch_count == 10

    def test_raise_prefetch_never_narrows(self, bridge: ConsumerBridge) -> None:
        bridge.raise_prefetch(10)
        bridge.raise_prefetch(2)

        assert bridge._prefetch_count == 10

    @pytest.mark.asyncio()
    async def test_a_widened_window_is_applied_while_running(
        self,
        bridge: ConsumerBridge,
        queue: str,
    ) -> None:
        """Messages keep flowing after the consumer re-negotiates QoS."""
        await bridge.start()
        bridge.raise_prefetch(5)

        publish(queue, [[], {}, {}], headers={"task": "t", "id": "1"})
        msg = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)

        assert msg.message.headers["id"] == "1"
        # The consumer thread applies the change on its next pass.
        await _until(lambda: not bridge._prefetch_changed.is_set())

        await bridge.stop()


class TestPayloads:
    @pytest.mark.asyncio()
    async def test_an_unaccepted_serializer_still_delivers_the_message(
        self,
        bridge: ConsumerBridge,
        queue: str,
    ) -> None:
        """Deciding what to do with it belongs to the parser, not the bridge."""
        await bridge.start()

        exchange = Exchange(queue, type="direct", durable=True)
        with Connection(MEMORY_URL) as connection:
            Producer(connection).publish(
                json.dumps({"a": 1}),
                content_type="application/x-unknown",
                content_encoding="utf-8",
                exchange=exchange,
                routing_key=queue,
                declare=[
                    Queue(queue, exchange=exchange, routing_key=queue, durable=True),
                ],
            )

        msg = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)

        assert msg.message.content_type == "application/x-unknown"

        await bridge.stop()


async def _until(predicate: Callable[[], bool], timeout: float = GET_TIMEOUT) -> None:
    """Wait for a state the bridge reaches on its consumer thread."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)

    msg = f"condition not reached within {timeout}s"
    raise AssertionError(msg)
