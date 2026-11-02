"""The kombu thread bridge, driven over kombu's in-process transport.

`memory://` gives a real `Consumer`, real `drain_events` and real acks, so
the sync-to-async boundary is exercised for real without a broker to run.
"""

import asyncio
import json
import time
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pytest
from faststream.exceptions import IncorrectState
from faststream.message import AckStatus
from kombu import Connection, Exchange, Producer, Queue

from faststream_celery import CeleryBroker
from faststream_celery.subscriber.bridge import (
    SETTLE_DRAIN_TIMEOUT,
    ConsumerBridge,
)
from tests.helpers import MEMORY_URL, ConnectionFaults, running

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
            await msg.settle(AckStatus.ACKED)

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

        await msg.settle(AckStatus.ACKED)

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
        await msg.settle(AckStatus.ACKED)

        # Acking twice is a kombu MessageStateError, raised on the thread.
        with pytest.raises(Exception, match="already acknowledged"):
            await msg.settle(AckStatus.ACKED)

        await bridge.stop()


class TestSettleLatency:
    """An unsettled delivery shortens the blocking read.

    An acknowledgement queued by the event loop runs when the thread comes back
    from `drain_events`, and with the QoS window full no frame will arrive to
    wake it. Idling at `drain_timeout` there would cap a `prefetch_count=1`
    subscriber at one message per `drain_timeout`.
    """

    def test_an_idle_thread_blocks_for_the_drain_timeout(
        self,
        bridge: ConsumerBridge,
    ) -> None:
        assert bridge._next_timeout() == DRAIN_TIMEOUT

    @pytest.mark.asyncio()
    async def test_an_unsettled_delivery_shortens_the_wait(
        self,
        bridge: ConsumerBridge,
        queue: str,
    ) -> None:
        await bridge.start()

        publish(queue, [[], {}, {}], headers={"task": "t", "id": "1"})
        msg = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)

        assert bridge._next_timeout() == min(DRAIN_TIMEOUT, SETTLE_DRAIN_TIMEOUT)

        await msg.settle(AckStatus.ACKED)

        assert bridge._next_timeout() == DRAIN_TIMEOUT

        await bridge.stop()

    @pytest.mark.asyncio()
    async def test_messages_are_not_paced_by_the_drain_timeout(
        self,
        queue: str,
    ) -> None:
        """Five messages through a one-slot QoS window, faster than five drains.

        With the ack waiting for a full `drain_timeout`, this took at least
        `total * drain_timeout` seconds.
        """
        total = 5
        slow_drain = 1.0

        bridge = ConsumerBridge(
            connection_factory=connection_factory(),
            queue_name=queue,
            accept=["json"],
            prefetch_count=1,
            drain_timeout=slow_drain,
        )

        await bridge.start()

        for index in range(total):
            publish(queue, [[index], {}, {}], headers={"task": "t", "id": str(index)})

        started = time.monotonic()
        for _ in range(total):
            msg = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)
            await msg.settle(AckStatus.ACKED)
        elapsed = time.monotonic() - started

        assert elapsed < total * slow_drain

        await bridge.stop()


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


class TestRecovery:
    @pytest.mark.asyncio()
    async def test_messages_resume_after_a_connection_loss(self, queue: str) -> None:
        faults = ConnectionFaults(MEMORY_URL)
        bridge = _faulty_bridge(faults, queue)
        await bridge.start()
        try:
            publish(queue, "before")
            first = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)
            await first.settle(AckStatus.ACKED)

            faults.unavailable.set()
            faults.disconnect.set()
            await _until(lambda: len(faults.connections) >= 2)
            assert not bridge.is_alive

            faults.unavailable.clear()
            publish(queue, "after")
            recovered = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)
            assert recovered.message.decode() == "after"
            await recovered.settle(AckStatus.ACKED)
            assert bridge.is_alive
        finally:
            faults.unavailable.clear()
            await bridge.stop()

    @pytest.mark.asyncio()
    async def test_old_messages_cannot_ack_on_a_recovered_connection(
        self,
        queue: str,
    ) -> None:
        faults = ConnectionFaults(MEMORY_URL)
        bridge = _faulty_bridge(faults, queue)
        await bridge.start()
        try:
            publish(queue, "old")
            old = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)
            faults.disconnect.set()
            await _until(lambda: len(faults.connections) >= 2 and bridge.is_alive)

            with pytest.raises(IncorrectState, match="closed"):
                await asyncio.wait_for(old.settle(AckStatus.ACKED), timeout=GET_TIMEOUT)
            assert not old.message.acknowledged

            publish(queue, "new")
            new = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)
            assert new.message.channel is not old.message.channel
            await new.settle(AckStatus.ACKED)
        finally:
            await bridge.stop()

    @pytest.mark.asyncio()
    async def test_queued_ack_fails_when_its_connection_is_lost(self, queue: str) -> None:
        faults = ConnectionFaults(MEMORY_URL)
        bridge = _faulty_bridge(faults, queue)
        await bridge.start()
        pending: asyncio.Future[None] | None = None
        try:
            publish(queue, "pending")
            message = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)
            faults.pause.set()
            faults.disconnect.set()
            await _until(faults.disconnected.is_set)

            pending = asyncio.ensure_future(message.settle(AckStatus.ACKED))
            await asyncio.sleep(0)
            faults.resume.set()

            with pytest.raises(IncorrectState, match=r"closed|stopped"):
                await asyncio.wait_for(pending, timeout=GET_TIMEOUT)
            assert not message.message.acknowledged
        finally:
            faults.resume.set()
            if pending is not None:
                await asyncio.gather(pending, return_exceptions=True)
            await bridge.stop()

    @pytest.mark.asyncio()
    async def test_fatal_errors_wake_waiting_readers(self, queue: str) -> None:
        faults = ConnectionFaults(MEMORY_URL)
        bridge = _faulty_bridge(faults, queue)
        await bridge.start()
        waiting = [asyncio.create_task(bridge.get()) for _ in range(2)]
        try:
            faults.error = ValueError("Invalid consumer configuration.")
            faults.disconnect.set()
            for reader in waiting:
                with pytest.raises(IncorrectState, match="stopped") as error:
                    await asyncio.wait_for(reader, timeout=GET_TIMEOUT)
                assert isinstance(error.value.__cause__, ValueError)
            with pytest.raises(IncorrectState, match="stopped"):
                await bridge.get()
            assert not bridge.is_alive

            assert len(faults.connections) == 1
        finally:
            await bridge.stop()

    @pytest.mark.asyncio()
    async def test_buffered_old_deliveries_are_discarded(self, queue: str) -> None:
        faults = ConnectionFaults(MEMORY_URL)
        bridge = _faulty_bridge(faults, queue)
        bridge.raise_prefetch(3)
        await bridge.start()
        try:
            for body in ("first", "buffered-1", "buffered-2"):
                publish(queue, body)
            first = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)
            await _until(lambda: bridge._messages.qsize() == 2)
            await first.settle(AckStatus.ACKED)

            faults.disconnect.set()
            await _until(lambda: len(faults.connections) >= 2 and bridge.is_alive)
            publish(queue, "fresh")
            fresh = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)
            assert fresh.message.decode() == "fresh"
            await fresh.settle(AckStatus.ACKED)
        finally:
            await bridge.stop()

    @pytest.mark.asyncio()
    async def test_connection_factory_failure_reaches_start(self, queue: str) -> None:
        failure = ValueError("Invalid connection configuration.")
        bridge = ConsumerBridge(
            connection_factory=MagicMock(side_effect=failure),
            queue_name=queue,
            accept=["json"],
            prefetch_count=1,
        )
        try:
            with pytest.raises(ValueError, match="Invalid connection"):
                await asyncio.wait_for(bridge.start(), timeout=GET_TIMEOUT)
            assert bridge._thread is None
        finally:
            # Release a startup waiter even if a broken thread exited before
            # signalling it, so a regression cannot leak an executor thread.
            bridge._started.set()
            await bridge.stop()

    @pytest.mark.asyncio()
    async def test_ack_transport_failure_triggers_recovery(
        self,
        queue: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        faults = ConnectionFaults(MEMORY_URL)
        bridge = _faulty_bridge(faults, queue)
        await bridge.start()
        try:
            publish(queue, "old")
            old = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)
            failure = MagicMock(side_effect=OSError("Ack connection lost."))
            monkeypatch.setattr(old.message, "ack", failure)
            with pytest.raises(OSError, match="Ack connection lost"):
                await asyncio.wait_for(old.settle(AckStatus.ACKED), timeout=GET_TIMEOUT)

            await _until(lambda: len(faults.connections) >= 2 and bridge.is_alive)
            publish(queue, "new")
            new = await asyncio.wait_for(bridge.get(), timeout=GET_TIMEOUT)
            await new.settle(AckStatus.ACKED)
            assert new.message.decode() == "new"
        finally:
            await bridge.stop()

    @pytest.mark.asyncio()
    async def test_stop_wakes_waiting_readers(self, bridge: ConsumerBridge) -> None:
        await bridge.start()
        waiting = [asyncio.create_task(bridge.get()) for _ in range(2)]
        await asyncio.sleep(0)
        await bridge.stop()
        for reader in waiting:
            with pytest.raises(IncorrectState, match="stopped"):
                await asyncio.wait_for(reader, timeout=GET_TIMEOUT)

    @pytest.mark.asyncio()
    async def test_ping_reflects_read_connection_recovery(
        self,
        memory_broker: CeleryBroker,
        queue: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        faults = ConnectionFaults(MEMORY_URL)
        monkeypatch.setattr(memory_broker.config.broker_config, "make_connection", faults)
        memory_broker.subscriber(queue)
        async with running(memory_broker):
            assert await memory_broker.ping()
            faults.unavailable.set()
            faults.disconnect.set()
            try:
                # One write connection, the original reader, and a failed
                # replacement reader; the write connection stays available.
                await _until(lambda: len(faults.connections) >= 3)
                assert not await memory_broker.ping()
            finally:
                faults.unavailable.clear()

            consumer = memory_broker.config.broker_config.consumers._consumers[queue]
            await _until(lambda: consumer.bridge.is_alive)
            assert await memory_broker.ping()

    @pytest.mark.asyncio()
    async def test_stop_interrupts_reconnect_backoff(
        self,
        queue: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            "faststream_celery.subscriber.bridge.RECONNECT_DELAY",
            60.0,
            raising=False,
        )
        faults = ConnectionFaults(MEMORY_URL)
        bridge = _faulty_bridge(faults, queue)
        backoff = MagicMock(wraps=bridge._stop_event.wait)
        monkeypatch.setattr(bridge._stop_event, "wait", backoff)
        await bridge.start()
        try:
            faults.unavailable.set()
            faults.disconnect.set()
            await _until(lambda: backoff.called)
            await asyncio.wait_for(bridge.stop(), timeout=1.0)
        finally:
            faults.unavailable.clear()
            await bridge.stop()

    @pytest.mark.asyncio()
    async def test_a_bridge_can_restart_after_stop(self, bridge: ConsumerBridge) -> None:
        await bridge.start()
        await bridge.stop()
        try:
            await bridge.start()
            assert bridge.is_alive
        finally:
            await bridge.stop()


def _faulty_bridge(faults: ConnectionFaults, queue: str) -> ConsumerBridge:
    return ConsumerBridge(
        connection_factory=faults,
        queue_name=queue,
        accept=["json"],
        prefetch_count=1,
        drain_timeout=DRAIN_TIMEOUT,
    )


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
