"""RabbitMQ: the shared transport suite plus AMQP-only behaviour."""

import asyncio
from pathlib import Path
from typing import Any

import pytest
from celery import Celery
from faststream import FastStream, TestApp

from faststream_celery import CeleryBroker, CeleryTask
from tests.helpers import running

from ..base import CONSUME_TIMEOUT, WORKER_TIMEOUT, TransportTestcase

pytestmark = [pytest.mark.connected(), pytest.mark.rabbit(), pytest.mark.slow()]


class TestRabbitTransport(TransportTestcase):
    """Every transport behaviour, over RabbitMQ."""


class TestAmqpRpc:
    """Task results over AMQP RPC — `reply_to` plus `correlation_id`."""

    @pytest.mark.asyncio()
    async def test_celery_client_gets_our_handler_result(
        self,
        broker: CeleryBroker,
        celery_app: Celery,
        queue: str,
    ) -> None:
        """`send_task(...).get()` returns what our handler returned."""

        @broker.subscriber(queue, task="tests.add")
        async def handler(args: list[int], kwargs: dict[str, Any]) -> int:
            return sum(args)

        async with running(broker):
            async_result = celery_app.send_task(
                "tests.add",
                args=[2, 3],
                kwargs={},
                queue=queue,
            )
            value = await asyncio.to_thread(async_result.get, timeout=CONSUME_TIMEOUT)

        assert value == 5

    @pytest.mark.asyncio()
    async def test_celery_client_sees_our_handler_failure(
        self,
        broker: CeleryBroker,
        celery_app: Celery,
        queue: str,
    ) -> None:
        """An exception in our handler reaches the client as FAILURE."""

        @broker.subscriber(queue, task="tests.fail")
        async def handler() -> None:
            msg = "boom"
            raise ValueError(msg)

        async with running(broker):
            async_result = celery_app.send_task(
                "tests.fail",
                args=[],
                kwargs={},
                queue=queue,
            )

            with pytest.raises(ValueError, match="boom"):
                await asyncio.to_thread(async_result.get, timeout=CONSUME_TIMEOUT)

            assert async_result.status == "FAILURE"

    @pytest.mark.asyncio()
    async def test_our_request_gets_a_celery_worker_result(
        self,
        broker: CeleryBroker,
        queue: str,
        celery_worker: Path,
    ) -> None:
        """`broker.request(...)` returns a real worker's result."""
        async with running(broker):
            response = await broker.request(
                CeleryTask("tests.add", args=[2, 3]),
                queue=queue,
                timeout=WORKER_TIMEOUT,
            )
            envelope = await response.decode()

        assert isinstance(envelope, dict)
        assert envelope["status"] == "SUCCESS"
        assert envelope["result"] == 5

    @pytest.mark.asyncio()
    async def test_our_request_reports_a_worker_failure(
        self,
        broker: CeleryBroker,
        queue: str,
        celery_worker: Path,
    ) -> None:
        """A task that raises on the worker comes back as a FAILURE envelope."""
        async with running(broker):
            response = await broker.request(
                CeleryTask("tests.fail", args=["nope"]),
                queue=queue,
                timeout=WORKER_TIMEOUT,
            )
            envelope = await response.decode()

        assert isinstance(envelope, dict)
        assert envelope["status"] == "FAILURE"
        assert envelope["result"]["exc_type"] == "ValueError"

    @pytest.mark.asyncio()
    async def test_request_times_out_with_a_clear_error(
        self,
        broker: CeleryBroker,
        queue: str,
    ) -> None:
        """Nothing consumes the queue, so the wait ends in a TimeoutError."""
        async with running(broker):
            with pytest.raises(TimeoutError, match="No Celery reply"):
                await broker.request(
                    CeleryTask("tests.add", args=[2, 3]),
                    queue=queue,
                    timeout=2.0,
                )

    @pytest.mark.asyncio()
    async def test_each_request_uses_its_own_reply_queue(
        self,
        broker: CeleryBroker,
        queue: str,
        celery_worker: Path,
    ) -> None:
        """Concurrent requests do not read each other's replies."""
        async with running(broker):
            responses = await asyncio.gather(
                *(
                    broker.request(
                        CeleryTask("tests.add", args=[value, 0]),
                        queue=queue,
                        timeout=WORKER_TIMEOUT,
                    )
                    for value in range(3)
                ),
            )
            envelopes = [await response.decode() for response in responses]

        assert sorted(envelope["result"] for envelope in envelopes) == [0, 1, 2]


class TestAppLifecycle:
    @pytest.mark.asyncio()
    async def test_faststream_app_starts_and_stops(
        self,
        broker: CeleryBroker,
        queue: str,
    ) -> None:
        """`FastStream(broker)` runs the whole lifecycle against a live broker."""

        @broker.subscriber(queue, task="tests.echo")
        async def handler() -> None: ...

        app = FastStream(broker)

        async with TestApp(app):
            assert await broker.ping(timeout=5.0)
            assert all(subscriber.running for subscriber in broker.subscribers)

        assert not any(subscriber.running for subscriber in broker.subscribers)

    @pytest.mark.asyncio()
    async def test_broker_can_be_restarted(
        self,
        broker: CeleryBroker,
        celery_app: Celery,
        queue: str,
    ) -> None:
        """A stopped broker releases its consumer and can start again."""
        seen: list[str] = []
        first = asyncio.Event()
        second = asyncio.Event()

        @broker.subscriber(queue, task="tests.echo")
        async def handler() -> None:
            seen.append("echo")
            (first if len(seen) == 1 else second).set()

        async with running(broker):
            celery_app.send_task("tests.echo", args=[], kwargs={}, queue=queue)
            await asyncio.wait_for(first.wait(), timeout=CONSUME_TIMEOUT)

        async with running(broker):
            celery_app.send_task("tests.echo", args=[], kwargs={}, queue=queue)
            await asyncio.wait_for(second.wait(), timeout=CONSUME_TIMEOUT)

        assert seen == ["echo", "echo"]
