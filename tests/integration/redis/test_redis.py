"""Redis: the shared transport suite plus virtual-transport specifics.

kombu emulates queues on Redis (a LIST per queue, `_kombu.binding.*` sets, an
`unacked` hash guarded by `visibility_timeout`), so the same broker code has
to work with no AMQP exchange anywhere in sight.
"""

import asyncio
import json
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from celery import Celery

from faststream_celery import CeleryBroker, CeleryTask
from tests.helpers import running
from tests.integration.base import (
    CONSUME_TIMEOUT,
    WORKER_TIMEOUT,
    TransportTestcase,
    wait_for,
)

if TYPE_CHECKING:
    import redis

pytestmark = [pytest.mark.connected(), pytest.mark.redis(), pytest.mark.slow()]

# Client and worker must agree on this, or one redelivers what the other is
# still working on (docs/design.md §11).
VISIBILITY_TIMEOUT = 3600

# kombu keeps in-flight messages of every queue in this one hash.
UNACKED_KEY = "unacked"


def _entries_for(unacked: Mapping[Any, Any], queue: str) -> list[Any]:
    """The in-flight entries routed to one queue."""
    entries = []
    for payload in unacked.values():
        message, _exchange, routing_key = json.loads(payload)
        if routing_key == queue:
            entries.append(message)
    return entries


class TestRedisTransport(TransportTestcase):
    """Every transport behaviour, over the Redis virtual transport."""


class TestRedisSpecifics:
    @pytest.mark.asyncio()
    async def test_transport_options_reach_kombu(self, redis_url: str) -> None:
        """`visibility_timeout` is passed through, so both sides can match."""
        broker = CeleryBroker(
            redis_url,
            transport_options={"visibility_timeout": VISIBILITY_TIMEOUT},
        )

        async with running(broker):
            connection = broker.config.broker_config.make_connection()
            options = connection.transport_options or {}

        assert options["visibility_timeout"] == VISIBILITY_TIMEOUT

    @pytest.mark.asyncio()
    async def test_published_task_lands_on_the_queue_list(
        self,
        broker: CeleryBroker,
        queue: str,
        redis_client: Any,
    ) -> None:
        """With no consumer, the task waits in the Redis LIST kombu uses."""
        async with running(broker):
            await broker.publish(
                CeleryTask("tests.echo", args=["queued"]),
                queue=queue,
            )

            payload = await asyncio.to_thread(redis_client.lrange, queue, 0, -1)

        assert len(payload) == 1
        envelope = json.loads(payload[0])
        assert envelope["headers"]["task"] == "tests.echo"

    @pytest.mark.asyncio()
    async def test_acked_task_leaves_no_unacked_entry(
        self,
        broker: CeleryBroker,
        queue: str,
        event: asyncio.Event,
        redis_client: "redis.Redis",
    ) -> None:
        """A handled task is removed from kombu's `unacked` hash.

        `unacked` is one hash for the whole database, so only the entries
        routed to this test's queue say anything about this test.
        """

        @broker.subscriber(queue, task="tests.echo")
        async def handler() -> None:
            event.set()

        async with running(broker):
            await broker.publish(CeleryTask("tests.echo"), queue=queue)
            await wait_for(event)

            # The ack hops to the consumer thread, so give it a moment.
            await asyncio.sleep(1)
            unacked = await asyncio.to_thread(redis_client.hgetall, UNACKED_KEY)

        assert _entries_for(unacked, queue) == []

    @pytest.mark.asyncio()
    async def test_matching_visibility_timeout_on_both_sides(
        self,
        redis_url: str,
        celery_app: Celery,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """A Celery client and our broker interoperate with the same setting."""
        broker = CeleryBroker(
            redis_url,
            transport_options={"visibility_timeout": VISIBILITY_TIMEOUT},
        )
        celery_app.conf.broker_transport_options = {
            "visibility_timeout": VISIBILITY_TIMEOUT,
        }

        received: list[list[int]] = []

        @broker.subscriber(queue, task="tests.add")
        async def handler(args: list[int], kwargs: dict[str, Any]) -> None:
            received.append(args)
            event.set()

        async with running(broker):
            celery_app.send_task("tests.add", args=[7, 8], kwargs={}, queue=queue)
            await wait_for(event, timeout=CONSUME_TIMEOUT)

        assert received == [[7, 8]]


class TestResultBackend:
    """`celery-task-meta-<id>` in both directions (ticket-4)."""

    @pytest.fixture()
    def broker(self, redis_url: str) -> CeleryBroker:
        return CeleryBroker(redis_url, result_backend=redis_url)

    @pytest.mark.asyncio()
    async def test_a_celery_client_reads_our_result(
        self,
        broker: CeleryBroker,
        celery_app: Celery,
        queue: str,
    ) -> None:
        """`send_task(...).get()` reads the meta our handler wrote."""

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
    async def test_a_celery_client_sees_our_failure(
        self,
        broker: CeleryBroker,
        celery_app: Celery,
        queue: str,
    ) -> None:
        """An exception is recorded as FAILURE with a traceback."""

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
            assert "ValueError: boom" in str(async_result.traceback)

    @pytest.mark.asyncio()
    async def test_we_write_the_key_celery_expects(
        self,
        broker: CeleryBroker,
        queue: str,
        event: asyncio.Event,
        redis_client: "redis.Redis",
    ) -> None:
        @broker.subscriber(queue, task="tests.add")
        async def handler() -> int:
            event.set()
            return 7

        async with running(broker):
            await broker.publish(
                CeleryTask("tests.add"),
                queue=queue,
                correlation_id="known-task-id",
            )
            await wait_for(event)
            await asyncio.sleep(0.5)

            payload = await asyncio.to_thread(
                redis_client.get,
                "celery-task-meta-known-task-id",
            )

        assert payload is not None
        meta = json.loads(payload)
        assert meta["status"] == "SUCCESS"
        assert meta["result"] == 7
        assert meta["task_id"] == "known-task-id"
        assert meta["date_done"]

    @pytest.mark.asyncio()
    async def test_our_request_reads_a_celery_worker_result(
        self,
        broker: CeleryBroker,
        queue: str,
        celery_worker: Path,  # ruff: ignore[unused-method-argument]
    ) -> None:
        """`broker.request()` waits on the backend, not a reply queue."""
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
    async def test_our_request_times_out_when_nobody_runs_the_task(
        self,
        broker: CeleryBroker,
        queue: str,
    ) -> None:
        async with running(broker):
            with pytest.raises(TimeoutError, match="No Celery result"):
                await broker.request(
                    CeleryTask("tests.add", args=[2, 3]),
                    queue=queue,
                    timeout=2.0,
                )

    @pytest.mark.asyncio()
    async def test_ignore_result_records_nothing(
        self,
        broker: CeleryBroker,
        queue: str,
        event: asyncio.Event,
        redis_client: "redis.Redis",
    ) -> None:
        @broker.subscriber(queue, task="tests.add")
        async def handler() -> int:
            event.set()
            return 7

        async with running(broker):
            await broker.publish(
                CeleryTask("tests.add"),
                queue=queue,
                correlation_id="ignored-task-id",
                headers={"ignore_result": True},
            )
            await wait_for(event)
            await asyncio.sleep(0.5)

            payload = await asyncio.to_thread(
                redis_client.get,
                "celery-task-meta-ignored-task-id",
            )

        assert payload is None
