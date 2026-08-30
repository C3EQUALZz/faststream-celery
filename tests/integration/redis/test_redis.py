"""Redis: the shared transport suite plus virtual-transport specifics.

kombu emulates queues on Redis (a LIST per queue, `_kombu.binding.*` sets, an
`unacked` hash guarded by `visibility_timeout`), so the same broker code has
to work with no AMQP exchange anywhere in sight.
"""

import asyncio
import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import pytest
from celery import Celery

from faststream_celery import CeleryBroker, CeleryTask
from tests.helpers import running
from tests.integration.base import CONSUME_TIMEOUT, TransportTestcase, wait_for

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
