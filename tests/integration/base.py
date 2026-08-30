"""Behaviour every supported kombu transport must provide.

Specified once here and inherited by ``test_rabbit.py`` and ``test_redis.py``;
each subclass only supplies the ``broker_url`` its transport runs on. Anything
that depends on a transport's own semantics (AMQP RPC, Redis result keys)
belongs in that transport's module instead.
"""

import asyncio
import time
from pathlib import Path
from typing import Any

import pytest
from celery import Celery

from faststream_celery import CeleryBroker, CeleryTask, signature
from tests.helpers import running

from .celery_tasks import read_log

CONSUME_TIMEOUT = 30.0
WORKER_TIMEOUT = 30.0
COUNTDOWN = 3.0


async def wait_for(event: asyncio.Event, timeout: float = CONSUME_TIMEOUT) -> None:
    await asyncio.wait_for(event.wait(), timeout=timeout)


def wait_for_log(log_file: Path, timeout: float = WORKER_TIMEOUT) -> list[Any]:
    """Poll the worker's log file until it records something."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if entries := read_log(log_file):
            return entries
        time.sleep(0.2)

    return read_log(log_file)


class TransportTestcase:
    """Wire compatibility with a live Celery, on one kombu transport."""

    @pytest.mark.asyncio()
    async def test_celery_client_reaches_our_subscriber(
        self,
        broker: CeleryBroker,
        celery_app: Celery,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """`app.send_task` from a real Celery client is consumed by our handler."""
        received: dict[str, Any] = {}

        @broker.subscriber(queue, task="tests.add")
        async def handler(args: list[int], kwargs: dict[str, Any]) -> None:
            received["args"] = args
            received["kwargs"] = kwargs
            event.set()

        async with running(broker):
            celery_app.send_task(
                "tests.add",
                args=[2, 3],
                kwargs={"debug": True},
                queue=queue,
            )
            await wait_for(event)

        assert received == {"args": [2, 3], "kwargs": {"debug": True}}

    @pytest.mark.asyncio()
    async def test_our_publish_is_executed_by_a_celery_worker(
        self,
        broker: CeleryBroker,
        queue: str,
        celery_worker: Path,
    ) -> None:
        """`broker.publish(CeleryTask(...))` runs on a real `celery worker`."""
        async with running(broker):
            await broker.publish(CeleryTask("tests.echo", args=["hello"]), queue=queue)

        assert wait_for_log(celery_worker) == [
            {"task": "tests.echo", "payload": "hello"},
        ]

    @pytest.mark.asyncio()
    async def test_task_filter_ignores_other_tasks(
        self,
        broker: CeleryBroker,
        celery_app: Celery,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """A subscriber bound to one task name does not pick up another."""
        seen: list[str] = []

        @broker.subscriber(queue, task="tests.echo")
        async def echo_handler() -> None:
            seen.append("echo")
            event.set()

        async with running(broker):
            celery_app.send_task("tests.add", args=[1, 1], kwargs={}, queue=queue)
            celery_app.send_task("tests.echo", args=[], kwargs={}, queue=queue)
            await wait_for(event)

        assert seen == ["echo"]

    @pytest.mark.asyncio()
    async def test_two_subscribers_share_one_queue(
        self,
        broker: CeleryBroker,
        celery_app: Celery,
        queue: str,
    ) -> None:
        """Both subscribers on a queue get their own task, in either order.

        They share a single kombu consumer; one consumer each would make the
        broker round-robin the messages and each would drop the other's.
        """
        seen: list[str] = []
        both = asyncio.Event()

        def note(name: str) -> None:
            seen.append(name)
            if len(seen) == 2:
                both.set()

        @broker.subscriber(queue, task="tests.add")
        async def add_handler() -> None:
            note("add")

        @broker.subscriber(queue, task="tests.echo")
        async def echo_handler() -> None:
            note("echo")

        async with running(broker):
            celery_app.send_task("tests.add", args=[1, 1], kwargs={}, queue=queue)
            celery_app.send_task("tests.echo", args=[], kwargs={}, queue=queue)
            await wait_for(both)

        assert sorted(seen) == ["add", "echo"]

    @pytest.mark.asyncio()
    async def test_a_catch_all_subscriber_takes_the_rest(
        self,
        broker: CeleryBroker,
        celery_app: Celery,
        queue: str,
    ) -> None:
        """A subscriber with no `task=` receives what the named ones do not."""
        seen: list[str] = []
        both = asyncio.Event()

        def note(name: str) -> None:
            seen.append(name)
            if len(seen) == 2:
                both.set()

        @broker.subscriber(queue, task="tests.add")
        async def add_handler() -> None:
            note("add")

        @broker.subscriber(queue)
        async def catch_all() -> None:
            note("other")

        async with running(broker):
            celery_app.send_task("tests.add", args=[1, 1], kwargs={}, queue=queue)
            celery_app.send_task("tests.fail", args=[], kwargs={}, queue=queue)
            await wait_for(both)

        assert sorted(seen) == ["add", "other"]

    @pytest.mark.asyncio()
    async def test_countdown_delays_our_subscriber(
        self,
        broker: CeleryBroker,
        celery_app: Celery,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """A Celery client's `countdown` defers our handler."""
        ran_at: list[float] = []

        @broker.subscriber(queue, task="tests.echo")
        async def handler() -> None:
            ran_at.append(time.monotonic())
            event.set()

        async with running(broker):
            sent_at = time.monotonic()
            celery_app.send_task(
                "tests.echo",
                args=[],
                kwargs={},
                queue=queue,
                countdown=COUNTDOWN,
            )
            await wait_for(event, timeout=COUNTDOWN + CONSUME_TIMEOUT)

        assert ran_at[0] - sent_at >= COUNTDOWN

    @pytest.mark.asyncio()
    async def test_countdown_delays_execution_by_a_celery_worker(
        self,
        broker: CeleryBroker,
        queue: str,
        celery_worker: Path,
    ) -> None:
        """A `countdown` we publish is honoured by the Celery worker."""
        async with running(broker):
            started = time.monotonic()
            await broker.publish(
                CeleryTask("tests.echo", args=["late"], countdown=COUNTDOWN),
                queue=queue,
            )

        entries = wait_for_log(celery_worker, timeout=COUNTDOWN + WORKER_TIMEOUT)
        elapsed = time.monotonic() - started

        assert entries == [{"task": "tests.echo", "payload": "late"}]
        assert elapsed >= COUNTDOWN

    @pytest.mark.asyncio()
    async def test_expired_task_is_not_executed(
        self,
        broker: CeleryBroker,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """An already-expired task is dropped instead of run.

        Published by us rather than by a Celery client: Celery turns a past
        `expires` into a broker-level TTL, which can drop the message before
        our subscriber gets to decide anything about it.
        """
        seen: list[str] = []

        @broker.subscriber(queue)
        async def handler(args: list[str], kwargs: dict[str, Any]) -> None:
            seen.append(args[0])
            event.set()

        async with running(broker):
            await broker.publish(
                CeleryTask("tests.echo", args=["expired"], expires=-1),
                queue=queue,
            )
            # A second task behind it tells us the first one has been handled.
            await broker.publish(CeleryTask("tests.echo", args=["marker"]), queue=queue)
            await wait_for(event)

        assert seen == ["marker"]

    @pytest.mark.asyncio()
    async def test_protocol_v1_message_is_consumed(
        self,
        broker: CeleryBroker,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """A hand-assembled protocol v1 message is parsed and executed."""
        received: dict[str, Any] = {}

        @broker.subscriber(queue, task="tests.add")
        async def handler(args: list[int], kwargs: dict[str, Any]) -> None:
            received["args"] = args
            event.set()

        async with running(broker):
            # The flat v1 body, published with no protocol v2 headers at all.
            await broker.publish(
                {
                    "task": "tests.add",
                    "id": "v1-task-id",
                    "args": [4, 5],
                    "kwargs": {},
                    "retries": 0,
                    "eta": None,
                    "expires": None,
                    "utc": True,
                },
                queue=queue,
            )
            await wait_for(event)

        assert received["args"] == [4, 5]

    @pytest.mark.asyncio()
    async def test_broker_pings_a_live_connection(self, broker: CeleryBroker) -> None:
        async with running(broker):
            assert await broker.ping(timeout=5.0)

    @pytest.mark.asyncio()
    async def test_ping_is_false_before_connecting(self, broker: CeleryBroker) -> None:
        assert not await broker.ping(timeout=5.0)

    @pytest.mark.asyncio()
    async def test_concurrent_subscriber_drains_the_queue(
        self,
        broker: CeleryBroker,
        celery_app: Celery,
        queue: str,
    ) -> None:
        """`max_workers` processes several tasks without losing any."""
        total = 5
        seen: list[int] = []
        done = asyncio.Event()

        @broker.subscriber(queue, task="tests.add", max_workers=3)
        async def handler(args: list[int], kwargs: dict[str, Any]) -> None:
            seen.append(args[0])
            if len(seen) == total:
                done.set()

        async with running(broker):
            for index in range(total):
                celery_app.send_task(
                    "tests.add",
                    args=[index, 0],
                    kwargs={},
                    queue=queue,
                )
            await wait_for(done)

        assert sorted(seen) == list(range(total))

    @pytest.mark.asyncio()
    async def test_a_chain_runs_through_us_to_a_celery_worker(
        self,
        broker: CeleryBroker,
        queue: str,
        our_queue: str,
        celery_worker: Path,
        event: asyncio.Event,
    ) -> None:
        """A → B → C, where B is ours and C runs on a real Celery worker.

        C receives B's return value as its first argument, which is what
        makes the chain a chain.
        """

        @broker.subscriber(our_queue, task="tests.middle")
        async def middle(args: list[int], kwargs: dict[str, Any]) -> int:
            event.set()
            return args[0] * 10

        async with running(broker):
            await broker.publish(
                CeleryTask(
                    "tests.middle",
                    args=[4],
                    chain=[signature("tests.echo", options={"queue": queue})],
                ),
                queue=our_queue,
            )
            await wait_for(event)

        assert wait_for_log(celery_worker) == [
            {"task": "tests.echo", "payload": 40},
        ]

    @pytest.mark.asyncio()
    async def test_a_callback_runs_on_a_celery_worker(
        self,
        broker: CeleryBroker,
        queue: str,
        our_queue: str,
        celery_worker: Path,
        event: asyncio.Event,
    ) -> None:
        """`link=` on a task we publish fires once our handler succeeds."""

        @broker.subscriber(our_queue, task="tests.middle")
        async def middle() -> str:
            event.set()
            return "done"

        async with running(broker):
            await broker.publish(
                CeleryTask(
                    "tests.middle",
                    link=[signature("tests.echo", options={"queue": queue})],
                ),
                queue=our_queue,
            )
            await wait_for(event)

        assert wait_for_log(celery_worker) == [
            {"task": "tests.echo", "payload": "done"},
        ]

    @pytest.mark.asyncio()
    async def test_an_errback_runs_on_a_celery_worker(
        self,
        broker: CeleryBroker,
        queue: str,
        our_queue: str,
        celery_worker: Path,
        event: asyncio.Event,
    ) -> None:
        """An errback receives the failed task id, as Celery calls it."""

        @broker.subscriber(our_queue, task="tests.middle")
        async def middle() -> None:
            event.set()
            msg = "boom"
            raise ValueError(msg)

        async with running(broker):
            await broker.publish(
                CeleryTask(
                    "tests.middle",
                    link_error=[signature("tests.echo", options={"queue": queue})],
                ),
                queue=our_queue,
                correlation_id="failing-task-id",
            )
            await wait_for(event)

        assert wait_for_log(celery_worker) == [
            {"task": "tests.echo", "payload": "failing-task-id"},
        ]

    @pytest.mark.asyncio()
    async def test_an_immutable_chain_step_keeps_its_own_arguments(
        self,
        broker: CeleryBroker,
        queue: str,
        our_queue: str,
        celery_worker: Path,
        event: asyncio.Event,
    ) -> None:
        """`.si()` in Celery: the previous result is not passed on."""

        @broker.subscriber(our_queue, task="tests.middle")
        async def middle() -> str:
            event.set()
            return "ignored"

        async with running(broker):
            await broker.publish(
                CeleryTask(
                    "tests.middle",
                    chain=[
                        signature(
                            "tests.echo",
                            args=["own"],
                            options={"queue": queue},
                            immutable=True,
                        ),
                    ],
                ),
                queue=our_queue,
            )
            await wait_for(event)

        assert wait_for_log(celery_worker) == [
            {"task": "tests.echo", "payload": "own"},
        ]
