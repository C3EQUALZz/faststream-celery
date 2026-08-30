"""The subscriber against a real kombu consumer, over `memory://`.

Everything the in-memory `TestCeleryBroker` fake skips: consumer threads,
acknowledgements, the shared consumer, and start/stop.
"""

import asyncio
from typing import TYPE_CHECKING, Any, cast

import pytest
from faststream.exceptions import IncorrectState

from faststream_celery import CeleryBroker, CeleryRouter, CeleryTask
from faststream_celery.subscriber import CeleryConcurrentSubscriber
from tests.helpers import MEMORY_URL, running

if TYPE_CHECKING:
    from collections.abc import Callable

    from faststream_celery.message import CeleryMessage
    from faststream_celery.subscriber.usecase import CelerySubscriber

CONSUME_TIMEOUT = 5.0


async def wait_for(event: asyncio.Event, timeout: float = CONSUME_TIMEOUT) -> None:
    await asyncio.wait_for(event.wait(), timeout=timeout)


class TestRoundTrip:
    @pytest.mark.asyncio()
    async def test_a_published_task_reaches_its_handler(
        self,
        memory_broker: CeleryBroker,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        received: dict[str, Any] = {}

        @memory_broker.subscriber(queue, task="proj.tasks.add")
        async def handler(args: list[int], kwargs: dict[str, Any]) -> None:
            received["args"] = args
            received["kwargs"] = kwargs
            event.set()

        async with running(memory_broker):
            await memory_broker.publish(
                CeleryTask("proj.tasks.add", args=[1, 2], kwargs={"debug": True}),
                queue=queue,
            )
            await wait_for(event)

        assert received == {"args": [1, 2], "kwargs": {"debug": True}}

    @pytest.mark.asyncio()
    async def test_a_raw_payload_reaches_its_handler(
        self,
        memory_broker: CeleryBroker,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        received: list[Any] = []

        @memory_broker.subscriber(queue)
        async def handler(body: dict[str, int]) -> None:
            received.append(body)
            event.set()

        async with running(memory_broker):
            await memory_broker.publish({"payload": 1}, queue=queue)
            await wait_for(event)

        assert received == [{"payload": 1}]

    @pytest.mark.asyncio()
    async def test_a_router_prefix_reaches_the_wire(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """The subscriber and the publisher must agree on the prefixed name."""
        router = CeleryRouter(prefix="pre-")

        @router.subscriber(queue, task="proj.tasks.add")
        async def handler() -> None:
            event.set()

        broker = CeleryBroker(MEMORY_URL)
        broker.include_router(router)

        async with running(broker):
            await broker.publish(CeleryTask("proj.tasks.add"), queue=f"pre-{queue}")
            await wait_for(event)


class TestSharedConsumer:
    @pytest.mark.asyncio()
    async def test_two_subscribers_share_one_consumer(
        self,
        memory_broker: CeleryBroker,
        queue: str,
    ) -> None:
        seen: list[str] = []
        both = asyncio.Event()

        def note(name: str) -> None:
            seen.append(name)
            if len(seen) == 2:
                both.set()

        @memory_broker.subscriber(queue, task="proj.tasks.add")
        async def add_handler() -> None:
            note("add")

        @memory_broker.subscriber(queue, task="proj.tasks.mul")
        async def mul_handler() -> None:
            note("mul")

        async with running(memory_broker):
            registry = memory_broker.config.broker_config.consumers
            assert len(registry._consumers) == 1

            await memory_broker.publish(CeleryTask("proj.tasks.add"), queue=queue)
            await memory_broker.publish(CeleryTask("proj.tasks.mul"), queue=queue)
            await wait_for(both)

        assert sorted(seen) == ["add", "mul"]

    @pytest.mark.asyncio()
    async def test_separate_queues_get_separate_consumers(
        self,
        memory_broker: CeleryBroker,
        queue: str,
    ) -> None:
        memory_broker.subscriber(queue)(_noop)
        memory_broker.subscriber(f"{queue}-other")(_noop)

        async with running(memory_broker):
            assert len(memory_broker.config.broker_config.consumers._consumers) == 2

    @pytest.mark.asyncio()
    async def test_consumers_are_released_on_stop(
        self,
        memory_broker: CeleryBroker,
        queue: str,
    ) -> None:
        memory_broker.subscriber(queue)(_noop)

        async with running(memory_broker):
            pass

        assert memory_broker.config.broker_config.consumers._consumers == {}

    @pytest.mark.asyncio()
    async def test_an_unclaimed_task_does_not_block_the_queue(
        self,
        memory_broker: CeleryBroker,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """With prefetch 1, an unsettled message would stall everything after it."""

        @memory_broker.subscriber(queue, task="proj.tasks.add", prefetch_count=1)
        async def handler() -> None:
            event.set()

        async with running(memory_broker):
            await memory_broker.publish(CeleryTask("proj.tasks.nobody"), queue=queue)
            await memory_broker.publish(CeleryTask("proj.tasks.add"), queue=queue)
            await wait_for(event)


class TestConcurrency:
    @pytest.mark.asyncio()
    async def test_max_workers_processes_every_message(
        self,
        memory_broker: CeleryBroker,
        queue: str,
    ) -> None:
        total = 5
        seen: list[int] = []
        done = asyncio.Event()

        @memory_broker.subscriber(queue, task="proj.tasks.add", max_workers=3)
        async def handler(args: list[int], kwargs: dict[str, Any]) -> None:
            seen.append(args[0])
            if len(seen) == total:
                done.set()

        async with running(memory_broker):
            for index in range(total):
                await memory_broker.publish(
                    CeleryTask("proj.tasks.add", args=[index]),
                    queue=queue,
                )
            await wait_for(done)

        assert sorted(seen) == list(range(total))

    def test_max_workers_selects_the_concurrent_subscriber(
        self,
        broker: CeleryBroker,
    ) -> None:
        assert isinstance(
            broker.subscriber("celery", max_workers=4),
            CeleryConcurrentSubscriber,
        )


class TestIteration:
    @pytest.mark.asyncio()
    async def test_get_one_returns_a_published_task(
        self,
        memory_broker: CeleryBroker,
        queue: str,
    ) -> None:
        subscriber = memory_broker.subscriber(queue)

        async with running(memory_broker):
            await memory_broker.publish(
                CeleryTask("proj.tasks.add", args=[1, 2]),
                queue=queue,
            )

            message = await subscriber.get_one(timeout=CONSUME_TIMEOUT)

            assert message is not None
            assert message.headers["task"] == "proj.tasks.add"
            assert await message.decode() == {"args": [1, 2], "kwargs": {}}

    @pytest.mark.asyncio()
    async def test_get_one_times_out_to_none(
        self,
        memory_broker: CeleryBroker,
        queue: str,
    ) -> None:
        subscriber = memory_broker.subscriber(queue)

        async with running(memory_broker):
            assert await subscriber.get_one(timeout=0.1) is None

    @pytest.mark.asyncio()
    async def test_get_one_before_start_is_refused(
        self,
        memory_broker: CeleryBroker,
        queue: str,
    ) -> None:
        subscriber = memory_broker.subscriber(queue)

        with pytest.raises(IncorrectState, match="start subscriber"):
            await subscriber.get_one()

    @pytest.mark.asyncio()
    async def test_get_one_with_a_handler_is_refused(
        self,
        memory_broker: CeleryBroker,
        queue: str,
    ) -> None:
        subscriber = memory_broker.subscriber(queue)
        subscriber(_noop)

        async with running(memory_broker):
            with pytest.raises(IncorrectState, match="registered handlers"):
                await subscriber.get_one()

    @pytest.mark.asyncio()
    async def test_iteration_yields_published_tasks(
        self,
        memory_broker: CeleryBroker,
        queue: str,
    ) -> None:
        subscriber = memory_broker.subscriber(queue)

        async with running(memory_broker):
            await memory_broker.publish(CeleryTask("proj.tasks.add"), queue=queue)

            async for message in subscriber:
                assert message.headers["task"] == "proj.tasks.add"
                break

    @pytest.mark.asyncio()
    async def test_iteration_before_start_is_refused(
        self,
        memory_broker: CeleryBroker,
        queue: str,
    ) -> None:
        subscriber = memory_broker.subscriber(queue)

        with pytest.raises(IncorrectState, match="start subscriber"):
            await _first(subscriber)

    @pytest.mark.asyncio()
    async def test_iteration_with_a_handler_is_refused(
        self,
        memory_broker: CeleryBroker,
        queue: str,
    ) -> None:
        subscriber = memory_broker.subscriber(queue)
        subscriber(_noop)

        async with running(memory_broker):
            with pytest.raises(IncorrectState, match="registered handlers"):
                await _first(subscriber)


class TestLifecycle:
    @pytest.mark.asyncio()
    async def test_a_broker_can_be_restarted(
        self,
        memory_broker: CeleryBroker,
        queue: str,
    ) -> None:
        seen: list[str] = []
        first = asyncio.Event()
        second = asyncio.Event()

        @memory_broker.subscriber(queue, task="proj.tasks.add")
        async def handler() -> None:
            seen.append("run")
            (first if len(seen) == 1 else second).set()

        async with running(memory_broker):
            await memory_broker.publish(CeleryTask("proj.tasks.add"), queue=queue)
            await wait_for(first)

        async with running(memory_broker):
            await memory_broker.publish(CeleryTask("proj.tasks.add"), queue=queue)
            await wait_for(second)

        assert seen == ["run", "run"]

    @pytest.mark.asyncio()
    async def test_stopping_twice_is_safe(
        self,
        memory_broker: CeleryBroker,
        queue: str,
    ) -> None:
        memory_broker.subscriber(queue)(_noop)

        async with running(memory_broker):
            pass

        await memory_broker.stop()

    @pytest.mark.asyncio()
    async def test_a_deferred_task_is_dropped_on_stop(
        self,
        memory_broker: CeleryBroker,
        queue: str,
    ) -> None:
        """It was never acked, so the broker will redeliver it."""
        ran: list[str] = []

        @memory_broker.subscriber(queue, task="proj.tasks.add")
        async def handler() -> None:
            ran.append("run")

        async with running(memory_broker):
            await memory_broker.publish(
                CeleryTask("proj.tasks.add", countdown=30),
                queue=queue,
            )

            subscriber = cast("CelerySubscriber", next(iter(memory_broker.subscribers)))
            await _until(lambda: subscriber._scheduler.pending == 1)

        assert ran == []


async def _noop() -> None: ...


async def _first(subscriber: "CelerySubscriber") -> "CeleryMessage":
    """Pull one message out of an async iterator."""
    async for message in subscriber:
        return message

    msg = "the subscriber yielded nothing"
    raise AssertionError(msg)


async def _until(
    predicate: "Callable[[], bool]",
    timeout: float = CONSUME_TIMEOUT,
) -> None:
    """Wait for a state the broker reaches on its own consumer thread."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)

    msg = f"condition not reached within {timeout}s"
    raise AssertionError(msg)
