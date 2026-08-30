from typing import Any
from unittest.mock import MagicMock

import pytest

from faststream_celery.message import ConsumerMessage
from faststream_celery.subscriber.consumer import ConsumerRegistry, SharedConsumer
from tests.helpers import consumer_message, recorded, task_message


class FakeSubscriber:
    """The slice of a subscriber the shared consumer routes to."""

    def __init__(self, task: str | None) -> None:
        self.task = task
        self.received: list[ConsumerMessage] = []

    async def dispatch(self, msg: ConsumerMessage) -> None:
        self.received.append(msg)


@pytest.fixture()
def consumer() -> SharedConsumer:
    return SharedConsumer(MagicMock(), MagicMock())


@pytest.fixture()
def registry() -> ConsumerRegistry:
    return ConsumerRegistry()


def acquire_args(queue: str, prefetch: int = 1) -> dict[str, Any]:
    return {
        "queue": queue,
        "connection_factory": MagicMock(),
        "accept": ["json"],
        "prefetch_count": prefetch,
        "logger": MagicMock(),
    }


class TestRouting:
    @pytest.mark.asyncio()
    async def test_message_goes_to_the_matching_subscriber(
        self,
        consumer: SharedConsumer,
    ) -> None:
        add = FakeSubscriber("proj.tasks.add")
        mul = FakeSubscriber("proj.tasks.mul")
        consumer.register(add)
        consumer.register(mul)

        message = task_message("proj.tasks.mul")
        await consumer._route(message)

        assert add.received == []
        assert mul.received == [message]

    @pytest.mark.asyncio()
    async def test_unclaimed_task_is_rejected(self, consumer: SharedConsumer) -> None:
        add = FakeSubscriber("proj.tasks.add")
        consumer.register(add)

        message = task_message("proj.tasks.other")
        await consumer._route(message)

        assert add.received == []
        assert recorded(message).rejects == [False]

    @pytest.mark.asyncio()
    async def test_catch_all_takes_what_nobody_claims(
        self,
        consumer: SharedConsumer,
    ) -> None:
        add = FakeSubscriber("proj.tasks.add")
        catch_all = FakeSubscriber(None)
        consumer.register(add)
        consumer.register(catch_all)

        message = task_message("proj.tasks.other")
        await consumer._route(message)

        assert add.received == []
        assert catch_all.received == [message]
        assert recorded(message).rejects == []

    @pytest.mark.asyncio()
    async def test_named_subscriber_wins_over_a_catch_all(
        self,
        consumer: SharedConsumer,
    ) -> None:
        catch_all = FakeSubscriber(None)
        add = FakeSubscriber("proj.tasks.add")
        consumer.register(catch_all)
        consumer.register(add)

        message = task_message("proj.tasks.add")
        await consumer._route(message)

        assert add.received == [message]
        assert catch_all.received == []

    @pytest.mark.asyncio()
    async def test_non_task_message_goes_to_a_catch_all(
        self,
        consumer: SharedConsumer,
    ) -> None:
        catch_all = FakeSubscriber(None)
        consumer.register(catch_all)

        message = consumer_message({"status": "SUCCESS", "result": 1})
        await consumer._route(message)

        assert catch_all.received == [message]

    @pytest.mark.asyncio()
    async def test_non_task_message_is_rejected_without_a_catch_all(
        self,
        consumer: SharedConsumer,
    ) -> None:
        consumer.register(FakeSubscriber("proj.tasks.add"))

        message = consumer_message({"status": "SUCCESS", "result": 1})
        await consumer._route(message)

        assert recorded(message).rejects == [False]

    @pytest.mark.asyncio()
    async def test_an_undecodable_body_still_routes(
        self,
        consumer: SharedConsumer,
    ) -> None:
        """Routing must not raise on a body it cannot read."""
        catch_all = FakeSubscriber(None)
        consumer.register(catch_all)

        message = consumer_message(
            {"whatever": 1},
            content_type="application/x-unknown",
        )
        await consumer._route(message)

        assert catch_all.received == [message]

    @pytest.mark.asyncio()
    async def test_nothing_registered_rejects_the_message(
        self,
        consumer: SharedConsumer,
    ) -> None:
        message = task_message()
        await consumer._route(message)

        assert recorded(message).rejects == [False]

    def test_registering_twice_does_not_duplicate(
        self,
        consumer: SharedConsumer,
    ) -> None:
        subscriber = FakeSubscriber("proj.tasks.add")

        consumer.register(subscriber)
        consumer.register(subscriber)

        assert consumer._subscribers == [subscriber]


class TestRegistry:
    def test_one_consumer_per_queue(self, registry: ConsumerRegistry) -> None:
        first = registry.acquire(**acquire_args("celery"))
        second = registry.acquire(**acquire_args("celery"))
        other = registry.acquire(**acquire_args("other"))

        assert first is second
        assert other is not first

    def test_widest_prefetch_window_wins(self, registry: ConsumerRegistry) -> None:
        consumer = registry.acquire(**acquire_args("celery", prefetch=2))
        registry.acquire(**acquire_args("celery", prefetch=10))
        registry.acquire(**acquire_args("celery", prefetch=5))

        assert consumer.bridge._prefetch_count == 10

    @pytest.mark.asyncio()
    async def test_consumer_closes_when_the_last_subscriber_leaves(
        self,
        registry: ConsumerRegistry,
    ) -> None:
        consumer = registry.acquire(**acquire_args("celery"))

        first = FakeSubscriber("proj.tasks.add")
        second = FakeSubscriber("proj.tasks.mul")
        consumer.register(first)
        consumer.register(second)

        await registry.release("celery", first)
        assert registry.acquire(**acquire_args("celery")) is consumer

        await registry.release("celery", second)
        assert registry.acquire(**acquire_args("celery")) is not consumer

    @pytest.mark.asyncio()
    async def test_releasing_an_unknown_queue_is_a_no_op(
        self,
        registry: ConsumerRegistry,
    ) -> None:
        await registry.release("never-acquired", FakeSubscriber(None))

    @pytest.mark.asyncio()
    async def test_releasing_an_unregistered_subscriber_still_closes(
        self,
        registry: ConsumerRegistry,
    ) -> None:
        """A subscriber that never registered — a `get_one` one — still releases."""
        consumer = registry.acquire(**acquire_args("celery"))

        await registry.release("celery", FakeSubscriber(None))

        assert registry.acquire(**acquire_args("celery")) is not consumer
