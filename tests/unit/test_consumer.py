import json
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pytest
from kombu import Message

from faststream_celery.message import ConsumerMessage
from faststream_celery.subscriber.consumer import ConsumerRegistry, SharedConsumer
from faststream_celery.task import CeleryTask, build_task_envelope


async def _run_inline(action: Callable[[], None]) -> None:
    action()


def _message(task: str | None) -> ConsumerMessage:
    if task is None:
        raw = Message(
            body=json.dumps({"status": "SUCCESS", "result": 1}).encode(),
            content_type="application/json",
            content_encoding="utf-8",
            headers={},
            properties={},
        )
    else:
        envelope = build_task_envelope(CeleryTask(task), task_id="task-id-1")
        raw = Message(
            body=json.dumps(envelope.body).encode(),
            content_type="application/json",
            content_encoding="utf-8",
            headers=dict(envelope.headers),
            properties={},
        )

    raw.reject = MagicMock()
    return ConsumerMessage(raw, _run_inline)


class FakeSubscriber:
    def __init__(self, task: str | None) -> None:
        self.task = task
        self.received: list[ConsumerMessage] = []

    async def dispatch(self, msg: ConsumerMessage) -> None:
        self.received.append(msg)


def _consumer() -> SharedConsumer:
    return SharedConsumer(MagicMock(), MagicMock())


@pytest.mark.asyncio()
async def test_message_goes_to_the_matching_subscriber() -> None:
    consumer = _consumer()
    add = FakeSubscriber("proj.tasks.add")
    mul = FakeSubscriber("proj.tasks.mul")
    consumer.register(add)
    consumer.register(mul)

    message = _message("proj.tasks.mul")
    await consumer._route(message)

    assert add.received == []
    assert mul.received == [message]


@pytest.mark.asyncio()
async def test_unclaimed_task_is_rejected() -> None:
    consumer = _consumer()
    add = FakeSubscriber("proj.tasks.add")
    consumer.register(add)

    message = _message("proj.tasks.other")
    await consumer._route(message)

    assert add.received == []
    message.message.reject.assert_called_once_with(requeue=False)


@pytest.mark.asyncio()
async def test_catch_all_takes_what_nobody_claims() -> None:
    consumer = _consumer()
    add = FakeSubscriber("proj.tasks.add")
    catch_all = FakeSubscriber(None)
    consumer.register(add)
    consumer.register(catch_all)

    message = _message("proj.tasks.other")
    await consumer._route(message)

    assert add.received == []
    assert catch_all.received == [message]
    message.message.reject.assert_not_called()


@pytest.mark.asyncio()
async def test_named_subscriber_wins_over_a_catch_all() -> None:
    consumer = _consumer()
    catch_all = FakeSubscriber(None)
    add = FakeSubscriber("proj.tasks.add")
    consumer.register(catch_all)
    consumer.register(add)

    message = _message("proj.tasks.add")
    await consumer._route(message)

    assert add.received == [message]
    assert catch_all.received == []


@pytest.mark.asyncio()
async def test_non_task_message_goes_to_a_catch_all() -> None:
    consumer = _consumer()
    catch_all = FakeSubscriber(None)
    consumer.register(catch_all)

    message = _message(None)
    await consumer._route(message)

    assert catch_all.received == [message]


def test_registering_twice_does_not_duplicate() -> None:
    consumer = _consumer()
    subscriber = FakeSubscriber("proj.tasks.add")

    consumer.register(subscriber)
    consumer.register(subscriber)

    assert consumer._subscribers == [subscriber]


def _registry_args(queue: str, prefetch: int) -> dict[str, Any]:
    return {
        "queue": queue,
        "connection_factory": MagicMock(),
        "accept": ["json"],
        "prefetch_count": prefetch,
        "logger": MagicMock(),
    }


def test_one_consumer_per_queue() -> None:
    registry = ConsumerRegistry()

    first = registry.acquire(**_registry_args("celery", 1))
    second = registry.acquire(**_registry_args("celery", 1))
    other = registry.acquire(**_registry_args("other", 1))

    assert first is second
    assert other is not first


def test_widest_prefetch_window_wins() -> None:
    registry = ConsumerRegistry()

    consumer = registry.acquire(**_registry_args("celery", 2))
    registry.acquire(**_registry_args("celery", 10))
    registry.acquire(**_registry_args("celery", 5))

    assert consumer.bridge._prefetch_count == 10


@pytest.mark.asyncio()
async def test_consumer_closes_when_the_last_subscriber_leaves() -> None:
    registry = ConsumerRegistry()
    consumer = registry.acquire(**_registry_args("celery", 1))

    first = FakeSubscriber("proj.tasks.add")
    second = FakeSubscriber("proj.tasks.mul")
    consumer.register(first)
    consumer.register(second)

    await registry.release("celery", first)
    assert registry.acquire(**_registry_args("celery", 1)) is consumer

    await registry.release("celery", second)
    assert registry.acquire(**_registry_args("celery", 1)) is not consumer
