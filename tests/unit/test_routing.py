"""A queue delivers a message to one exact task subscriber or its fallback."""

from typing import Any

import pytest
from faststream.exceptions import SubscriberNotFound

from faststream_celery import CeleryBroker, CeleryRouter, CeleryTask, TestCeleryBroker
from faststream_celery.message import CeleryMessage


@pytest.mark.asyncio()
@pytest.mark.parametrize("fallback_first", (False, True))
@pytest.mark.parametrize("operation", ("publish", "request"))
@pytest.mark.parametrize("protocol", ("v2", "v1", "unknown", "raw"))
async def test_exact_task_takes_precedence_over_fallback(
    queue: str,
    fallback_first: bool,
    operation: str,
    protocol: str,
) -> None:
    broker = CeleryBroker()
    router = CeleryRouter()
    exact_scope = router if fallback_first else broker
    fallback_scope = broker if fallback_first else router

    @exact_scope.subscriber(queue, task="tests.exact")
    async def exact() -> str:
        return "exact"

    @fallback_scope.subscriber(queue)
    async def fallback() -> str:
        return "fallback"

    broker.include_router(router)
    payload: Any
    if protocol == "v2":
        payload = CeleryTask("tests.exact")
    elif protocol == "v1":
        payload = {"task": "tests.exact", "id": "routing-id", "args": [], "kwargs": {}}
    elif protocol == "unknown":
        payload = CeleryTask("tests.other")
    else:
        payload = {"value": 1}
    expected = "exact" if protocol in {"v2", "v1"} else "fallback"

    async with TestCeleryBroker(broker):
        if operation == "request":
            response = await broker.request(
                payload,
                queue=queue,
                correlation_id="routing-id",
                timeout=1,
            )
            envelope = await response.decode()
            assert isinstance(envelope, dict)
            assert envelope["result"] == expected
        else:
            await broker.publish(payload, queue=queue, correlation_id="routing-id")
        if expected == "exact":
            exact.mock.assert_called_once()
            fallback.mock.assert_not_called()
        else:
            exact.mock.assert_not_called()
            fallback.mock.assert_called_once()


@pytest.mark.asyncio()
async def test_competing_workers_do_not_receive_a_broadcast(queue: str) -> None:
    client = CeleryBroker()
    first = CeleryBroker()
    second = CeleryBroker()

    @first.subscriber(queue, task="tests.exact")
    async def first_handler() -> None: ...

    @second.subscriber(queue, task="tests.exact")
    async def second_handler() -> None: ...

    async with TestCeleryBroker(client, first, second):
        await client.publish(CeleryTask("tests.exact"), queue=queue)
        assert first_handler.mock.call_count + second_handler.mock.call_count == 1


@pytest.mark.asyncio()
async def test_a_pull_endpoint_does_not_claim_handler_deliveries(queue: str) -> None:
    broker = CeleryBroker()
    broker.subscriber(queue, task="tests.exact")

    @broker.subscriber(queue)
    async def fallback() -> str:
        return "fallback"

    async with TestCeleryBroker(broker):
        await broker.publish(CeleryTask("tests.exact"), queue=queue)
        fallback.mock.assert_called_once()


def decline(message: CeleryMessage) -> bool:
    return False


@pytest.mark.asyncio()
@pytest.mark.parametrize("fallback_first", (False, True))
@pytest.mark.parametrize("task", ("tests.exact", "tests.other"))
async def test_publisher_mock_records_the_selected_handler(
    queue: str,
    fallback_first: bool,
    task: str,
) -> None:
    broker = CeleryBroker()
    router = CeleryRouter()
    exact_scope = router if fallback_first else broker
    fallback_scope = broker if fallback_first else router

    @exact_scope.subscriber(queue, task="tests.exact")
    async def exact() -> None: ...

    @fallback_scope.subscriber(queue)
    async def fallback() -> None: ...

    broker.include_router(router)
    publisher = broker.publisher(queue)
    async with TestCeleryBroker(broker):
        await publisher.publish(CeleryTask(task))
        publisher.mock.assert_called_once_with({"args": [], "kwargs": {}})
        if task == "tests.exact":
            exact.mock.assert_called_once()
            fallback.mock.assert_not_called()
        else:
            exact.mock.assert_not_called()
            fallback.mock.assert_called_once()


@pytest.mark.asyncio()
@pytest.mark.parametrize("operation", ("publish", "request"))
async def test_an_exact_subscriber_filter_does_not_retry_the_fallback(
    queue: str,
    operation: str,
) -> None:
    broker = CeleryBroker()
    subscriber = broker.subscriber(queue, task="tests.exact")

    @subscriber(filter=decline)
    async def exact() -> None: ...

    @broker.subscriber(queue)
    async def fallback() -> None: ...

    async with TestCeleryBroker(broker):
        if operation == "request":
            with pytest.raises(TimeoutError):
                await broker.request(CeleryTask("tests.exact"), queue=queue, timeout=0.02)
        else:
            with pytest.raises(SubscriberNotFound):
                await broker.publish(CeleryTask("tests.exact"), queue=queue)
        exact.mock.assert_not_called()
        fallback.mock.assert_not_called()
