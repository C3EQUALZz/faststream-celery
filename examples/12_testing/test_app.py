"""Testing Celery handlers without a broker, without Celery, without Docker.

`TestCeleryBroker` swaps the producer for one that routes a publish straight
into the matching subscribers, so middlewares, the parser, Pydantic validation
and dependency injection all run — in memory, in the same process.

Every handler gets a `.mock` recording what it was called with, and a publisher
gets one recording what it published. Both are reset when the context manager
exits, so assert inside the block.

Run:
    pytest test_app.py
"""

from typing import Any

import pytest
from faststream.exceptions import SubscriberNotFound
from pydantic import ValidationError

from faststream_celery import CeleryTask, TestCeleryBroker
from faststream_celery.annotations import CeleryMessage

from app import AUDIT_QUEUE, QUEUE, audit, broker, charge, fail


@pytest.mark.asyncio()
async def test_a_task_reaches_its_handler() -> None:
    async with TestCeleryBroker(broker):
        await broker.publish(
            CeleryTask("examples.charge", kwargs={"order_id": 1, "amount_cents": 500}),
            queue=QUEUE,
        )

        # The normalized body: what every Celery task looks like to a handler.
        charge.mock.assert_called_once_with(
            {"args": [], "kwargs": {"order_id": 1, "amount_cents": 500}},
        )


@pytest.mark.asyncio()
async def test_the_handler_result_comes_back() -> None:
    async with TestCeleryBroker(broker):
        response = await broker.request(
            CeleryTask("examples.charge", kwargs={"order_id": 7, "amount_cents": 999}),
            queue=QUEUE,
        )

        assert await response.decode() == {
            "status": "captured",
            "order_id": 7,
            "amount_cents": 999,
        }


@pytest.mark.asyncio()
async def test_the_handler_publishes_an_audit_task() -> None:
    async with TestCeleryBroker(broker):
        await broker.publish(
            CeleryTask("examples.charge", kwargs={"order_id": 1, "amount_cents": 500}),
            queue=QUEUE,
        )

        assert audit.queue == AUDIT_QUEUE
        audit.mock.assert_called_once_with(
            {"args": [], "kwargs": {"order_id": 1, "amount_cents": 500}},
        )


@pytest.mark.asyncio()
async def test_an_invalid_payload_fails_the_task() -> None:
    """Validation runs before the handler body, and the error propagates.

    `charge.mock` *is* called here: it records the message that was delivered
    to the subscriber, not a successful call of the handler body. Assert on
    the exception for that, and on the mock for routing.
    """
    async with TestCeleryBroker(broker):
        with pytest.raises(ValidationError):
            await broker.publish(
                CeleryTask("examples.charge", kwargs={"order_id": -1}),
                queue=QUEUE,
            )

        audit.mock.assert_not_called()


@pytest.mark.asyncio()
async def test_a_failing_handler_raises_through() -> None:
    async with TestCeleryBroker(broker):
        with pytest.raises(RuntimeError, match="nope"):
            await broker.publish(
                CeleryTask("examples.fail", kwargs={"message": "nope"}),
                queue=QUEUE,
            )

        fail.mock.assert_called_once()


@pytest.mark.asyncio()
async def test_a_task_nobody_claims_finds_no_subscriber() -> None:
    async with TestCeleryBroker(broker):
        with pytest.raises(SubscriberNotFound):
            await broker.publish(CeleryTask("examples.unknown"), queue=QUEUE)


@pytest.mark.asyncio()
async def test_the_celery_metadata_reaches_the_handler() -> None:
    """A handler reading `CeleryMessage` can be tested the same way.

    `persistent=False` keeps this test's subscriber out of the app: it is
    dropped when the block exits, instead of staying registered on the shared
    broker for the tests that follow.
    """
    seen: list[dict[str, Any]] = []

    @broker.subscriber(QUEUE, task="examples.metadata", persistent=False)
    async def handler(message: CeleryMessage) -> None:
        seen.append(dict(message.headers))

    async with TestCeleryBroker(broker):
        await broker.publish(
            CeleryTask("examples.metadata"),
            queue=QUEUE,
            correlation_id="task-id-1",
        )

    (headers,) = seen
    assert headers["task"] == "examples.metadata"
    assert headers["id"] == "task-id-1"
    assert headers["root_id"] == "task-id-1"
