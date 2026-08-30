from typing import Any

import pytest
from faststream import Context
from faststream.exceptions import SubscriberNotFound
from faststream.middlewares import BaseMiddleware
from typing_extensions import override

from faststream_celery import CeleryBroker, CeleryRouter, CeleryTask, TestCeleryBroker
from faststream_celery.message import CeleryMessage


@pytest.mark.asyncio()
async def test_publish_reaches_handler(queue: str) -> None:
    broker = CeleryBroker()

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler(args: list[int], kwargs: dict[str, Any]) -> None: ...

    async with TestCeleryBroker(broker):
        await broker.publish(
            CeleryTask("proj.tasks.add", args=[1, 2], kwargs={"debug": True}),
            queue=queue,
        )

        handler.mock.assert_called_once_with(
            {"args": [1, 2], "kwargs": {"debug": True}},
        )


@pytest.mark.asyncio()
async def test_task_filter_is_honored(queue: str) -> None:
    broker = CeleryBroker()

    @broker.subscriber(queue, task="proj.tasks.add")
    async def add() -> None: ...

    @broker.subscriber(queue, task="proj.tasks.mul")
    async def mul() -> None: ...

    async with TestCeleryBroker(broker):
        await broker.publish(CeleryTask("proj.tasks.mul"), queue=queue)

        add.mock.assert_not_called()
        mul.mock.assert_called_once()


@pytest.mark.asyncio()
async def test_unknown_task_finds_no_subscriber(queue: str) -> None:
    broker = CeleryBroker()

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler() -> None: ...

    async with TestCeleryBroker(broker):
        with pytest.raises(SubscriberNotFound):
            await broker.publish(CeleryTask("proj.tasks.other"), queue=queue)

        handler.mock.assert_not_called()


@pytest.mark.asyncio()
async def test_subscriber_without_task_accepts_any_task(queue: str) -> None:
    broker = CeleryBroker()

    @broker.subscriber(queue)
    async def handler() -> None: ...

    async with TestCeleryBroker(broker):
        await broker.publish(CeleryTask("proj.tasks.anything"), queue=queue)

        handler.mock.assert_called_once()


@pytest.mark.asyncio()
async def test_wrong_queue_is_not_delivered(queue: str) -> None:
    broker = CeleryBroker()

    @broker.subscriber(queue)
    async def handler() -> None: ...

    async with TestCeleryBroker(broker):
        with pytest.raises(SubscriberNotFound):
            await broker.publish(CeleryTask("proj.tasks.add"), queue=f"{queue}-other")

        handler.mock.assert_not_called()


@pytest.mark.asyncio()
async def test_request_returns_handler_result(queue: str) -> None:
    broker = CeleryBroker()

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler() -> dict[str, int]:
        return {"result": 3}

    async with TestCeleryBroker(broker):
        response = await broker.request(CeleryTask("proj.tasks.add"), queue=queue)

        assert await response.decode() == {"result": 3}


@pytest.mark.asyncio()
async def test_headers_and_correlation_id_reach_the_handler(queue: str) -> None:
    broker = CeleryBroker()
    received: list[CeleryMessage] = []

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler(msg: CeleryMessage = Context("message")) -> None:
        received.append(msg)

    async with TestCeleryBroker(broker):
        await broker.publish(
            CeleryTask("proj.tasks.add"),
            queue=queue,
            correlation_id="task-id-1",
            headers={"custom": "value"},
        )

    (message,) = received
    assert message.correlation_id == "task-id-1"
    assert message.headers["task"] == "proj.tasks.add"
    assert message.headers["id"] == "task-id-1"
    assert message.headers["custom"] == "value"


@pytest.mark.asyncio()
async def test_publisher_result_is_routed_to_its_queue(queue: str) -> None:
    broker = CeleryBroker()
    publisher = broker.publisher(f"{queue}-out")

    @broker.subscriber(queue, task="proj.tasks.add")
    @publisher
    async def handler() -> dict[str, int]:
        return {"result": 3}

    async with TestCeleryBroker(broker):
        await broker.publish(CeleryTask("proj.tasks.add"), queue=queue)

        publisher.mock.assert_called_once_with({"result": 3})


@pytest.mark.asyncio()
async def test_middlewares_run_in_fake_mode(queue: str) -> None:
    seen: list[str] = []

    class TrackingMiddleware(BaseMiddleware[Any, Any]):
        @override
        async def on_receive(self) -> None:
            seen.append("enter")

        @override
        async def after_processed(self, *_args: Any) -> bool | None:
            seen.append("exit")
            return False

        @override
        async def consume_scope(self, call_next: Any, msg: Any) -> Any:
            seen.append("consume")
            return await call_next(msg)

    broker = CeleryBroker(middlewares=(TrackingMiddleware,))

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler() -> None: ...

    async with TestCeleryBroker(broker):
        await broker.publish(CeleryTask("proj.tasks.add"), queue=queue)

    assert seen == ["enter", "consume", "exit"]


@pytest.mark.asyncio()
async def test_raw_payload_round_trip(queue: str) -> None:
    broker = CeleryBroker()

    @broker.subscriber(queue)
    async def handler(body: dict[str, int]) -> None: ...

    async with TestCeleryBroker(broker):
        await broker.publish({"payload": 1}, queue=queue)

        handler.mock.assert_called_once_with({"payload": 1})


@pytest.mark.asyncio()
async def test_router_prefix_is_respected(queue: str) -> None:
    router = CeleryRouter(prefix="pre-")

    @router.subscriber(queue, task="proj.tasks.add")
    async def handler() -> None: ...

    broker = CeleryBroker()
    broker.include_router(router)

    async with TestCeleryBroker(broker):
        await broker.publish(CeleryTask("proj.tasks.add"), queue=f"pre-{queue}")

        handler.mock.assert_called_once()


@pytest.mark.asyncio()
async def test_protocol_v1_message_is_consumed(queue: str) -> None:
    """A hand-assembled v1 body (no headers) reaches the same handler."""
    broker = CeleryBroker()

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler(args: list[int], kwargs: dict[str, Any]) -> None: ...

    async with TestCeleryBroker(broker):
        await broker.publish(
            {
                "task": "proj.tasks.add",
                "id": "task-id-1",
                "args": [1, 2],
                "kwargs": {"debug": True},
                "retries": 0,
            },
            queue=queue,
        )

        handler.mock.assert_called_once_with(
            {"args": [1, 2], "kwargs": {"debug": True}},
        )
