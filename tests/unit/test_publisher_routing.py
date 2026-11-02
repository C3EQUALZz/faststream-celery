"""Publisher endpoints keep their routing in direct and subscriber flows."""

import pytest
from faststream.response import PublishType, Response

from faststream_celery import CeleryBroker, CeleryRouter, TestCeleryBroker
from faststream_celery.annotations import CeleryMessage
from faststream_celery.response import CeleryPublishCommand


@pytest.mark.asyncio()
@pytest.mark.parametrize("prefix", ("", "pre."))
@pytest.mark.parametrize("exchange", (None, "tasks", ""))
@pytest.mark.parametrize("operation", ("publish", "decorator"))
async def test_publisher_routing_survives_both_publish_paths(
    queue: str,
    prefix: str,
    exchange: str | None,
    operation: str,
) -> None:
    broker = CeleryBroker()
    router = CeleryRouter(prefix=prefix)
    destination = f"{prefix}{queue}"
    routing_key = destination if exchange == "" else "tasks.output"
    publisher = router.publisher(
        queue,
        exchange=exchange,
        routing_key=routing_key,
        headers={"x-default": "endpoint", "x-shared": "endpoint"},
    )
    broker.include_router(router)
    received: list[CeleryMessage] = []

    @broker.subscriber(destination)
    async def output(message: CeleryMessage) -> None:
        received.append(message)

    @broker.subscriber(f"{queue}.input")
    @publisher
    async def handler() -> Response:
        return Response(
            {"value": 7},
            headers={"x-shared": "message"},
            correlation_id="output-id",
        )

    async with TestCeleryBroker(broker):
        if operation == "publish":
            await publisher.publish(
                {"value": 7},
                headers={"x-shared": "message"},
                correlation_id="output-id",
            )
        else:
            await broker.publish({}, queue=f"{queue}.input")

        output.mock.assert_called_once_with({"value": 7})
        publisher.mock.assert_called_once_with({"value": 7})
        (message,) = received
        assert message.raw_message.message.delivery_info == {
            "exchange": exchange if exchange is not None else destination,
            "routing_key": routing_key,
        }
        assert message.headers["x-default"] == "endpoint"
        assert message.headers["x-shared"] == "message"
        assert message.correlation_id == "output-id"


@pytest.mark.asyncio()
async def test_direct_publish_can_override_the_exchange_with_an_empty_name(
    queue: str,
) -> None:
    broker = CeleryBroker()
    publisher = broker.publisher(queue, exchange="configured-exchange")
    received: list[CeleryMessage] = []

    @broker.subscriber(queue)
    async def output(message: CeleryMessage) -> None:
        received.append(message)

    async with TestCeleryBroker(broker):
        await publisher.publish(7, exchange="")
        output.mock.assert_called_once_with(7)
        assert received[0].raw_message.message.delivery_info["exchange"] == ""


@pytest.mark.asyncio()
@pytest.mark.parametrize("exchange", ("explicit-exchange", ""))
async def test_subscriber_flow_preserves_explicit_command_routing(
    queue: str,
    exchange: str,
) -> None:
    broker = CeleryBroker()
    publisher = broker.publisher(
        "configured-queue",
        exchange="configured-exchange",
        routing_key="configured-key",
    )
    received: list[CeleryMessage] = []

    @broker.subscriber(queue)
    async def output(message: CeleryMessage) -> None:
        received.append(message)

    command = CeleryPublishCommand(
        7,
        queue=queue,
        exchange=exchange,
        routing_key=queue,
        _publish_type=PublishType.REPLY,
    )
    async with TestCeleryBroker(broker):
        await publisher._publish(command, _extra_middlewares=())
        output.mock.assert_called_once_with(7)
        assert received[0].raw_message.message.delivery_info == {
            "exchange": exchange,
            "routing_key": queue,
        }
