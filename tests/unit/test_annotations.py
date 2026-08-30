import pytest
from kombu import Connection as KombuConnection

from faststream_celery import (
    CeleryBroker,
    CeleryTask,
    TestCeleryBroker,
    annotations as an,
)
from faststream_celery.message import CeleryMessage
from faststream_celery.publisher.producer import CeleryFastProducer


def test_public_annotations_are_exported() -> None:
    assert set(an.__all__) == {
        "CeleryBroker",
        "CeleryMessage",
        "CeleryProducer",
        "Connection",
        "ContextRepo",
        "Logger",
        "NoCast",
    }
    for name in an.__all__:
        assert hasattr(an, name), name


@pytest.mark.asyncio()
async def test_annotations_resolve_from_the_context(queue: str) -> None:
    broker = CeleryBroker()
    seen: dict[str, object] = {}

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler(
        message: an.CeleryMessage,
        current_broker: an.CeleryBroker,
        producer: an.CeleryProducer,
        logger: an.Logger,
        context: an.ContextRepo,
    ) -> None:
        seen["message"] = message
        seen["broker"] = current_broker
        seen["producer"] = producer
        seen["logger"] = logger
        seen["context"] = context

    async with TestCeleryBroker(broker):
        await broker.publish(
            CeleryTask("proj.tasks.add"),
            queue=queue,
            correlation_id="task-id-1",
        )

    assert isinstance(seen["message"], CeleryMessage)
    assert seen["broker"] is broker
    assert isinstance(seen["producer"], CeleryFastProducer)
    assert seen["logger"] is not None
    assert seen["context"] is broker.config.fd_config.context


@pytest.mark.asyncio()
async def test_connection_annotation_resolves(queue: str) -> None:
    broker = CeleryBroker()
    seen: list[object] = []

    @broker.subscriber(queue, task="proj.tasks.add")
    async def handler(connection: an.Connection) -> None:
        seen.append(connection)

    async with TestCeleryBroker(broker, connect_only=False):
        broker._connection = KombuConnection("memory://")
        await broker.publish(CeleryTask("proj.tasks.add"), queue=queue)

    assert isinstance(seen[0], KombuConnection)
