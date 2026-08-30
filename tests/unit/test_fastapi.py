"""The FastAPI integration."""

from typing import Any

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from faststream_celery import CeleryTask, TestCeleryBroker
from faststream_celery.fastapi import CeleryMessage, CeleryRouter, Logger
from faststream_celery.publisher import CeleryPublisher
from faststream_celery.subscriber import CelerySubscriber
from tests.helpers import MEMORY_URL


@pytest.fixture()
def router() -> CeleryRouter:
    return CeleryRouter(MEMORY_URL)


class TestRouter:
    def test_it_builds_a_celery_broker(self, router: CeleryRouter) -> None:
        assert router.broker.config.url == MEMORY_URL

    def test_subscriber_returns_a_celery_subscriber(
        self,
        router: CeleryRouter,
        queue: str,
    ) -> None:
        subscriber = router.subscriber(queue, task="proj.tasks.add")

        assert isinstance(subscriber, CelerySubscriber)
        assert subscriber.task == "proj.tasks.add"

    def test_publisher_returns_a_celery_publisher(
        self,
        router: CeleryRouter,
        queue: str,
    ) -> None:
        publisher = router.publisher(queue, exchange="tasks", routing_key="high")

        assert isinstance(publisher, CeleryPublisher)
        assert publisher.queue == queue
        assert publisher.exchange == "tasks"
        assert publisher.routing_key == "high"

    def test_subscriber_options_reach_the_broker(
        self,
        router: CeleryRouter,
        queue: str,
    ) -> None:
        subscriber = router.subscriber(queue, prefetch_count=7)

        assert subscriber.config.prefetch_count == 7

    def test_http_routes_still_work(self, router: CeleryRouter) -> None:
        @router.get("/health")
        def health() -> dict[str, str]:
            return {"status": "ok"}

        app = FastAPI()
        app.include_router(router)

        with TestClient(app) as client:
            assert client.get("/health").json() == {"status": "ok"}


class TestHandlers:
    @pytest.mark.asyncio()
    async def test_a_handler_receives_a_published_task(
        self,
        router: CeleryRouter,
        queue: str,
    ) -> None:
        received: dict[str, Any] = {}

        @router.subscriber(queue, task="proj.tasks.add")
        async def handler(args: list[int], kwargs: dict[str, Any]) -> None:
            received["args"] = args

        async with TestCeleryBroker(router.broker) as broker:
            await broker.publish(
                CeleryTask("proj.tasks.add", args=[1, 2]),
                queue=queue,
            )

        assert received["args"] == [1, 2]

    @pytest.mark.asyncio()
    async def test_fastapi_dependencies_are_injected(
        self,
        router: CeleryRouter,
        queue: str,
    ) -> None:
        seen: list[str] = []

        def token() -> str:
            return "injected"

        @router.subscriber(queue, task="proj.tasks.add")
        async def handler(value: str = Depends(token)) -> None:
            seen.append(value)

        async with TestCeleryBroker(router.broker) as broker:
            await broker.publish(CeleryTask("proj.tasks.add"), queue=queue)

        assert seen == ["injected"]

    @pytest.mark.asyncio()
    async def test_the_context_annotations_resolve(
        self,
        router: CeleryRouter,
        queue: str,
    ) -> None:
        seen: dict[str, Any] = {}

        @router.subscriber(queue, task="proj.tasks.add")
        async def handler(message: CeleryMessage, logger: Logger) -> None:
            seen["task"] = message.headers["task"]
            seen["logger"] = logger

        async with TestCeleryBroker(router.broker) as broker:
            await broker.publish(
                CeleryTask("proj.tasks.add"),
                queue=queue,
                correlation_id="task-id-1",
            )

        assert seen["task"] == "proj.tasks.add"
        assert seen["logger"] is not None

    @pytest.mark.asyncio()
    async def test_a_publisher_forwards_the_return_value(
        self,
        router: CeleryRouter,
        queue: str,
    ) -> None:
        publisher = router.publisher(f"{queue}-out")

        @router.subscriber(queue, task="proj.tasks.add")
        @publisher
        async def handler() -> dict[str, int]:
            return {"result": 3}

        async with TestCeleryBroker(router.broker) as broker:
            await broker.publish(CeleryTask("proj.tasks.add"), queue=queue)

            publisher.mock.assert_called_once_with({"result": 3})
