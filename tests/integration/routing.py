"""Shared task-routing parity checks for RabbitMQ and Redis."""

import asyncio

import pytest

from faststream_celery import CeleryBroker, CeleryRouter, CeleryTask, TestCeleryBroker
from tests.helpers import running

from .base import CONSUME_TIMEOUT, wait_for


class RoutingTestcase:
    @pytest.mark.asyncio()
    @pytest.mark.parametrize("fake", (False, True))
    @pytest.mark.parametrize("fallback_first", (False, True))
    @pytest.mark.parametrize("operation", ("publish", "request"))
    async def test_exact_and_fallback_dispatch_match_in_fake_and_live_modes(
        self,
        broker: CeleryBroker,
        queue: str,
        fake: bool,
        fallback_first: bool,
        operation: str,
    ) -> None:
        router = CeleryRouter()
        exact_scope = router if fallback_first else broker
        fallback_scope = broker if fallback_first else router
        seen: list[str] = []
        complete = asyncio.Event()

        @exact_scope.subscriber(queue, task="tests.exact")
        async def exact() -> str:
            seen.append("exact")
            if len(seen) == 3:
                complete.set()
            return "exact"

        @fallback_scope.subscriber(queue)
        async def fallback() -> str:
            seen.append("fallback")
            if len(seen) == 3:
                complete.set()
            return "fallback"

        broker.include_router(router)
        context = TestCeleryBroker(broker) if fake else running(broker)
        async with context:
            for index, (payload, expected) in enumerate(
                (
                    (CeleryTask("tests.exact"), "exact"),
                    (CeleryTask("tests.other"), "fallback"),
                    ({"value": 1}, "fallback"),
                ),
            ):
                task_id = f"{queue}-{index}"
                if operation == "request":
                    response = await broker.request(
                        payload,
                        queue=queue,
                        correlation_id=task_id,
                        timeout=CONSUME_TIMEOUT,
                    )
                    envelope = await response.decode()
                    assert isinstance(envelope, dict)
                    assert envelope["result"] == expected
                else:
                    await broker.publish(payload, queue=queue, correlation_id=task_id)
            await wait_for(complete)
            assert seen == ["exact", "fallback", "fallback"]
