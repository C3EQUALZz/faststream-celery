"""Requests expose Celery result envelopes in fake and connected modes."""

import asyncio
from typing import Any

import pytest
from faststream.exceptions import NackMessage
from faststream.response import Response

from faststream_celery import CeleryBroker, CeleryTask, TestCeleryBroker


@pytest.mark.asyncio()
@pytest.mark.parametrize("backend", (None, "redis://localhost"))
@pytest.mark.parametrize("fail", (False, True))
async def test_request_returns_the_task_outcome(
    queue: str,
    backend: str | None,
    fail: bool,
) -> None:
    broker = CeleryBroker(result_backend=backend)

    @broker.subscriber(queue, task="tests.outcome")
    async def handler() -> Response:
        if fail:
            error = "request failed"
            raise ValueError(error)
        return Response(7, headers={"x-result": "value"}, correlation_id="other-id")

    async with TestCeleryBroker(broker):
        response = await broker.request(
            CeleryTask("tests.outcome"),
            queue=queue,
            correlation_id="task-id",
            timeout=1,
        )
        envelope = await response.decode()
        assert isinstance(envelope, dict)
        assert envelope["task_id"] == response.correlation_id == "task-id"
        assert envelope["status"] == ("FAILURE" if fail else "SUCCESS")
        if fail:
            assert envelope["result"]["exc_type"] == "ValueError"
            assert envelope["result"]["exc_message"] == ["request failed"]
        else:
            assert envelope["result"] == 7
        assert response.raw_message.acknowledgement is None
        handler.mock.assert_called_once()


@pytest.mark.asyncio()
@pytest.mark.parametrize("backend", (None, "redis://localhost"))
@pytest.mark.parametrize("scenario", ("slow", "ignore_result", "nack", "unknown"))
async def test_request_without_a_result_times_out(
    queue: str,
    backend: str | None,
    scenario: str,
) -> None:
    broker = CeleryBroker(result_backend=backend)

    @broker.subscriber(queue, task="tests.outcome")
    async def handler() -> int:
        if scenario == "slow":
            await asyncio.sleep(1)
        if scenario == "nack":
            raise NackMessage
        return 7

    async with TestCeleryBroker(broker):
        with pytest.raises(TimeoutError, match=r"No Celery (reply|result)"):
            await broker.request(
                CeleryTask("tests.unknown" if scenario == "unknown" else "tests.outcome"),
                queue=queue,
                headers={"ignore_result": scenario == "ignore_result"},
                timeout=0.02,
            )


@pytest.mark.asyncio()
async def test_concurrent_requests_keep_their_replies(queue: str) -> None:
    broker = CeleryBroker()

    @broker.subscriber(queue, task="tests.echo")
    async def handler(args: list[int], kwargs: dict[str, Any]) -> int:
        await asyncio.sleep((3 - args[0]) * 0.01)
        return args[0]

    async with TestCeleryBroker(broker):
        responses = await asyncio.gather(
            *(
                broker.request(
                    CeleryTask("tests.echo", args=[value]),
                    queue=queue,
                    correlation_id=f"task-{value}",
                    timeout=1,
                )
                for value in range(3)
            ),
        )
        for value, response in enumerate(responses):
            envelope = await response.decode()
            assert isinstance(envelope, dict)
            assert envelope["task_id"] == f"task-{value}"
            assert envelope["result"] == value


@pytest.mark.asyncio()
async def test_rpc_request_can_receive_a_reply_from_another_test_broker(
    queue: str,
) -> None:
    client = CeleryBroker()
    worker = CeleryBroker()

    @worker.subscriber(queue, task="tests.echo")
    async def handler() -> int:
        return 7

    async with TestCeleryBroker(client, worker):
        response = await client.request(CeleryTask("tests.echo"), queue=queue, timeout=1)
        envelope = await response.decode()
        assert isinstance(envelope, dict)
        assert envelope["result"] == 7


@pytest.mark.asyncio()
async def test_a_none_return_is_a_successful_result(queue: str) -> None:
    broker = CeleryBroker()

    @broker.subscriber(queue)
    async def handler() -> None: ...

    async with TestCeleryBroker(broker):
        response = await broker.request(CeleryTask("tests.none"), queue=queue, timeout=1)
        envelope = await response.decode()
        assert isinstance(envelope, dict)
        assert envelope["status"] == "SUCCESS"
        assert envelope["result"] is None


@pytest.mark.asyncio()
async def test_timed_out_handlers_are_cancelled_when_the_test_broker_closes(
    queue: str,
) -> None:
    broker = CeleryBroker()
    started = asyncio.Event()
    cancelled = asyncio.Event()

    @broker.subscriber(queue)
    async def handler() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with TestCeleryBroker(broker):
        with pytest.raises(TimeoutError):
            await broker.request(CeleryTask("tests.pending"), queue=queue, timeout=0.02)
        assert started.is_set()
        assert not cancelled.is_set()
    assert cancelled.is_set()


@pytest.mark.asyncio()
async def test_closing_the_test_broker_cancels_pending_rpc_waiters(queue: str) -> None:
    broker = CeleryBroker()
    started = asyncio.Event()

    @broker.subscriber(queue)
    async def handler() -> None:
        started.set()
        await asyncio.Event().wait()

    async with TestCeleryBroker(broker):
        request = asyncio.create_task(
            broker.request(CeleryTask("tests.pending"), queue=queue, timeout=10),
        )
        await asyncio.wait_for(started.wait(), timeout=1)
    await asyncio.wait_for(asyncio.gather(request, return_exceptions=True), timeout=1)
    assert request.cancelled()
