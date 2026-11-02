"""Publisher requests use endpoint defaults and the broker's result mechanism."""

from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from faststream.message import StreamMessage
from faststream.message.source_type import SourceType
from faststream.middlewares import BaseMiddleware
from faststream.response import PublishType
from typing_extensions import override

from faststream_celery import CeleryBroker, CeleryRouter, CeleryTask, TestCeleryBroker
from faststream_celery.annotations import CeleryMessage
from faststream_celery.message import ConsumerMessage
from faststream_celery.parser import CeleryParser
from faststream_celery.response import CeleryPublishCommand


class TrackingParser(CeleryParser):
    def __init__(self) -> None:
        self.parse_count = 0
        self.decoded_sources: list[SourceType] = []

    @override
    async def parse_message(self, message: ConsumerMessage) -> CeleryMessage:
        self.parse_count += 1
        return await super().parse_message(message)

    @override
    async def decode_message(self, message: CeleryMessage) -> Any:
        self.decoded_sources.append(message.source_type)
        return await super().decode_message(message)


@pytest.mark.asyncio()
@pytest.mark.parametrize("backend", (None, "redis://localhost"))
@pytest.mark.parametrize("override", (False, True))
async def test_publisher_request_uses_defaults_and_explicit_overrides(
    queue: str,
    backend: str | None,
    override: bool,
) -> None:
    broker = CeleryBroker(result_backend=backend)
    router = CeleryRouter(prefix="pre.")
    publisher = router.publisher(
        queue,
        exchange="configured-exchange",
        routing_key="configured-key",
        headers={"x-default": "endpoint", "x-shared": "endpoint"},
        reply_to="unused-reply-queue",
    )
    broker.include_router(router)
    destination = f"{queue}.override" if override else f"pre.{queue}"
    received: list[CeleryMessage] = []

    @broker.subscriber(destination, task="tests.request")
    async def handler(message: CeleryMessage) -> int:
        received.append(message)
        return 7

    async with TestCeleryBroker(broker):
        response = await publisher.request(
            CeleryTask("tests.request"),
            queue=destination if override else None,
            exchange="" if override else None,
            routing_key=destination if override else None,
            headers={"x-shared": "message"},
            correlation_id="request-id",
            timeout=1,
        )
        envelope = await response.decode()
        assert isinstance(envelope, dict)
        assert envelope["task_id"] == response.correlation_id == "request-id"
        assert envelope["status"] == "SUCCESS"
        assert envelope["result"] == 7
        assert response.source_type is SourceType.RESPONSE
        assert response.raw_message.acknowledgement is None
        handler.mock.assert_called_once()
        (message,) = received
        assert message.correlation_id == "request-id"
        assert message.headers["x-default"] == "endpoint"
        assert message.headers["x-shared"] == "message"
        assert message.raw_message.message.delivery_info == {
            "exchange": "" if override else "configured-exchange",
            "routing_key": destination if override else "configured-key",
        }
        if backend is not None:
            assert message.reply_to == ""
        else:
            assert message.reply_to.startswith("faststream-celery.reply.")


@pytest.mark.asyncio()
@pytest.mark.parametrize("backend", (None, "redis://localhost"))
async def test_publisher_request_returns_a_failure_envelope(
    queue: str,
    backend: str | None,
) -> None:
    broker = CeleryBroker(result_backend=backend)
    publisher = broker.publisher(queue)

    @broker.subscriber(queue)
    async def handler() -> None:
        error = "request failed"
        raise ValueError(error)

    async with TestCeleryBroker(broker):
        response = await publisher.request(7, correlation_id="failed-id", timeout=1)
        envelope = await response.decode()
        assert isinstance(envelope, dict)
        assert envelope["task_id"] == "failed-id"
        assert envelope["status"] == "FAILURE"
        assert envelope["result"]["exc_type"] == "ValueError"
        assert envelope["result"]["exc_message"] == ["request failed"]


@pytest.mark.asyncio()
@pytest.mark.parametrize("backend", (None, "redis://localhost"))
@pytest.mark.parametrize("timeout", (0.0, 0.02))
async def test_publisher_request_without_a_result_times_out(
    queue: str,
    backend: str | None,
    timeout: float,
) -> None:
    broker = CeleryBroker(result_backend=backend)
    publisher = broker.publisher(queue)

    async with TestCeleryBroker(broker):
        with pytest.raises(TimeoutError, match=r"No Celery (reply|result)"):
            await publisher.request(
                CeleryTask("tests.ignored"),
                headers={"ignore_result": True},
                timeout=timeout,
            )


@pytest.mark.asyncio()
@pytest.mark.parametrize("backend", (None, "redis://localhost"))
@pytest.mark.parametrize("api", ("broker", "publisher"))
async def test_request_passes_once_through_publish_and_response_middlewares(
    queue: str,
    backend: str | None,
    api: str,
) -> None:
    events: list[str] = []

    class TrackingMiddleware(BaseMiddleware[Any, Any]):
        @override
        async def publish_scope(
            self,
            call_next: Callable[[CeleryPublishCommand], Awaitable[Any]],
            cmd: CeleryPublishCommand,
        ) -> Any:
            if cmd.publish_type is PublishType.REQUEST:
                events.append("request")
            return await call_next(cmd)

        @override
        async def consume_scope(
            self,
            call_next: Callable[..., Awaitable[Any]],
            msg: StreamMessage[Any],
        ) -> Any:
            if msg.source_type is SourceType.RESPONSE:
                events.append("response")
            return await call_next(msg)

    broker = CeleryBroker(result_backend=backend, middlewares=(TrackingMiddleware,))
    client = broker.publisher(queue) if api == "publisher" else broker

    @broker.subscriber(queue)
    async def handler() -> int:
        return 7

    async with TestCeleryBroker(broker):
        response = await client.request(7, queue=queue, timeout=None)
        envelope = await response.decode()
        assert isinstance(envelope, dict)
        assert envelope["result"] == 7
        assert events == ["request", "response"]


@pytest.mark.asyncio()
@pytest.mark.parametrize("backend", (None, "redis://localhost"))
@pytest.mark.parametrize("api", ("broker", "publisher"))
async def test_request_uses_the_custom_parser_and_decoder_for_the_response(
    queue: str,
    backend: str | None,
    api: str,
) -> None:
    parser = TrackingParser()
    broker = CeleryBroker(
        result_backend=backend,
        parser=parser.parse_message,
        decoder=parser.decode_message,
    )
    client = broker.publisher(queue) if api == "publisher" else broker

    @broker.subscriber(queue)
    async def handler() -> int:
        return 7

    async with TestCeleryBroker(broker):
        response = await client.request(7, queue=queue, timeout=1)
        envelope = await response.decode()
        assert isinstance(envelope, dict)
        assert envelope["result"] == 7
        assert parser.parse_count == 2
        assert parser.decoded_sources == [
            SourceType.CONSUME,
            SourceType.RESPONSE,
        ]
