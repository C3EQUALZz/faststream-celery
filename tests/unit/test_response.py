"""Response bodies feed Celery results and canvas without losing publisher metadata."""

from typing import Any

import pytest
from faststream.response import Response

from faststream_celery import CeleryBroker, CeleryTask, TestCeleryBroker, signature
from faststream_celery.annotations import CeleryMessage
from faststream_celery.backend import InMemoryResultBackend


@pytest.mark.asyncio()
@pytest.mark.parametrize("body", (3, {"value": 3}, None))
@pytest.mark.parametrize("configured_backend", (False, True))
async def test_response_body_is_reported_without_changing_publisher_metadata(
    queue: str,
    body: Any,
    configured_backend: bool,
) -> None:
    broker = CeleryBroker(
        result_backend="redis://localhost" if configured_backend else None,
    )
    observer = CeleryBroker()
    replies = observer.publisher("reply-queue")
    continuations = observer.publisher("continuations")
    output: list[CeleryMessage] = []
    response = Response(
        body,
        headers={"x-result": "metadata"},
        correlation_id="outgoing-id",
    )

    @observer.subscriber("outgoing")
    async def published(message: CeleryMessage) -> None:
        output.append(message)

    @broker.subscriber(queue, task="tests.response")
    @broker.publisher("outgoing")
    async def handler() -> Response:
        return response

    task = CeleryTask(
        "tests.response",
        link=[signature("tests.callback", options={"queue": "continuations"})],
        chain=[signature("tests.following", options={"queue": "continuations"})],
    )
    async with TestCeleryBroker(broker, observer):
        await broker.publish(
            task,
            queue=queue,
            correlation_id="task-id",
            reply_to="reply-queue",
        )

        backend = broker.config.broker_config.result_backend
        if configured_backend:
            assert isinstance(backend, InMemoryResultBackend)
            assert backend.results["task-id"]["result"] == body
            assert backend.results["task-id"]["status"] == "SUCCESS"
        else:
            assert backend is None

        replies.mock.assert_called_once()
        reply = replies.mock.call_args.args[0]
        assert reply["task_id"] == "task-id"
        assert reply["status"] == "SUCCESS"
        assert reply["result"] == body

        assert continuations.mock.call_count == 2
        for call in continuations.mock.call_args_list:
            assert call.args[0] == {"args": [body], "kwargs": {}}

        # The stock codec sends a raw None payload as an empty byte string.
        published.mock.assert_called_once_with(b"" if body is None else body)
        (message,) = output
        assert message.headers["x-result"] == "metadata"
        assert message.correlation_id == "outgoing-id"
        assert response.headers == {"x-result": "metadata"}
        assert response.correlation_id == "outgoing-id"
