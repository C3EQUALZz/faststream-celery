"""RPC requests ignore unrelated and intermediate Celery replies."""

import json
from collections import deque
from typing import Any
from unittest.mock import MagicMock

import pytest
from kombu import Message

from faststream_celery.publisher.producer import Destination, Payload, request_sync
from faststream_celery.schemas.result import build_success


class RpcReplies:
    def __init__(self, consumer: MagicMock) -> None:
        self.consumer = consumer
        self.messages: deque[Message] = deque()

    def drain_events(self, **_kwargs: Any) -> None:
        if not self.messages:
            raise TimeoutError
        self.consumer.call_args.kwargs["on_message"](self.messages.popleft())


@pytest.fixture()
def rpc(monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, RpcReplies]:
    consumer = MagicMock()
    stream = RpcReplies(consumer)
    connection = MagicMock()
    connection.drain_events.side_effect = stream.drain_events
    factory = MagicMock()
    factory.return_value.__enter__.return_value = connection
    monkeypatch.setattr("faststream_celery.publisher.producer.Consumer", consumer)
    monkeypatch.setattr("faststream_celery.publisher.producer.Queue", MagicMock())
    monkeypatch.setattr("faststream_celery.publisher.producer.Producer", MagicMock())
    monkeypatch.setattr("faststream_celery.publisher.producer.publish_sync", MagicMock())
    return factory, stream


def reply(
    status: str,
    task_id: str = "task-id-1",
    *,
    correlation_id: str | None = None,
) -> Message:
    return Message(
        body=json.dumps({**build_success(task_id, 7), "status": status}),
        content_type="application/json",
        content_encoding="utf-8",
        properties={"correlation_id": correlation_id or task_id},
        channel=MagicMock(),
    )


def request(factory: MagicMock, timeout: float = 5) -> Message:
    return request_sync(
        factory,
        Payload(b"task", None, None, {"task": "proj.tasks.add"}, "task-id-1", None),
        Destination(MagicMock(), "tasks", None),
        timeout=timeout,
    )


@pytest.mark.parametrize("status", ("SUCCESS", "FAILURE", "REVOKED"))
def test_rpc_waits_for_a_matching_terminal_result(
    rpc: tuple[MagicMock, RpcReplies],
    status: str,
) -> None:
    factory, stream = rpc
    discarded = [
        reply("SUCCESS", correlation_id="other-task"),
        reply("SUCCESS", task_id="other-task", correlation_id="task-id-1"),
        *(reply(state) for state in ("STARTED", "RETRY", "PROGRESS")),
    ]
    terminal = reply(status)
    stream.messages.extend([*discarded, terminal])

    response = request(factory)
    assert response is not terminal
    assert response.channel is None
    assert response.decode() == terminal.decode()
    assert terminal.acknowledged
    assert all(message.acknowledged for message in discarded)


def test_rpc_progress_does_not_reset_the_deadline(
    rpc: tuple[MagicMock, RpcReplies],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory, stream = rpc
    stream.messages.extend(reply("PROGRESS") for _ in range(3))
    clock = MagicMock(side_effect=[0, 0.25, 0.5, 0.75, 1.0])
    monkeypatch.setattr("faststream_celery.publisher.producer.monotonic", clock)

    with pytest.raises(TimeoutError, match="No Celery reply"):
        request(factory, timeout=1)


def test_terminal_reply_is_acked_before_its_connection_closes(
    rpc: tuple[MagicMock, RpcReplies],
) -> None:
    factory, stream = rpc
    terminal = reply("SUCCESS")
    stream.messages.append(terminal)
    order = MagicMock()
    assert terminal.channel is not None
    order.attach_mock(terminal.channel.basic_ack, "ack")
    order.attach_mock(factory.return_value.__exit__, "close")

    request(factory)

    assert [call[0] for call in order.mock_calls] == ["ack", "close"]
