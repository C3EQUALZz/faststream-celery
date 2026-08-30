from typing import Any

import pytest
from faststream.message import StreamMessage

from faststream_celery import CeleryBroker
from faststream_celery.subscriber.usecase import _task_filter


def _message_with_task(task: str) -> StreamMessage[Any]:
    return StreamMessage(raw_message=None, body=b"", headers={"task": task})


def test_task_filter_matches_by_header() -> None:
    assert _task_filter("proj.tasks.add", _message_with_task("proj.tasks.add"))
    assert not _task_filter("proj.tasks.add", _message_with_task("proj.tasks.other"))


@pytest.mark.asyncio()
async def test_subscriber_installs_task_filter() -> None:
    broker = CeleryBroker()
    subscriber = broker.subscriber("celery", task="proj.tasks.add")

    @subscriber
    async def handler() -> None: ...

    (call,) = subscriber.calls
    assert await call.filter(_message_with_task("proj.tasks.add"))
    assert not await call.filter(_message_with_task("proj.tasks.other"))


@pytest.mark.asyncio()
async def test_subscriber_without_task_accepts_everything() -> None:
    broker = CeleryBroker()
    subscriber = broker.subscriber("celery")

    @subscriber
    async def handler() -> None: ...

    (call,) = subscriber.calls
    assert await call.filter(_message_with_task("proj.tasks.add"))
    assert await call.filter(_message_with_task("proj.tasks.other"))
