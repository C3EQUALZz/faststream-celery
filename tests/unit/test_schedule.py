import asyncio
import json
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from faststream.middlewares import AckPolicy
from kombu import Message

from faststream_celery import CeleryBroker, CeleryTask
from faststream_celery.message import ConsumerMessage
from faststream_celery.parser import extract_schedule, parse_iso8601, read_headers
from faststream_celery.subscriber.scheduler import EtaScheduler
from faststream_celery.subscriber.usecase import CelerySubscriber
from faststream_celery.task import build_task_envelope


async def _run_inline(action: Callable[[], None]) -> None:
    action()


def _task_message(
    *,
    eta: datetime | None = None,
    expires: datetime | float | None = None,
) -> ConsumerMessage:
    envelope = build_task_envelope(
        CeleryTask("proj.tasks.add", args=[1, 2], eta=eta, expires=expires),
        task_id="task-id-1",
    )
    raw = Message(
        body=json.dumps(envelope.body).encode(),
        content_type="application/json",
        content_encoding="utf-8",
        headers=dict(envelope.headers),
        properties={},
    )
    return ConsumerMessage(raw, _run_inline)


def test_countdown_becomes_an_eta_header() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    envelope = build_task_envelope(
        CeleryTask("proj.tasks.add", countdown=30),
        task_id="task-id-1",
        now=now,
    )

    assert envelope.headers["eta"] == "2026-01-01T00:00:30+00:00"
    assert envelope.headers["expires"] is None


def test_relative_expires_becomes_an_absolute_header() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    envelope = build_task_envelope(
        CeleryTask("proj.tasks.add", expires=60),
        task_id="task-id-1",
        now=now,
    )

    assert envelope.headers["expires"] == "2026-01-01T00:01:00+00:00"


def test_naive_eta_is_treated_as_utc() -> None:
    envelope = build_task_envelope(
        CeleryTask("proj.tasks.add", eta=datetime(2026, 1, 1)),  # ruff: ignore[call-datetime-without-tzinfo]
        task_id="task-id-1",
    )

    assert envelope.headers["eta"] == "2026-01-01T00:00:00+00:00"


def test_countdown_and_eta_are_mutually_exclusive() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        CeleryTask("proj.tasks.add", countdown=1, eta=datetime.now(timezone.utc))


def test_parse_iso8601_accepts_the_z_suffix() -> None:
    assert parse_iso8601("2026-01-01T00:00:00Z") == datetime(
        2026,
        1,
        1,
        tzinfo=timezone.utc,
    )


def test_parse_iso8601_rejects_garbage() -> None:
    assert parse_iso8601("not-a-date") is None
    assert parse_iso8601(None) is None


def test_extract_schedule_reads_both_headers() -> None:
    eta = datetime(2026, 1, 1, tzinfo=timezone.utc)
    expires = eta + timedelta(minutes=5)

    message = _task_message(eta=eta, expires=expires)
    schedule = extract_schedule(read_headers(message.message))

    assert schedule.eta == eta
    assert schedule.expires == expires


@pytest.mark.asyncio()
async def test_scheduler_dispatches_after_the_delay() -> None:
    dispatched: list[str] = []

    async def dispatch(msg: str) -> None:
        dispatched.append(msg)

    scheduler: EtaScheduler[str] = EtaScheduler(dispatch)
    scheduler.schedule("task", delay=0.01)

    assert scheduler.pending == 1
    assert dispatched == []

    await asyncio.sleep(0.05)

    assert dispatched == ["task"]
    assert scheduler.pending == 0


@pytest.mark.asyncio()
async def test_scheduler_stop_cancels_pending_messages() -> None:
    dispatched: list[str] = []

    async def dispatch(msg: str) -> None:
        dispatched.append(msg)

    scheduler: EtaScheduler[str] = EtaScheduler(dispatch)
    scheduler.schedule("task", delay=5)

    await scheduler.stop()
    await asyncio.sleep(0.01)

    assert dispatched == []
    assert scheduler.pending == 0


def _subscriber() -> CelerySubscriber:
    broker = CeleryBroker()
    # `_drop_expired` logs, and the logger is only wired up on connect.
    broker._setup_logger()

    subscriber = broker.subscriber("celery", task="proj.tasks.add")

    @subscriber
    async def handler(args: list[int], kwargs: dict[str, Any]) -> None: ...

    # Normally done by `subscriber.start()`, which needs a live connection.
    subscriber._build_fastdepends_model()
    subscriber._post_start()

    return subscriber


@pytest.mark.asyncio()
async def test_future_eta_defers_instead_of_consuming() -> None:
    subscriber = _subscriber()
    consumed: list[ConsumerMessage] = []
    subscriber.consume_one = consumed.append  # type: ignore[method-assign,assignment]

    message = _task_message(eta=datetime.now(timezone.utc) + timedelta(seconds=30))
    await subscriber.dispatch(message)

    assert consumed == []
    assert subscriber._scheduler.pending == 1

    await subscriber._scheduler.stop()


@pytest.mark.asyncio()
async def test_past_eta_is_consumed_immediately() -> None:
    subscriber = _subscriber()
    consumed: list[ConsumerMessage] = []

    async def consume_one(msg: ConsumerMessage) -> None:
        consumed.append(msg)

    subscriber.consume_one = consume_one  # type: ignore[method-assign]

    message = _task_message(eta=datetime.now(timezone.utc) - timedelta(seconds=30))
    await subscriber.dispatch(message)

    assert len(consumed) == 1


@pytest.mark.asyncio()
async def test_expired_task_is_dropped_and_acked() -> None:
    subscriber = _subscriber()
    consumed: list[ConsumerMessage] = []

    async def consume_one(msg: ConsumerMessage) -> None:
        consumed.append(msg)

    subscriber.consume_one = consume_one  # type: ignore[method-assign]

    acked: list[bool] = []
    message = _task_message(expires=datetime.now(timezone.utc) - timedelta(seconds=1))
    message.message.ack = lambda *_args, **_kwargs: acked.append(True)

    await subscriber.dispatch(message)

    assert consumed == []
    assert acked == [True]


@pytest.mark.asyncio()
async def test_plain_task_is_consumed_immediately() -> None:
    subscriber = _subscriber()
    consumed: list[ConsumerMessage] = []

    async def consume_one(msg: ConsumerMessage) -> None:
        consumed.append(msg)

    subscriber.consume_one = consume_one  # type: ignore[method-assign]

    await subscriber.dispatch(_task_message())

    assert len(consumed) == 1
    assert subscriber._scheduler.pending == 0


@pytest.mark.asyncio()
async def test_filtered_out_message_is_rejected() -> None:
    """An unmatched message must be settled or the prefetch window stalls."""
    subscriber = _subscriber()

    rejected: list[bool] = []
    message = _task_message()
    message.message.headers["task"] = "proj.tasks.other"
    message.message.reject = lambda requeue=False: rejected.append(requeue)

    await subscriber.consume(message)

    assert rejected == [False]


@pytest.mark.asyncio()
async def test_handled_message_is_not_rejected_twice() -> None:
    subscriber = _subscriber()

    rejected: list[bool] = []
    acked: list[bool] = []
    message = _task_message()

    def ack(multiple: bool = False) -> None:
        # A real kombu message needs a channel to ack; record the state
        # change the broker would have made instead.
        message.message._state = "ACK"
        acked.append(True)

    message.message.ack = ack
    message.message.reject = lambda requeue=False: rejected.append(requeue)

    await subscriber.consume(message)

    assert acked == [True]
    assert rejected == []


@pytest.mark.asyncio()
async def test_manual_ack_policy_leaves_the_message_alone() -> None:
    broker = CeleryBroker(ack_policy=AckPolicy.MANUAL)
    broker._setup_logger()

    subscriber = broker.subscriber("celery", task="proj.tasks.add")

    @subscriber
    async def handler() -> None: ...

    subscriber._build_fastdepends_model()
    subscriber._post_start()

    rejected: list[bool] = []
    message = _task_message()
    message.message.headers["task"] = "proj.tasks.other"
    message.message.reject = lambda requeue=False: rejected.append(requeue)

    await subscriber.consume(message)

    assert rejected == []
