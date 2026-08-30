import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy

from faststream_celery import CeleryBroker, CeleryTask
from faststream_celery.message import ConsumerMessage
from faststream_celery.parser import extract_schedule, parse_iso8601, read_headers
from faststream_celery.schemas.task import build_task_envelope
from faststream_celery.subscriber.scheduler import EtaScheduler
from faststream_celery.subscriber.usecase import CelerySubscriber
from tests.helpers import consumer_message, recorded, task_message

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def subscribed(broker: CeleryBroker, **kwargs: Any) -> CelerySubscriber:
    """A subscriber ready to consume, without a connection to start against."""
    # `_drop_expired` logs, and the logger is only wired up on connect.
    broker._setup_logger()

    subscriber = broker.subscriber("celery", task="proj.tasks.add", **kwargs)

    @subscriber
    async def handler(args: list[int], kwargs: dict[str, Any]) -> None: ...

    # Normally done by `subscriber.start()`, which needs a live connection.
    subscriber._build_fastdepends_model()
    subscriber._post_start()

    return subscriber


class TestEtaHeaders:
    def test_countdown_becomes_an_eta(self) -> None:
        envelope = build_task_envelope(
            CeleryTask("proj.tasks.add", countdown=30),
            task_id="task-id-1",
            now=NOW,
        )

        assert envelope.headers["eta"] == "2026-01-01T00:00:30+00:00"
        assert envelope.headers["expires"] is None

    def test_relative_expires_becomes_absolute(self) -> None:
        envelope = build_task_envelope(
            CeleryTask("proj.tasks.add", expires=60),
            task_id="task-id-1",
            now=NOW,
        )

        assert envelope.headers["expires"] == "2026-01-01T00:01:00+00:00"

    def test_absolute_eta_is_kept(self) -> None:
        envelope = build_task_envelope(
            CeleryTask("proj.tasks.add", eta=NOW + timedelta(days=1)),
            task_id="task-id-1",
            now=NOW,
        )

        assert envelope.headers["eta"] == "2026-01-02T00:00:00+00:00"

    def test_naive_eta_is_treated_as_utc(self) -> None:
        envelope = build_task_envelope(
            CeleryTask("proj.tasks.add", eta=datetime(2026, 1, 1)),  # ruff: ignore[call-datetime-without-tzinfo]
            task_id="task-id-1",
        )

        assert envelope.headers["eta"] == "2026-01-01T00:00:00+00:00"

    def test_a_non_utc_eta_keeps_its_offset(self) -> None:
        eta = datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=3)))

        envelope = build_task_envelope(
            CeleryTask("proj.tasks.add", eta=eta),
            task_id="task-id-1",
        )

        assert envelope.headers["eta"] == "2026-01-01T00:00:00+03:00"

    def test_countdown_and_eta_are_mutually_exclusive(self) -> None:
        with pytest.raises(SetupError, match="mutually exclusive"):
            CeleryTask("proj.tasks.add", countdown=1, eta=NOW)

    def test_zero_countdown_is_still_scheduled(self) -> None:
        """`countdown=0` differs from no countdown: it names a due time."""
        envelope = build_task_envelope(
            CeleryTask("proj.tasks.add", countdown=0),
            task_id="task-id-1",
            now=NOW,
        )

        assert envelope.headers["eta"] == "2026-01-01T00:00:00+00:00"


class TestIso8601:
    @pytest.mark.parametrize(
        "value",
        (
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T00:00:00",
        ),
    )
    def test_supported_shapes_parse_to_utc(self, value: str) -> None:
        assert parse_iso8601(value) == NOW

    @pytest.mark.parametrize("value", ("not-a-date", "", None, 12345, []))
    def test_unreadable_values_become_none(self, value: object) -> None:
        assert parse_iso8601(value) is None

    def test_a_datetime_passes_through(self) -> None:
        assert parse_iso8601(NOW) == NOW

    def test_extract_reads_both_headers(self) -> None:
        expires = NOW + timedelta(minutes=5)

        message = task_message(eta=NOW, expires=expires)
        schedule = extract_schedule(read_headers(message.message))

        assert schedule.eta == NOW
        assert schedule.expires == expires

    def test_extract_of_a_plain_message_is_empty(self) -> None:
        schedule = extract_schedule(read_headers(consumer_message({"a": 1}).message))

        assert schedule.eta is None
        assert schedule.expires is None


class TestEtaScheduler:
    @pytest.mark.asyncio()
    async def test_dispatches_after_the_delay(self) -> None:
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
    async def test_stop_cancels_pending_messages(self) -> None:
        dispatched: list[str] = []

        async def dispatch(msg: str) -> None:
            dispatched.append(msg)

        scheduler: EtaScheduler[str] = EtaScheduler(dispatch)
        scheduler.schedule("task", delay=5)

        await scheduler.stop()
        await asyncio.sleep(0.01)

        assert dispatched == []
        assert scheduler.pending == 0

    @pytest.mark.asyncio()
    async def test_stop_is_idempotent(self) -> None:
        scheduler: EtaScheduler[str] = EtaScheduler(_never)

        await scheduler.stop()
        await scheduler.stop()

        assert scheduler.pending == 0

    @pytest.mark.asyncio()
    async def test_several_messages_wait_independently(self) -> None:
        dispatched: list[str] = []

        async def dispatch(msg: str) -> None:
            dispatched.append(msg)

        scheduler: EtaScheduler[str] = EtaScheduler(dispatch)
        scheduler.schedule("soon", delay=0.01)
        scheduler.schedule("later", delay=5)

        await asyncio.sleep(0.05)

        assert dispatched == ["soon"]
        assert scheduler.pending == 1

        await scheduler.stop()


async def _never(msg: str) -> None:  # pragma: no cover - never scheduled
    raise AssertionError


class TestSubscriberDispatch:
    @pytest.mark.asyncio()
    async def test_a_future_eta_is_deferred(self, broker: CeleryBroker) -> None:
        subscriber = subscribed(broker)
        consumed: list[ConsumerMessage] = []
        subscriber.consume_one = _collect(consumed)  # type: ignore[method-assign]

        message = task_message(eta=_in(seconds=30))
        await subscriber.dispatch(message)

        assert consumed == []
        assert subscriber._scheduler.pending == 1

        await subscriber._scheduler.stop()

    @pytest.mark.asyncio()
    async def test_a_past_eta_is_consumed_at_once(self, broker: CeleryBroker) -> None:
        subscriber = subscribed(broker)
        consumed: list[ConsumerMessage] = []
        subscriber.consume_one = _collect(consumed)  # type: ignore[method-assign]

        await subscriber.dispatch(task_message(eta=_in(seconds=-30)))

        assert len(consumed) == 1

    @pytest.mark.asyncio()
    async def test_a_plain_task_is_consumed_at_once(
        self,
        broker: CeleryBroker,
    ) -> None:
        subscriber = subscribed(broker)
        consumed: list[ConsumerMessage] = []
        subscriber.consume_one = _collect(consumed)  # type: ignore[method-assign]

        await subscriber.dispatch(task_message())

        assert len(consumed) == 1
        assert subscriber._scheduler.pending == 0

    @pytest.mark.asyncio()
    async def test_an_expired_task_is_dropped_and_acked(
        self,
        broker: CeleryBroker,
    ) -> None:
        subscriber = subscribed(broker)
        consumed: list[ConsumerMessage] = []
        subscriber.consume_one = _collect(consumed)  # type: ignore[method-assign]

        message = task_message(expires=_in(seconds=-1))
        await subscriber.dispatch(message)

        assert consumed == []
        assert recorded(message).acks == [False]

    @pytest.mark.asyncio()
    async def test_expiry_wins_over_a_future_eta(self, broker: CeleryBroker) -> None:
        """A task that expires before it is due never runs."""
        subscriber = subscribed(broker)
        consumed: list[ConsumerMessage] = []
        subscriber.consume_one = _collect(consumed)  # type: ignore[method-assign]

        message = task_message(eta=_in(seconds=30), expires=_in(seconds=-1))
        await subscriber.dispatch(message)

        assert consumed == []
        assert subscriber._scheduler.pending == 0
        assert recorded(message).acks == [False]

    @pytest.mark.asyncio()
    async def test_a_future_expiry_does_not_drop(self, broker: CeleryBroker) -> None:
        subscriber = subscribed(broker)
        consumed: list[ConsumerMessage] = []
        subscriber.consume_one = _collect(consumed)  # type: ignore[method-assign]

        await subscriber.dispatch(task_message(expires=_in(seconds=30)))

        assert len(consumed) == 1

    @pytest.mark.asyncio()
    async def test_an_unreadable_body_is_consumed_not_scheduled(
        self,
        broker: CeleryBroker,
    ) -> None:
        """A parsing error belongs to the pipeline, not to the scheduler."""
        subscriber = subscribed(broker)
        consumed: list[ConsumerMessage] = []
        subscriber.consume_one = _collect(consumed)  # type: ignore[method-assign]

        await subscriber.dispatch(
            consumer_message([[1, 2]], headers={"task": "proj.tasks.add"}),
        )

        assert len(consumed) == 1


class TestSettlingUnhandledMessages:
    @pytest.mark.asyncio()
    async def test_a_filtered_out_message_is_rejected(
        self,
        broker: CeleryBroker,
    ) -> None:
        """Unsettled, it would hold a prefetch slot for good."""
        subscriber = subscribed(broker)

        message = task_message("proj.tasks.other")
        await subscriber.consume(message)

        assert recorded(message).rejects == [False]

    @pytest.mark.asyncio()
    async def test_a_handled_message_is_not_rejected(
        self,
        broker: CeleryBroker,
    ) -> None:
        subscriber = subscribed(broker)

        message = task_message()
        await subscriber.consume(message)

        assert recorded(message).acks == [False]
        assert recorded(message).rejects == []

    @pytest.mark.asyncio()
    async def test_manual_ack_policy_leaves_the_message_alone(self) -> None:
        broker = CeleryBroker(ack_policy=AckPolicy.MANUAL)
        subscriber = subscribed(broker)

        message = task_message("proj.tasks.other")
        await subscriber.consume(message)

        assert recorded(message).rejects == []
        assert recorded(message).acks == []


def _in(*, seconds: float) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def _collect(sink: list[ConsumerMessage]) -> Any:
    async def consume_one(msg: ConsumerMessage) -> None:
        sink.append(msg)

    return consume_one
