"""Chains, callbacks and errbacks (ticket-5)."""

from typing import Any
from unittest.mock import AsyncMock

import pytest

from faststream_celery import CeleryBroker, CeleryTask, TestCeleryBroker, signature
from faststream_celery.canvas import CanvasDispatcher
from faststream_celery.message import CeleryMessage, run_inline
from faststream_celery.parser import read_embed, read_headers
from faststream_celery.schemas.signature import call_args, queue_of
from faststream_celery.schemas.task import as_signature, build_task_envelope
from faststream_celery.types import TaskEmbed
from tests.helpers import consumer_message, task_message

INCOMING_QUEUE = "celery"


def embed(**slots: Any) -> TaskEmbed:
    return TaskEmbed(
        callbacks=slots.get("callbacks"),
        errbacks=slots.get("errbacks"),
        chain=slots.get("chain"),
        chord=slots.get("chord"),
    )


class TestSignature:
    def test_it_serializes_like_celery(self) -> None:
        sig = signature(
            "proj.tasks.b",
            args=[1],
            kwargs={"k": 2},
            options={"queue": "other"},
        )

        assert sig == {
            "task": "proj.tasks.b",
            "args": [1],
            "kwargs": {"k": 2},
            "options": {"queue": "other"},
            "subtask_type": None,
            "immutable": False,
        }

    def test_a_result_is_prepended_to_the_arguments(self) -> None:
        assert call_args(signature("b", args=[2, 3]), 1) == [1, 2, 3]

    def test_an_immutable_signature_refuses_the_result(self) -> None:
        """Celery spells this `.si()`, as against `.s()`."""
        assert call_args(signature("b", args=[2], immutable=True), 1) == [2]

    def test_a_signature_queue_wins_over_the_incoming_one(self) -> None:
        assert queue_of(signature("b", options={"queue": "other"}), "celery") == "other"

    def test_without_one_it_stays_on_the_incoming_queue(self) -> None:
        assert queue_of(signature("b"), "celery") == "celery"

    def test_a_celery_task_converts_to_a_signature(self) -> None:
        sig = as_signature(CeleryTask("proj.tasks.b", args=[1], kwargs={"k": 2}))

        assert sig["task"] == "proj.tasks.b"
        assert sig["args"] == [1]
        assert sig["kwargs"] == {"k": 2}

    def test_a_signature_passes_through(self) -> None:
        sig = signature("proj.tasks.b")

        assert as_signature(sig) is sig


class TestPublishingACanvas:
    def test_link_and_link_error_reach_the_embed(self) -> None:
        envelope = build_task_envelope(
            CeleryTask(
                "proj.tasks.a",
                link=[signature("proj.tasks.b")],
                link_error=[signature("proj.tasks.on_error")],
            ),
            task_id="task-id-1",
        )

        _, _, slots = envelope.body
        assert slots["callbacks"] == [signature("proj.tasks.b")]
        assert slots["errbacks"] == [signature("proj.tasks.on_error")]

    def test_a_chain_reaches_the_embed(self) -> None:
        envelope = build_task_envelope(
            CeleryTask("proj.tasks.a", chain=[signature("proj.tasks.c")]),
            task_id="task-id-1",
        )

        _, _, slots = envelope.body
        assert slots["chain"] == [signature("proj.tasks.c")]

    def test_an_empty_canvas_stays_null(self) -> None:
        """Celery writes `None`, not an empty list."""
        _, _, slots = build_task_envelope(
            CeleryTask("proj.tasks.a"),
            task_id="task-id-1",
        ).body

        assert slots == {
            "callbacks": None,
            "errbacks": None,
            "chain": None,
            "chord": None,
        }

    def test_a_task_is_its_own_root_by_default(self) -> None:
        headers = build_task_envelope(
            CeleryTask("proj.tasks.a"),
            task_id="task-id-1",
        ).headers

        assert headers["root_id"] == "task-id-1"
        assert headers["parent_id"] is None

    def test_canvas_position_is_carried(self) -> None:
        headers = build_task_envelope(
            CeleryTask(
                "proj.tasks.a",
                root_id="root-1",
                parent_id="parent-1",
                group="group-1",
                group_index=2,
            ),
            task_id="task-id-1",
        ).headers

        assert headers["root_id"] == "root-1"
        assert headers["parent_id"] == "parent-1"
        assert headers["group"] == "group-1"
        assert headers["group_index"] == 2


class TestReadingTheEmbed:
    def test_it_reads_a_published_canvas_back(self) -> None:
        envelope = build_task_envelope(
            CeleryTask("proj.tasks.a", link=[signature("proj.tasks.b")]),
            task_id="task-id-1",
        )
        incoming = consumer_message(envelope.body, headers=dict(envelope.headers))

        assert read_embed(incoming.message)["callbacks"] == [signature("proj.tasks.b")]

    def test_a_plain_message_has_no_canvas(self) -> None:
        assert read_embed(consumer_message({"a": 1}).message) == embed()

    def test_a_short_body_has_no_canvas(self) -> None:
        incoming = consumer_message([[1], {}], headers={"task": "proj.tasks.a"})

        assert read_embed(incoming.message) == embed()

    def test_a_non_mapping_embed_has_no_canvas(self) -> None:
        incoming = consumer_message([[1], {}, "nope"], headers={"task": "proj.tasks.a"})

        assert read_embed(incoming.message) == embed()


class TestDispatcher:
    @pytest.fixture()
    def producer(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture()
    def dispatcher(self, producer: AsyncMock) -> CanvasDispatcher:
        return CanvasDispatcher(producer)

    @pytest.fixture()
    def message(self) -> CeleryMessage:
        incoming = task_message("proj.tasks.a", task_id="task-id-1")
        incoming.message.delivery_info = {"routing_key": INCOMING_QUEUE}

        return CeleryMessage(
            raw_message=incoming,
            body=b"",
            ack_executor=run_inline,
            headers=dict(read_headers(incoming.message)),
            correlation_id="task-id-1",
            message_id="task-id-1",
        )

    @pytest.mark.asyncio()
    async def test_a_callback_receives_the_result(
        self,
        dispatcher: CanvasDispatcher,
        producer: AsyncMock,
        message: CeleryMessage,
    ) -> None:
        await dispatcher.on_success(
            message,
            embed(callbacks=[signature("proj.tasks.b")]),
            result=42,
        )

        (cmd,), _ = producer.publish.call_args
        assert cmd.body.task == "proj.tasks.b"
        assert cmd.body.args == [42]
        assert cmd.queue == INCOMING_QUEUE

    @pytest.mark.asyncio()
    async def test_every_callback_fires(
        self,
        dispatcher: CanvasDispatcher,
        producer: AsyncMock,
        message: CeleryMessage,
    ) -> None:
        await dispatcher.on_success(
            message,
            embed(callbacks=[signature("proj.tasks.b"), signature("proj.tasks.c")]),
            result=1,
        )

        published = [call.args[0].body.task for call in producer.publish.call_args_list]
        assert published == ["proj.tasks.b", "proj.tasks.c"]

    @pytest.mark.asyncio()
    async def test_the_chain_steps_from_the_end(
        self,
        dispatcher: CanvasDispatcher,
        producer: AsyncMock,
        message: CeleryMessage,
    ) -> None:
        """Celery keeps a chain reversed: the next step is the last element."""
        chain = [signature("proj.tasks.d"), signature("proj.tasks.c")]

        await dispatcher.on_success(message, embed(chain=chain), result=1)

        (cmd,), _ = producer.publish.call_args
        assert cmd.body.task == "proj.tasks.c"
        assert cmd.body.args == [1]
        assert list(cmd.body.chain) == [signature("proj.tasks.d")]

    @pytest.mark.asyncio()
    async def test_the_last_chain_step_carries_nothing_on(
        self,
        dispatcher: CanvasDispatcher,
        producer: AsyncMock,
        message: CeleryMessage,
    ) -> None:
        await dispatcher.on_success(
            message,
            embed(chain=[signature("proj.tasks.c")]),
            result=1,
        )

        (cmd,), _ = producer.publish.call_args
        assert list(cmd.body.chain) == []

    @pytest.mark.asyncio()
    async def test_an_immutable_chain_step_refuses_the_result(
        self,
        dispatcher: CanvasDispatcher,
        producer: AsyncMock,
        message: CeleryMessage,
    ) -> None:
        await dispatcher.on_success(
            message,
            embed(chain=[signature("proj.tasks.c", args=[9], immutable=True)]),
            result=1,
        )

        (cmd,), _ = producer.publish.call_args
        assert cmd.body.args == [9]

    @pytest.mark.asyncio()
    async def test_a_continuation_inherits_the_lineage(
        self,
        dispatcher: CanvasDispatcher,
        producer: AsyncMock,
        message: CeleryMessage,
    ) -> None:
        await dispatcher.on_success(
            message,
            embed(callbacks=[signature("proj.tasks.b")]),
            result=1,
        )

        (cmd,), _ = producer.publish.call_args
        assert cmd.body.parent_id == "task-id-1"
        assert cmd.body.root_id == "task-id-1"

    @pytest.mark.asyncio()
    async def test_a_signature_queue_is_honoured(
        self,
        dispatcher: CanvasDispatcher,
        producer: AsyncMock,
        message: CeleryMessage,
    ) -> None:
        await dispatcher.on_success(
            message,
            embed(callbacks=[signature("proj.tasks.b", options={"queue": "other"})]),
            result=1,
        )

        (cmd,), _ = producer.publish.call_args
        assert cmd.queue == "other"

    @pytest.mark.asyncio()
    async def test_an_empty_canvas_publishes_nothing(
        self,
        dispatcher: CanvasDispatcher,
        producer: AsyncMock,
        message: CeleryMessage,
    ) -> None:
        await dispatcher.on_success(message, embed(), result=1)

        producer.publish.assert_not_called()

    @pytest.mark.asyncio()
    async def test_an_errback_receives_the_failed_task_id(
        self,
        dispatcher: CanvasDispatcher,
        producer: AsyncMock,
        message: CeleryMessage,
    ) -> None:
        """Celery calls an errback with the task id, not the exception."""
        await dispatcher.on_failure(
            message,
            embed(errbacks=[signature("proj.tasks.on_error")]),
            task_id="task-id-1",
        )

        (cmd,), _ = producer.publish.call_args
        assert cmd.body.task == "proj.tasks.on_error"
        assert cmd.body.args == ["task-id-1"]

    @pytest.mark.asyncio()
    async def test_callbacks_do_not_fire_on_failure(
        self,
        dispatcher: CanvasDispatcher,
        producer: AsyncMock,
        message: CeleryMessage,
    ) -> None:
        await dispatcher.on_failure(
            message,
            embed(callbacks=[signature("proj.tasks.b")]),
            task_id="task-id-1",
        )

        producer.publish.assert_not_called()


class TestCanvasThroughTheBroker:
    @pytest.mark.asyncio()
    async def test_a_handler_result_travels_down_the_chain(self, queue: str) -> None:
        broker = CeleryBroker()
        seen: list[Any] = []

        @broker.subscriber(queue, task="proj.tasks.a")
        async def first(args: list[int], kwargs: dict[str, Any]) -> int:
            return sum(args)

        @broker.subscriber(queue, task="proj.tasks.b")
        async def second(args: list[int], kwargs: dict[str, Any]) -> None:
            seen.append(args)

        async with TestCeleryBroker(broker):
            await broker.publish(
                CeleryTask(
                    "proj.tasks.a",
                    args=[2, 3],
                    chain=[signature("proj.tasks.b")],
                ),
                queue=queue,
            )

        assert seen == [[5]]

    @pytest.mark.asyncio()
    async def test_a_callback_fires_after_success(self, queue: str) -> None:
        broker = CeleryBroker()
        seen: list[Any] = []

        @broker.subscriber(queue, task="proj.tasks.a")
        async def first() -> str:
            return "done"

        @broker.subscriber(queue, task="proj.tasks.notify")
        async def notify(args: list[str], kwargs: dict[str, Any]) -> None:
            seen.append(args)

        async with TestCeleryBroker(broker):
            await broker.publish(
                CeleryTask("proj.tasks.a", link=[signature("proj.tasks.notify")]),
                queue=queue,
            )

        assert seen == [["done"]]

    @pytest.mark.asyncio()
    async def test_an_errback_fires_after_a_failure(self, queue: str) -> None:
        broker = CeleryBroker()
        seen: list[Any] = []

        @broker.subscriber(queue, task="proj.tasks.a")
        async def first() -> None:
            msg = "boom"
            raise ValueError(msg)

        @broker.subscriber(queue, task="proj.tasks.on_error")
        async def on_error(args: list[str], kwargs: dict[str, Any]) -> None:
            seen.append(args)

        async with TestCeleryBroker(broker):
            with pytest.raises(ValueError, match="boom"):
                await broker.publish(
                    CeleryTask(
                        "proj.tasks.a",
                        link_error=[signature("proj.tasks.on_error")],
                    ),
                    queue=queue,
                    correlation_id="task-id-1",
                )

        assert seen == [["task-id-1"]]

    @pytest.mark.asyncio()
    async def test_a_chain_written_with_celery_tasks(self, queue: str) -> None:
        """A chain step may be written as a `CeleryTask` instead of a signature."""
        broker = CeleryBroker()
        seen: list[Any] = []

        @broker.subscriber(queue, task="proj.tasks.a")
        async def first() -> int:
            return 1

        @broker.subscriber(queue, task="proj.tasks.b")
        async def second(args: list[int], kwargs: dict[str, Any]) -> None:
            seen.append(args)

        async with TestCeleryBroker(broker):
            await broker.publish(
                CeleryTask("proj.tasks.a", chain=[CeleryTask("proj.tasks.b")]),
                queue=queue,
            )

        assert seen == [[1]]
