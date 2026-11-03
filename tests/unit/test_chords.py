"""Chord dispatch boundaries and the Redis protocol used with Celery."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from celery import Celery
from faststream.exceptions import SetupError
from kombu.utils.json import (
    dumps as kombu_dumps,
    loads as kombu_loads,
)

from faststream_celery import CeleryTask, signature
from faststream_celery.backend.redis import GROUP_PREFIX, RedisResultBackend
from faststream_celery.canvas import CanvasDispatcher
from faststream_celery.exceptions import ChordJoinError
from faststream_celery.message import CeleryMessage
from faststream_celery.schemas.result import ChordResult, build_failure, build_success
from faststream_celery.schemas.task import as_signature, build_task_envelope
from tests.helpers import task_message


@pytest.fixture()
def message() -> CeleryMessage:
    raw = task_message("part", task_id="part-id")
    return CeleryMessage(
        raw_message=raw,
        body=b"",
        headers={"id": "part-id", "group": "incoming-group", "group_index": 7},
        correlation_id="part-id",
        message_id="part-id",
    )


@pytest.fixture()
def chord_options() -> dict[str, Any]:
    return {
        "group_id": "group-id",
        "group_index": 0,
        "chord": signature("body", options={"task_id": "body-id"}),
    }


class TestChordDispatch:
    @pytest.mark.asyncio()
    @pytest.mark.parametrize("immutable", (False, True))
    async def test_callback_receives_ordered_values(
        self,
        message: CeleryMessage,
        chord_options: dict[str, Any],
        *,
        immutable: bool,
    ) -> None:
        producer = AsyncMock()
        backend = AsyncMock(spec=RedisResultBackend)
        backend.record_chord_part.return_value = [
            ChordResult("first", "SUCCESS", 1),
            ChordResult("second", "SUCCESS", 2),
        ]
        dispatcher = CanvasDispatcher(producer, backend)
        callback = signature(
            "body",
            args=["own"],
            options={
                "task_id": "body-id",
                "queue": "body-queue",
            },
            immutable=immutable,
        )
        result = build_success("part-id", 1)
        chord_options["chord"] = callback

        await dispatcher.on_chord_part(message, chord_options, result, queue="parts")

        backend.record_chord_part.assert_awaited_once_with("group-id", 0, result)
        command = producer.publish.call_args.args[0]
        assert command.body.args == (["own"] if immutable else [[1, 2], "own"])
        assert command.correlation_id == "body-id"
        assert command.queue == "body-queue"
        backend.clear_chord.assert_awaited_once_with("group-id")

    @pytest.mark.asyncio()
    async def test_incomplete_group_does_not_dispatch(
        self,
        message: CeleryMessage,
        chord_options: dict[str, Any],
    ) -> None:
        producer = AsyncMock()
        backend = AsyncMock(spec=RedisResultBackend)
        backend.record_chord_part.return_value = None

        await CanvasDispatcher(producer, backend).on_chord_part(
            message,
            chord_options,
            build_success("part-id", 1),
            queue="parts",
        )

        producer.publish.assert_not_awaited()
        backend.clear_chord.assert_not_awaited()

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("failure_source", ("dependency", "publish", "join"))
    async def test_failure_finishes_the_body_and_runs_its_errback(
        self,
        message: CeleryMessage,
        chord_options: dict[str, Any],
        *,
        failure_source: str,
    ) -> None:
        producer = AsyncMock()
        backend = AsyncMock(spec=RedisResultBackend)
        failure = build_failure("part-id", ValueError("failed"))
        message.headers["root_id"] = "part-root"
        backend.record_chord_part.return_value = [
            ChordResult(
                "part-id",
                "SUCCESS" if failure_source == "publish" else "FAILURE",
                1 if failure_source == "publish" else failure["result"],
            ),
        ]
        if failure_source == "publish":
            producer.publish.side_effect = [OSError("offline"), None]
        elif failure_source == "join":
            backend.record_chord_part.side_effect = ChordJoinError("results expired")
        callback = signature(
            "body",
            options={
                "task_id": "body-id",
                "chain": [signature("tail", options={"task_id": "tail-id"})],
                "link_error": [
                    signature("error", args=["own"], options={"queue": "errors"}),
                ],
            },
        )
        chord_options["chord"] = callback

        await CanvasDispatcher(producer, backend).on_chord_part(
            message,
            chord_options,
            failure,
            queue="parts",
        )

        assert backend.store.await_count == 2
        body_id, body_failure = backend.store.await_args_list[0].args
        assert body_id == "body-id"
        assert body_failure["result"]["exc_type"] == "ChordError"
        assert body_failure["result"]["exc_module"] == "celery.exceptions"
        if failure_source == "join":
            assert body_failure["result"]["exc_message"] == [
                "Join error: results expired",
            ]
        assert backend.store.await_args_list[1].args[0] == "tail-id"
        command = producer.publish.call_args.args[0]
        assert command.body.task == "error"
        assert command.body.args == ["body-id", "own"]
        assert command.body.parent_id == "body-id"
        assert command.body.root_id == "body-id"
        backend.clear_chord.assert_awaited_once_with("group-id")

    @pytest.mark.asyncio()
    async def test_backend_without_chord_coordination_is_explicit(
        self,
        message: CeleryMessage,
        chord_options: dict[str, Any],
    ) -> None:
        with pytest.raises(SetupError, match="Redis result backend"):
            await CanvasDispatcher(AsyncMock()).on_chord_part(
                message,
                chord_options,
                build_success("part-id", 1),
                queue="parts",
            )

    def test_chord_metadata_survives_task_serialization(self) -> None:
        callback = signature("body", options={"task_id": "body-id"})
        task = CeleryTask("part", group="group-id", group_index=0, chord=callback)

        assert as_signature(task)["options"]["chord"] == callback
        assert build_task_envelope(task, task_id="part-id").body[2]["chord"] == callback


class TestRedisChordJournal:
    @pytest.fixture()
    def saved_group(self) -> bytes:
        with Celery("saved-group", backend="cache+memory://") as app:
            group = app.GroupResult(
                "group-id",
                [app.AsyncResult("second"), app.AsyncResult("first")],
            )
            return kombu_dumps({"result": group.as_tuple()}).encode()

    @pytest.fixture()
    def pipe(self) -> MagicMock:
        pipe = MagicMock()
        pipe.execute = AsyncMock()
        return pipe

    @pytest.fixture()
    def client(self, pipe: MagicMock) -> MagicMock:
        client = MagicMock()
        client.pipeline.return_value.__aenter__ = AsyncMock(return_value=pipe)
        client.pipeline.return_value.__aexit__ = AsyncMock()
        client.get = AsyncMock(return_value=None)
        client.mget = AsyncMock()
        client.type = AsyncMock(return_value=b"zset")
        client.zrange = AsyncMock(
            return_value=[
                kombu_dumps([1, "first", "SUCCESS", 1]).encode(),
                kombu_dumps([1, "second", "SUCCESS", 2]).encode(),
            ],
        )
        client.delete = AsyncMock()
        return client

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        ("reply", "ready"),
        (
            ([1, 1, None, b"2"], False),
            ([0, 2, None, b"2"], True),
            ([1, 2, None, None], False),
            ([1, 2, b"1", b"2"], False),
            ([1, 2, None, b"2"], True),
            ([1, 2, b"1", b"1"], True),
        ),
    )
    async def test_shared_size_adjustments_and_duplicate_members(
        self,
        pipe: MagicMock,
        client: MagicMock,
        reply: list[Any],
        *,
        ready: bool,
    ) -> None:
        backend = RedisResultBackend("redis://localhost")
        backend._client = client
        pipe.execute.return_value = reply

        results = await backend.record_chord_part(
            "group-id",
            0,
            build_success("first", 1),
        )

        journal, members = pipe.zadd.call_args.args
        assert journal == f"{GROUP_PREFIX}group-id.j"
        encoded, score = next(iter(members.items()))
        assert kombu_loads(encoded) == [1, "first", "SUCCESS", 1]
        assert score == 0
        if ready:
            assert results == [
                ChordResult("first", "SUCCESS", 1),
                ChordResult("second", "SUCCESS", 2),
            ]
        else:
            assert results is None
            client.get.assert_not_awaited()
            client.zrange.assert_not_awaited()
        assert pipe.expire.call_count == 3

    @pytest.mark.asyncio()
    async def test_retry_reads_a_ready_group_after_an_interrupted_join(
        self,
        pipe: MagicMock,
        client: MagicMock,
    ) -> None:
        backend = RedisResultBackend("redis://localhost")
        backend._client = client
        pipe.execute.side_effect = [[1, 2, None, b"2"], [0, 2, None, b"2"]]
        entries = client.zrange.return_value
        client.zrange.side_effect = [OSError("read interrupted"), entries]
        result = build_success("first", 1)

        with pytest.raises(OSError, match="read interrupted"):
            await backend.record_chord_part("group-id", 0, result)
        assert await backend.record_chord_part("group-id", 0, result) == [
            ChordResult("first", "SUCCESS", 1),
            ChordResult("second", "SUCCESS", 2),
        ]

    @pytest.mark.asyncio()
    async def test_unordered_journals_are_explicit(
        self,
        client: MagicMock,
    ) -> None:
        backend = RedisResultBackend("redis://localhost")
        backend._client = client
        client.type.return_value = b"list"
        with pytest.raises(SetupError, match="ordered"):
            await backend.record_chord_part("group-id", 0, build_success("first", 1))
        client.pipeline.assert_not_called()

    @pytest.mark.asyncio()
    async def test_saved_nested_groups_are_explicit(
        self,
        pipe: MagicMock,
        client: MagicMock,
    ) -> None:
        backend = RedisResultBackend("redis://localhost")
        backend._client = client
        pipe.execute.return_value = [1, 2, None, b"2"]
        with Celery("nested-group", backend="cache+memory://") as app:
            nested = app.GroupResult("nested", [app.AsyncResult("first")])
            # celery-types only admits AsyncResult children; Celery also supports groups.
            group = app.GroupResult("group-id", [nested])  # type: ignore[list-item]
            client.get.return_value = kombu_dumps({"result": group.as_tuple()}).encode()

        with pytest.raises(SetupError, match="nested"):
            await backend.record_chord_part("group-id", 0, build_success("first", 1))
        client.mget.assert_not_awaited()
        client.zrange.assert_not_awaited()

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("status", ("SUCCESS", "FAILURE", "REVOKED"))
    async def test_saved_group_reads_task_results_in_saved_order(
        self,
        pipe: MagicMock,
        client: MagicMock,
        saved_group: bytes,
        status: str,
    ) -> None:
        backend = RedisResultBackend("redis://localhost")
        backend._client = client
        client.get.return_value = saved_group
        pipe.execute.return_value = [1, 2, None, b"2"]
        # Task metadata, rather than the older journal value, supplies the result.
        second = {**build_success("second", 20), "status": status}
        client.mget.return_value = [
            kombu_dumps(second).encode(),
            kombu_dumps(build_success("first", 10)).encode(),
        ]

        assert await backend.record_chord_part(
            "group-id",
            0,
            build_success("first", 1),
        ) == [
            ChordResult("second", status, 20),
            ChordResult("first", "SUCCESS", 10),
        ]
        client.mget.assert_awaited_once_with([
            backend.key_for("second"),
            backend.key_for("first"),
        ])
        client.zrange.assert_not_awaited()

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("completes", (False, True))
    async def test_saved_group_waits_for_terminal_task_metadata(
        self,
        pipe: MagicMock,
        client: MagicMock,
        saved_group: bytes,
        *,
        completes: bool,
    ) -> None:
        backend = RedisResultBackend(
            "redis://localhost",
            poll_interval=0.001,
            chord_join_timeout=1 if completes else 0.02,
        )
        backend._client = client
        client.get.return_value = saved_group
        pipe.execute.return_value = [1, 2, None, b"2"]
        pending = [kombu_dumps({"status": "RETRY", "result": None}).encode(), None]
        ready = [
            kombu_dumps(build_success("second", 2)).encode(),
            kombu_dumps(build_success("first", 1)).encode(),
        ]
        client.mget.return_value = pending
        if completes:
            client.mget.side_effect = [pending, ready]
            assert await backend.record_chord_part(
                "group-id",
                0,
                build_success("first", 1),
            ) == [ChordResult("second", "SUCCESS", 2), ChordResult("first", "SUCCESS", 1)]
        else:
            with pytest.raises(ChordJoinError, match="group-id"):
                await backend.record_chord_part("group-id", 0, build_success("first", 1))

        client.zrange.assert_not_awaited()

    @pytest.mark.asyncio()
    async def test_saved_group_retains_ready_results_while_waiting(
        self,
        pipe: MagicMock,
        client: MagicMock,
        saved_group: bytes,
    ) -> None:
        backend = RedisResultBackend("redis://localhost", poll_interval=0.001)
        backend._client = client
        client.get.return_value = saved_group
        pipe.execute.return_value = [1, 2, None, b"2"]
        client.mget.side_effect = [
            [kombu_dumps(build_success("second", 2)).encode(), None],
            [kombu_dumps(build_success("first", 1)).encode()],
        ]

        assert await backend.record_chord_part(
            "group-id",
            0,
            build_success("first", 1),
        ) == [
            ChordResult("second", "SUCCESS", 2),
            ChordResult("first", "SUCCESS", 1),
        ]
        assert [call.args[0] for call in client.mget.await_args_list] == [
            [backend.key_for("second"), backend.key_for("first")],
            [backend.key_for("first")],
        ]
