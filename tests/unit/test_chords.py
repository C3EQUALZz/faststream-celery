"""Chord dispatch boundaries and the Redis protocol used with Celery."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from faststream.exceptions import SetupError
from kombu.utils.json import (
    dumps as kombu_dumps,
    loads as kombu_loads,
)

from faststream_celery import CeleryTask, signature
from faststream_celery.backend.proto import ChordResult
from faststream_celery.backend.redis import GROUP_PREFIX, RedisResultBackend
from faststream_celery.canvas import CanvasDispatcher
from faststream_celery.message import CeleryMessage
from faststream_celery.schemas.result import build_failure, build_success
from faststream_celery.schemas.task import as_signature, build_task_envelope
from tests.helpers import task_message


@pytest.fixture()
def message() -> CeleryMessage:
    raw = task_message("part", task_id="part-id")
    return CeleryMessage(
        raw_message=raw,
        body=b"",
        headers={"id": "part-id", "group": "group-id", "group_index": 0},
        correlation_id="part-id",
        message_id="part-id",
    )


class TestChordDispatch:
    @pytest.mark.asyncio()
    @pytest.mark.parametrize("immutable", (False, True))
    async def test_callback_receives_ordered_values(
        self,
        message: CeleryMessage,
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

        await dispatcher.on_chord_part(message, callback, result, queue="parts")

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
    ) -> None:
        producer = AsyncMock()
        backend = AsyncMock(spec=RedisResultBackend)
        backend.record_chord_part.return_value = None

        await CanvasDispatcher(producer, backend).on_chord_part(
            message,
            signature("body", options={"task_id": "body-id"}),
            build_success("part-id", 1),
            queue="parts",
        )

        producer.publish.assert_not_awaited()
        backend.clear_chord.assert_not_awaited()

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("publish_error", (False, True))
    async def test_failure_finishes_the_body_and_runs_its_errback(
        self,
        message: CeleryMessage,
        *,
        publish_error: bool,
    ) -> None:
        producer = AsyncMock()
        backend = AsyncMock(spec=RedisResultBackend)
        failure = build_failure("part-id", ValueError("failed"))
        message.headers["root_id"] = "part-root"
        backend.record_chord_part.return_value = [
            ChordResult(
                "part-id",
                "SUCCESS" if publish_error else "FAILURE",
                1 if publish_error else failure["result"],
            ),
        ]
        if publish_error:
            producer.publish.side_effect = [OSError("offline"), None]
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

        await CanvasDispatcher(producer, backend).on_chord_part(
            message,
            callback,
            failure,
            queue="parts",
        )

        assert backend.store.await_count == 2
        body_id, body_failure = backend.store.await_args_list[0].args
        assert body_id == "body-id"
        assert body_failure["result"]["exc_type"] == "ChordError"
        assert body_failure["result"]["exc_module"] == "celery.exceptions"
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
    ) -> None:
        with pytest.raises(SetupError, match="Redis result backend"):
            await CanvasDispatcher(AsyncMock()).on_chord_part(
                message,
                signature("body", options={"task_id": "body-id"}),
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
    def pipe(self) -> MagicMock:
        pipe = MagicMock()
        pipe.execute = AsyncMock()
        return pipe

    @pytest.fixture()
    def client(self, pipe: MagicMock) -> MagicMock:
        client = MagicMock()
        client.pipeline.return_value.__aenter__ = AsyncMock(return_value=pipe)
        client.pipeline.return_value.__aexit__ = AsyncMock()
        client.exists = AsyncMock(return_value=False)
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
            ([0, 2, None, b"2"], False),
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
            client.zrange.assert_not_awaited()
        assert pipe.expire.call_count == 3

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("nested", (False, True))
    async def test_unsupported_redis_group_shapes_are_explicit(
        self,
        client: MagicMock,
        *,
        nested: bool,
    ) -> None:
        backend = RedisResultBackend("redis://localhost")
        backend._client = client
        client.exists.return_value = nested
        client.type.return_value = b"list"
        with pytest.raises(SetupError, match="saved" if nested else "ordered"):
            await backend.record_chord_part("group-id", 0, build_success("first", 1))
        client.pipeline.assert_not_called()
