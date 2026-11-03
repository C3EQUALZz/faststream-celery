"""Flat chords shared by FastStream and Celery, using Celery's Redis backend."""

import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any
from unittest.mock import DEFAULT, Mock

import anyio
import pytest
import pytest_asyncio
from celery import Celery, chain, chord
from celery.exceptions import ChordError
from celery.result import AsyncResult, GroupResult, ResultBase
from faststream.middlewares import AckPolicy
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from faststream_celery import CeleryBroker
from faststream_celery.annotations import CeleryMessage
from faststream_celery.backend.redis import GROUP_PREFIX, RedisResultBackend
from faststream_celery.parser import read_embed
from faststream_celery.schemas.result import build_success
from tests.helpers import running
from tests.integration.base import CONSUME_TIMEOUT, COUNTDOWN
from tests.integration.celery_tasks import read_log


async def wait_for_part(store: Redis, group_id: str) -> None:
    """Hold our part until Celery has recorded its completion."""
    with anyio.fail_after(CONSUME_TIMEOUT):
        # The Celery subprocess exposes its completion through Redis, not an Event.
        while await store.zcard(f"{GROUP_PREFIX}{group_id}.j") != 1:  # ruff: ignore[async-busy-wait]
            await asyncio.sleep(0.01)


class RedisChordTestcase:
    @pytest.fixture()
    def result_backend(self, redis_url: str) -> str:
        return redis_url

    @pytest_asyncio.fixture()
    async def chord_store(self, redis_url: str) -> AsyncGenerator[Redis]:
        async with Redis.from_url(redis_url) as client:
            yield client

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("last", ("faststream", "celery"))
    @pytest.mark.parametrize("body_executor", ("faststream", "celery"))
    @pytest.mark.parametrize("saved", (False, True))
    async def test_mixed_chord_success(  # ruff: ignore[too-many-arguments]
        self,
        broker_url: str,
        result_backend: str,
        celery_app: Celery,
        queue: str,
        our_queue: str,
        *,
        chord_store: Redis,
        celery_worker: Path,
        last: str,
        body_executor: str,
        saved: bool,
    ) -> None:
        broker = CeleryBroker(broker_url, result_backend=result_backend)
        observed: list[list[str]] = []

        @broker.subscriber(our_queue, task="tests.chord_part")
        async def part(message: CeleryMessage) -> str:
            group_id = message.headers["group"]
            if last == "faststream":
                await wait_for_part(chord_store, group_id)
            else:
                backend = broker.config.broker_config.result_backend
                assert isinstance(backend, RedisResultBackend)
                result = build_success(message.headers["id"], "faststream")
                assert await backend.record_chord_part(group_id, 0, result) is None
                assert await backend.record_chord_part(group_id, 0, result) is None
                assert await chord_store.zcard(f"{GROUP_PREFIX}{group_id}.j") == 1
            return "faststream"

        @broker.subscriber(our_queue, task="tests.chord_body")
        async def body(args: list[list[str]], kwargs: dict[str, Any]) -> list[str]:
            observed.append(args[0])
            return args[0]

        callback = celery_app.signature(
            "tests.chord_body" if body_executor == "faststream" else "tests.echo",
            queue=our_queue if body_executor == "faststream" else queue,
        )
        workflow = chord(
            [
                celery_app.signature("tests.chord_part", queue=our_queue),
                celery_app.signature(
                    "tests.echo",
                    args=["celery"],
                    queue=queue,
                    countdown=COUNTDOWN if last == "celery" else 0,
                ),
            ],
            callback,
        )

        async with running(broker):
            if saved:
                frozen = workflow.freeze()
                header = frozen.parent
                assert isinstance(header, GroupResult)
                assert header.results is not None
                # Saved order is authoritative even when journal scores disagree.
                GroupResult(
                    header.id,
                    list(reversed(header.results)),
                    app=celery_app,
                ).save()
            result = workflow.apply_async()
            expected = ["celery", "faststream"] if saved else ["faststream", "celery"]
            assert (
                await asyncio.to_thread(
                    result.get,
                    timeout=CONSUME_TIMEOUT,
                    follow_parents=False,
                )
                == expected
            )
            if saved:
                assert isinstance(header, GroupResult)
                assert await chord_store.exists(f"{GROUP_PREFIX}{header.id}")

        entries = read_log(celery_worker)
        assert entries.count({"task": "tests.echo", "payload": "celery"}) == 1
        if body_executor == "faststream":
            assert observed == [expected]
            assert len(entries) == 1
        else:
            assert observed == []
            assert entries.count({"task": "tests.echo", "payload": expected}) == 1

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("last", ("faststream", "celery"))
    @pytest.mark.parametrize("failed_executor", ("faststream", "celery"))
    @pytest.mark.parametrize("saved", (False, True))
    async def test_mixed_chord_failure(  # ruff: ignore[too-many-arguments]
        self,
        broker_url: str,
        result_backend: str,
        celery_app: Celery,
        queue: str,
        our_queue: str,
        *,
        chord_store: Redis,
        celery_worker: Path,
        last: str,
        failed_executor: str,
        saved: bool,
    ) -> None:
        broker = CeleryBroker(broker_url, result_backend=result_backend)
        error_ids: list[str] = []
        handled = asyncio.Event()

        @broker.subscriber(our_queue, task="tests.chord_part")
        async def part(message: CeleryMessage) -> str:
            if last == "faststream":
                await wait_for_part(chord_store, message.headers["group"])
            if failed_executor == "faststream":
                error = "header failed"
                raise ValueError(error)
            return "faststream"

        @broker.subscriber(our_queue, task="tests.chord_error")
        async def error_handler(args: list[str], kwargs: dict[str, Any]) -> None:
            error_ids.append(args[0])
            handled.set()

        callback = celery_app.signature(
            "tests.echo",
            queue=queue,
            link_error=[celery_app.signature("tests.chord_error", queue=our_queue)],
        )
        workflow = chord(
            [
                celery_app.signature("tests.chord_part", queue=our_queue),
                celery_app.signature(
                    "tests.fail" if failed_executor == "celery" else "tests.echo",
                    args=["header failed" if failed_executor == "celery" else "celery"],
                    queue=queue,
                    countdown=COUNTDOWN if last == "celery" else 0,
                ),
            ],
            callback,
        )

        async with running(broker):
            if saved:
                frozen = workflow.freeze()
                assert isinstance(frozen.parent, GroupResult)
                frozen.parent.save()
            result = workflow.apply_async()
            with pytest.raises(ChordError, match="header failed"):
                await asyncio.to_thread(
                    result.get,
                    timeout=CONSUME_TIMEOUT,
                    follow_parents=False,
                )
            await asyncio.wait_for(handled.wait(), timeout=CONSUME_TIMEOUT)
            assert error_ids == [result.id]
            assert result.state == "FAILURE"

        assert len(read_log(celery_worker)) == 1

    @pytest.mark.asyncio()
    @pytest.mark.usefixtures("celery_worker")
    async def test_saved_chord_missing_result_fails_body(  # ruff: ignore[too-many-arguments]
        self,
        broker_url: str,
        result_backend: str,
        celery_app: Celery,
        queue: str,
        our_queue: str,
        *,
        chord_store: Redis,
    ) -> None:
        broker = CeleryBroker(broker_url, result_backend=result_backend)
        backend = broker.config.broker_config.result_backend
        assert isinstance(backend, RedisResultBackend)
        backend.chord_join_timeout = 0.05
        handled = asyncio.Event()
        error_ids: list[str] = []

        @broker.subscriber(our_queue, task="tests.chord_part")
        async def part(message: CeleryMessage) -> str:
            await wait_for_part(chord_store, message.headers["group"])
            await chord_store.delete(backend.key_for(missing_id))
            return "faststream"

        @broker.subscriber(our_queue, task="tests.chord_error")
        async def error_handler(args: list[str], kwargs: dict[str, Any]) -> None:
            error_ids.append(args[0])
            handled.set()

        workflow = chord(
            [
                celery_app.signature("tests.chord_part", queue=our_queue),
                celery_app.signature("tests.echo", args=["celery"], queue=queue),
            ],
            celery_app.signature(
                "tests.echo",
                queue=queue,
                link_error=[celery_app.signature("tests.chord_error", queue=our_queue)],
            ),
        )
        frozen = workflow.freeze()
        header = frozen.parent
        assert isinstance(header, GroupResult)
        assert header.results is not None
        missing_id = header.results[1].id
        header.save()

        async with running(broker):
            result = workflow.apply_async()
            with pytest.raises(ChordError, match="Join error"):
                await asyncio.to_thread(
                    result.get,
                    timeout=CONSUME_TIMEOUT,
                    follow_parents=False,
                )
            await asyncio.wait_for(handled.wait(), timeout=CONSUME_TIMEOUT)
            assert error_ids == [result.id]
            assert result.state == "FAILURE"
            part_result = await backend.load(header.results[0].id)
            assert part_result is not None
            assert part_result["status"] == "SUCCESS"
            assert await chord_store.exists(f"{GROUP_PREFIX}{header.id}")

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("saved", (False, True))
    async def test_mixed_chord_recovers_after_interrupted_join(  # ruff: ignore[too-many-arguments]
        self,
        broker_url: str,
        result_backend: str,
        celery_app: Celery,
        queue: str,
        our_queue: str,
        *,
        chord_store: Redis,
        celery_worker: Path,
        monkeypatch: pytest.MonkeyPatch,
        saved: bool,
    ) -> None:
        broker = CeleryBroker(broker_url, result_backend=result_backend)
        attempts = 0
        bodies: list[list[str]] = []

        @broker.subscriber(
            our_queue,
            task="tests.chord_part",
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def part(message: CeleryMessage) -> str:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                await wait_for_part(chord_store, message.headers["group"])
            return "faststream"

        @broker.subscriber(our_queue, task="tests.chord_body")
        async def body(args: list[list[str]], kwargs: dict[str, Any]) -> list[str]:
            bodies.append(args[0])
            return args[0]

        workflow = chord(
            [
                celery_app.signature("tests.chord_part", queue=our_queue),
                celery_app.signature("tests.echo", args=["celery"], queue=queue),
            ],
            celery_app.signature("tests.chord_body", queue=our_queue),
        )

        async with running(broker):
            backend = broker.config.broker_config.result_backend
            assert isinstance(backend, RedisResultBackend)
            client = backend._client
            assert client is not None
            method = "mget" if saved else "zrange"
            interrupted_read = Mock(
                wraps=getattr(client, method),
                side_effect=[RedisConnectionError("interrupted join"), DEFAULT],
            )
            monkeypatch.setattr(client, method, interrupted_read)

            if saved:
                frozen = workflow.freeze()
                assert isinstance(frozen.parent, GroupResult)
                frozen.parent.save()
            result = workflow.apply_async()
            assert await asyncio.to_thread(
                result.get,
                timeout=CONSUME_TIMEOUT,
                follow_parents=False,
            ) == ["faststream", "celery"]
            assert interrupted_read.call_count == 2

        assert attempts == 2
        assert bodies == [["faststream", "celery"]]
        assert read_log(celery_worker) == [{"task": "tests.echo", "payload": "celery"}]

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("last", ("faststream", "celery"))
    async def test_failed_header_chain_completes_chord(  # ruff: ignore[too-many-arguments]
        self,
        broker_url: str,
        result_backend: str,
        celery_app: Celery,
        queue: str,
        our_queue: str,
        *,
        chord_store: Redis,
        celery_worker: Path,
        last: str,
    ) -> None:
        broker = CeleryBroker(broker_url, result_backend=result_backend)
        handled = asyncio.Event()
        error_ids: list[str] = []

        @broker.subscriber(our_queue, task="tests.header_fail")
        async def fail(message: CeleryMessage) -> None:
            steps = read_embed(message.raw_message.message)["chain"]
            assert steps is not None
            tail_options = steps[0]["options"]
            assert tail_options["group_index"] == 1
            if last == "faststream":
                await wait_for_part(chord_store, tail_options["group_id"])
            error = "header chain failed"
            raise ValueError(error)

        @broker.subscriber(our_queue, task="tests.chord_error")
        async def error_handler(args: list[str], kwargs: dict[str, Any]) -> None:
            error_ids.append(args[0])
            handled.set()

        workflow = chord(
            [
                celery_app.signature(
                    "tests.echo",
                    args=["celery"],
                    queue=queue,
                    countdown=COUNTDOWN if last == "celery" else 0,
                ),
                chain(
                    celery_app.signature("tests.header_fail", queue=our_queue),
                    celery_app.signature("tests.echo", queue=queue),
                    celery_app.signature("tests.echo", queue=queue),
                ),
            ],
            celery_app.signature(
                "tests.echo",
                queue=queue,
                link_error=[celery_app.signature("tests.chord_error", queue=our_queue)],
            ),
        )

        async with running(broker):
            result = workflow.apply_async()
            assert isinstance(result.parent, GroupResult)
            assert result.parent.results is not None
            tail = result.parent.results[1]
            with pytest.raises(ChordError, match=f"Dependency {tail.id} raised"):
                await asyncio.to_thread(
                    result.get,
                    timeout=CONSUME_TIMEOUT,
                    follow_parents=False,
                )
            await asyncio.wait_for(handled.wait(), timeout=CONSUME_TIMEOUT)
            assert result.state == "FAILURE"
            assert error_ids == [result.id]
            ancestor: ResultBase | None = tail
            while isinstance(ancestor, AsyncResult):
                assert ancestor.state == "FAILURE"
                ancestor = ancestor.parent

        assert read_log(celery_worker) == [{"task": "tests.echo", "payload": "celery"}]
