"""Flat chords shared by FastStream and Celery, using Celery's Redis backend."""

import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import anyio
import pytest
import pytest_asyncio
from celery import Celery, chord
from celery.exceptions import ChordError
from redis.asyncio import Redis

from faststream_celery import CeleryBroker
from faststream_celery.annotations import CeleryMessage
from faststream_celery.backend.redis import GROUP_PREFIX, RedisResultBackend
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
            result = workflow.apply_async()
            assert await asyncio.to_thread(
                result.get,
                timeout=CONSUME_TIMEOUT,
                follow_parents=False,
            ) == ["faststream", "celery"]

        entries = read_log(celery_worker)
        assert entries.count({"task": "tests.echo", "payload": "celery"}) == 1
        expected = ["faststream", "celery"]
        if body_executor == "faststream":
            assert observed == [expected]
            assert len(entries) == 1
        else:
            assert observed == []
            assert entries.count({"task": "tests.echo", "payload": expected}) == 1

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("last", ("faststream", "celery"))
    @pytest.mark.parametrize("failed_executor", ("faststream", "celery"))
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
            result = workflow.apply_async()
            with pytest.raises(ChordError, match="Dependency"):
                await asyncio.to_thread(
                    result.get,
                    timeout=CONSUME_TIMEOUT,
                    follow_parents=False,
                )
            await asyncio.wait_for(handled.wait(), timeout=CONSUME_TIMEOUT)
            assert error_ids == [result.id]
            assert result.state == "FAILURE"

        assert len(read_log(celery_worker)) == 1
