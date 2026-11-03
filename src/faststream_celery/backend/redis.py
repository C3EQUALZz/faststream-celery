"""The Celery Redis result backend.

Writes the same ``celery-task-meta-<id>`` keys a Celery worker writes, so a
Celery client's ``AsyncResult.get()`` reads our results without knowing who
produced them (``celery.backends.redis.RedisBackend``).
"""

import json
from typing import TYPE_CHECKING, Final, cast

import anyio
from faststream.exceptions import IncorrectState, SetupError
from kombu.utils.json import (
    dumps as kombu_dumps,
    loads as kombu_loads,
)

from faststream_celery._internal import dump_json
from faststream_celery.schemas.result import READY_STATES, ChordResult

try:
    from redis.asyncio import Redis

except ImportError as exc:  # pragma: no cover - depends on the install
    msg = (
        "The Redis result backend needs redis installed. "
        'Install it with `pip install "faststream-celery[redis]"`.'
    )
    raise ImportError(msg) from exc

if TYPE_CHECKING:
    from faststream_celery.schemas.result import TaskResult

# `celery.backends.base.Backend.task_keyprefix`.
KEY_PREFIX: Final[str] = "celery-task-meta-"
GROUP_PREFIX: Final[str] = "celery-taskset-meta-"

# `result_expires`, one day.
DEFAULT_EXPIRES: Final[int] = 86400

DEFAULT_POLL_INTERVAL: Final[float] = 0.05


class RedisResultBackend:
    """Reads and writes Celery result meta in Redis."""

    def __init__(
        self,
        url: str,
        *,
        expires: int = DEFAULT_EXPIRES,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
    ) -> None:
        self.url = url
        self.expires = expires
        self.poll_interval = poll_interval

        self._client: Redis | None = None

    async def connect(self) -> None:
        if self._client is None:
            self._client = Redis.from_url(self.url)

    async def disconnect(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            await client.aclose()

    async def store(self, task_id: str, result: "TaskResult") -> None:
        """Write the result meta, mirroring `RedisBackend._set`.

        The publish is what lets a waiting Celery client wake up at once
        instead of polling the key.
        """
        client = self._connected()
        key = self.key_for(task_id)
        payload = dump_json(result)

        async with client.pipeline() as pipe:
            pipe.set(key, payload, ex=self.expires or None)

            pipe.publish(key, payload)
            await pipe.execute()

    async def load(self, task_id: str) -> "TaskResult | None":
        client = self._connected()

        payload = await client.get(self.key_for(task_id))
        if payload is None:
            return None

        return cast("TaskResult", json.loads(payload))

    async def wait(
        self,
        task_id: str,
        *,
        timeout: float,
    ) -> "TaskResult":
        """Poll until the task has a terminal result, like `AsyncResult.get()`."""
        with anyio.move_on_after(timeout):
            while True:
                result = await self.load(task_id)
                if result is not None and result["status"] in READY_STATES:
                    return result

                await anyio.sleep(self.poll_interval)

        msg = f"No Celery result for task {task_id!r} within {timeout} seconds."
        raise TimeoutError(msg)

    def key_for(self, task_id: str) -> str:
        return f"{KEY_PREFIX}{task_id}"

    async def record_chord_part(
        self,
        group_id: str,
        group_index: int | None,
        result: "TaskResult",
    ) -> list[ChordResult] | None:
        """Join Celery's default ordered JSON chord journal and size counters."""
        client = self._connected()
        group_key = f"{GROUP_PREFIX}{group_id}"
        journal, difference, size = f"{group_key}.j", f"{group_key}.t", f"{group_key}.s"

        if await client.exists(group_key):
            error = "Mixed Redis chords with saved group structures are not supported."
            raise SetupError(error)
        if await client.type(journal) not in {b"none", b"zset"}:
            error = "Mixed Redis chords require result_chord_ordered=True."
            raise SetupError(error)

        encoded = kombu_dumps([
            1,
            result["task_id"],
            result["status"],
            json.loads(dump_json(result["result"])),
        ])
        async with client.pipeline() as pipe:
            pipe.zadd(
                journal,
                {encoded: group_index if group_index is not None else float("inf")},
            )
            pipe.zcount(journal, "-inf", "+inf")
            pipe.get(difference)
            pipe.get(size)
            if self.expires:
                pipe.expire(journal, self.expires)
                pipe.expire(difference, self.expires)
                pipe.expire(size, self.expires)
            _, ready, extra, expected = (await pipe.execute())[:4]

        if not expected or ready != int(expected) + int(extra or 0):
            return None

        entries = cast("list[bytes]", await client.zrange(journal, 0, -1))
        return [ChordResult(*kombu_loads(entry)[1:]) for entry in entries]

    async def clear_chord(self, group_id: str) -> None:
        """Delete the same journal, size and adjustment keys Celery deletes."""
        key = f"{GROUP_PREFIX}{group_id}"
        await self._connected().delete(f"{key}.j", f"{key}.t", f"{key}.s")

    def _connected(self) -> Redis:
        if self._client is None:
            msg = "You should connect the broker at first."
            raise IncorrectState(msg)

        return self._client
