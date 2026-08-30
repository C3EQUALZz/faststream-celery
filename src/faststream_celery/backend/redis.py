"""The Celery Redis result backend.

Writes the same ``celery-task-meta-<id>`` keys a Celery worker writes, so a
Celery client's ``AsyncResult.get()`` reads our results without knowing who
produced them (``celery.backends.redis.RedisBackend``).
"""

import json
from typing import TYPE_CHECKING, cast

import anyio
from faststream.exceptions import IncorrectState

from faststream_celery._internal import dump_json

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
KEY_PREFIX = "celery-task-meta-"

# `result_expires`, one day.
DEFAULT_EXPIRES = 86400

DEFAULT_POLL_INTERVAL = 0.05


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
        """Poll until the task has a result, the way `AsyncResult.get()` does."""
        with anyio.move_on_after(timeout):
            while True:
                if (result := await self.load(task_id)) is not None:
                    return result

                await anyio.sleep(self.poll_interval)

        msg = f"No Celery result for task {task_id!r} within {timeout} seconds."
        raise TimeoutError(msg)

    def key_for(self, task_id: str) -> str:
        return f"{KEY_PREFIX}{task_id}"

    def _connected(self) -> Redis:
        if self._client is None:
            msg = "You should connect the broker at first."
            raise IncorrectState(msg)

        return self._client
