"""The result backend: which one a url names, and how Redis stores meta."""

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from faststream.exceptions import IncorrectState
from faststream.message.source_type import SourceType

from faststream_celery import CeleryBroker, CeleryTask, TestCeleryBroker
from faststream_celery.backend import make_result_backend
from faststream_celery.backend.redis import (
    DEFAULT_EXPIRES,
    KEY_PREFIX,
    RedisResultBackend,
)
from faststream_celery.message import CeleryMessage, local_message, run_inline
from faststream_celery.middlewares import CeleryResultMiddleware
from faststream_celery.schemas.constants import CONTENT_TYPE
from faststream_celery.schemas.result import build_success


class TestFactory:
    def test_no_url_means_no_backend(self) -> None:
        assert make_result_backend(None) is None
        assert make_result_backend("") is None

    def test_rpc_stays_on_the_reply_queue(self) -> None:
        """`rpc://` is Celery's name for what `request()` already does."""
        assert make_result_backend("rpc://") is None

    @pytest.mark.parametrize(
        "url",
        ("redis://localhost:6379/0", "rediss://localhost:6379/0"),
    )
    def test_a_redis_url_builds_the_redis_backend(self, url: str) -> None:
        backend = make_result_backend(url)

        assert isinstance(backend, RedisResultBackend)
        assert backend.url == url

    @pytest.mark.parametrize(
        "url",
        ("amqp://localhost", "db+postgresql://x", "cache+memcached://x"),
    )
    def test_an_unsupported_backend_is_refused(self, url: str) -> None:
        with pytest.raises(NotImplementedError, match="result backend"):
            make_result_backend(url)

    def test_the_broker_accepts_a_backend_url(self) -> None:
        broker = CeleryBroker(result_backend="redis://localhost:6379/0")

        assert isinstance(
            broker.config.broker_config.result_backend,
            RedisResultBackend,
        )

    def test_no_backend_by_default(self) -> None:
        assert CeleryBroker().config.broker_config.result_backend is None


class TestRedisBackend:
    @pytest.fixture()
    def pipe(self) -> MagicMock:
        made = MagicMock()
        made.execute = AsyncMock()
        return made

    @pytest.fixture()
    def client(self, pipe: MagicMock) -> MagicMock:
        client = MagicMock()
        client.pipeline.return_value.__aenter__ = AsyncMock(return_value=pipe)
        client.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)
        client.get = AsyncMock(return_value=None)
        client.aclose = AsyncMock()
        return client

    @pytest.fixture()
    def backend(self, client: MagicMock) -> RedisResultBackend:
        made = RedisResultBackend("redis://localhost:6379/0", poll_interval=0.01)
        made._client = client
        return made

    def test_the_key_matches_celery(self, backend: RedisResultBackend) -> None:
        assert backend.key_for("task-id-1") == "celery-task-meta-task-id-1"
        assert KEY_PREFIX == "celery-task-meta-"

    @pytest.mark.asyncio()
    async def test_store_sets_publishes_and_expires(
        self,
        backend: RedisResultBackend,
        pipe: MagicMock,
    ) -> None:
        """A Celery client waiting on `.get()` is woken by the publish."""
        envelope = build_success("task-id-1", 3)

        await backend.store("task-id-1", envelope)

        key = "celery-task-meta-task-id-1"

        (set_key, payload), set_kwargs = pipe.set.call_args
        assert set_key == key
        assert set_kwargs["ex"] == DEFAULT_EXPIRES
        assert json.loads(payload)["result"] == 3

        pipe.publish.assert_called_once()
        (published_key, _), _ = pipe.publish.call_args
        assert published_key == key

    @pytest.mark.asyncio()
    async def test_store_without_expiry_sets_no_ttl(
        self,
        client: MagicMock,
        pipe: MagicMock,
    ) -> None:
        backend = RedisResultBackend("redis://localhost", expires=0)
        backend._client = client

        await backend.store("task-id-1", build_success("task-id-1", 3))

        _, set_kwargs = pipe.set.call_args
        assert set_kwargs["ex"] is None

    @pytest.mark.asyncio()
    async def test_load_returns_none_while_pending(
        self,
        backend: RedisResultBackend,
    ) -> None:
        assert await backend.load("task-id-1") is None

    @pytest.mark.asyncio()
    async def test_load_decodes_the_stored_meta(
        self,
        backend: RedisResultBackend,
        client: MagicMock,
    ) -> None:
        envelope = build_success("task-id-1", {"value": 3})
        client.get = AsyncMock(return_value=json.dumps(envelope).encode())

        assert await backend.load("task-id-1") == envelope

    @pytest.mark.asyncio()
    async def test_wait_returns_as_soon_as_the_result_appears(
        self,
        backend: RedisResultBackend,
        client: MagicMock,
    ) -> None:
        envelope = build_success("task-id-1", 3)
        payloads = [None, None, json.dumps(envelope).encode()]
        client.get = AsyncMock(side_effect=payloads)

        assert await backend.wait("task-id-1", timeout=5) == envelope

    @pytest.mark.asyncio()
    async def test_wait_times_out_with_a_clear_error(
        self,
        backend: RedisResultBackend,
    ) -> None:
        with pytest.raises(TimeoutError, match="No Celery result"):
            await backend.wait("task-id-1", timeout=0.05)

    @pytest.mark.asyncio()
    async def test_using_it_before_connecting_is_refused(self) -> None:
        backend = RedisResultBackend("redis://localhost")

        with pytest.raises(IncorrectState, match="connect the broker"):
            await backend.load("task-id-1")

    @pytest.mark.asyncio()
    async def test_disconnect_closes_the_client(
        self,
        backend: RedisResultBackend,
        client: MagicMock,
    ) -> None:
        await backend.disconnect()

        client.aclose.assert_awaited_once()
        assert backend._client is None


class TestBrokerReportsToTheBackend:
    @pytest.fixture()
    def backend(self) -> Any:
        stored: dict[str, Any] = {}
        fake = MagicMock()
        fake.connect = AsyncMock()
        fake.disconnect = AsyncMock()
        fake.store = AsyncMock(
            side_effect=lambda task_id, result: stored.update(
                {task_id: result},
            ),
        )
        fake.stored = stored
        return fake

    @pytest.mark.asyncio()
    async def test_a_successful_handler_is_recorded(
        self,
        backend: Any,
        queue: str,
    ) -> None:
        broker = CeleryBroker()
        broker.config.broker_config.result_backend = backend

        @broker.subscriber(queue, task="proj.tasks.add")
        async def handler() -> int:
            return 3

        async with TestCeleryBroker(broker):
            await broker.publish(
                CeleryTask("proj.tasks.add"),
                queue=queue,
                correlation_id="task-id-1",
            )

        assert backend.stored["task-id-1"]["status"] == "SUCCESS"
        assert backend.stored["task-id-1"]["result"] == 3

    @pytest.mark.asyncio()
    async def test_a_failing_handler_is_recorded(
        self,
        backend: Any,
        queue: str,
    ) -> None:
        broker = CeleryBroker()
        broker.config.broker_config.result_backend = backend

        @broker.subscriber(queue, task="proj.tasks.add")
        async def handler() -> None:
            msg = "boom"
            raise ValueError(msg)

        async with TestCeleryBroker(broker):
            with pytest.raises(ValueError, match="boom"):
                await broker.publish(
                    CeleryTask("proj.tasks.add"),
                    queue=queue,
                    correlation_id="task-id-1",
                )

        recorded = backend.stored["task-id-1"]
        assert recorded["status"] == "FAILURE"
        assert recorded["result"]["exc_type"] == "ValueError"
        assert "ValueError: boom" in recorded["traceback"]

    @pytest.mark.asyncio()
    async def test_ignore_result_records_nothing(
        self,
        backend: Any,
        queue: str,
    ) -> None:
        broker = CeleryBroker()
        broker.config.broker_config.result_backend = backend

        @broker.subscriber(queue, task="proj.tasks.add")
        async def handler() -> int:
            return 3

        async with TestCeleryBroker(broker):
            await broker.publish(
                CeleryTask("proj.tasks.add"),
                queue=queue,
                headers={"ignore_result": True},
            )

        assert backend.stored == {}


class TestReplyMessagesAreNotResults:
    """A reply flowing back to `request()` runs the middleware stack too."""

    @pytest.mark.asyncio()
    async def test_a_reply_is_not_recorded_as_an_outcome(self) -> None:
        stored: dict[str, Any] = {}
        backend = MagicMock()
        backend.store = AsyncMock(
            side_effect=lambda task_id, result: stored.update({task_id: result}),
        )

        broker = CeleryBroker()
        broker.config.broker_config.result_backend = backend

        middleware = CeleryResultMiddleware(
            None,
            context=broker.config.fd_config.context,
            config=broker.config.broker_config,
        )

        reply = CeleryMessage(
            raw_message=local_message(
                b'{"status": "SUCCESS"}',
                content_type=CONTENT_TYPE,
                correlation_id="task-id-1",
            ),
            body=b'{"status": "SUCCESS"}',
            ack_executor=run_inline,
            correlation_id="task-id-1",
            source_type=SourceType.RESPONSE,
        )

        await middleware.consume_scope(_identity, reply)

        assert stored == {}


async def _identity(msg: Any) -> Any:
    return msg
