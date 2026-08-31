"""The result backend: which one a url names, and how Redis stores meta."""

import json
from functools import partial
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import anyio
import pytest
from faststream.exceptions import IncorrectState
from faststream.message.source_type import SourceType
from pydantic import BaseModel

from faststream_celery import CeleryBroker, CeleryTask, TestCeleryBroker
from faststream_celery.backend import InMemoryResultBackend, make_result_backend
from faststream_celery.backend.redis import (
    DEFAULT_EXPIRES,
    KEY_PREFIX,
    RedisResultBackend,
)
from faststream_celery.message import CeleryMessage, local_message, run_inline
from faststream_celery.middlewares import CeleryResultMiddleware
from faststream_celery.schemas.constants import CONTENT_TYPE
from faststream_celery.schemas.result import build_success

# Never connected to: the backend a fake-mode broker is given is in-memory.
REDIS_URL = "redis://localhost:6379/0"

REQUEST_TIMEOUT = 5.0


class _Report(BaseModel):
    """A handler result that is not JSON by itself."""

    rows: int


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
    """Every outcome is recorded, whichever backend is configured.

    In fake mode the configured backend is swapped for an
    `InMemoryResultBackend` (nothing is connected), so these read what the
    broker recorded out of that one.
    """

    @pytest.mark.asyncio()
    async def test_a_successful_handler_is_recorded(self, queue: str) -> None:
        broker = CeleryBroker(result_backend=REDIS_URL)

        @broker.subscriber(queue, task="proj.tasks.add")
        async def handler() -> int:
            return 3

        async with TestCeleryBroker(broker):
            await broker.publish(
                CeleryTask("proj.tasks.add"),
                queue=queue,
                correlation_id="task-id-1",
            )

            recorded = in_memory_backend(broker).results["task-id-1"]

        assert recorded["status"] == "SUCCESS"
        assert recorded["result"] == 3

    @pytest.mark.asyncio()
    async def test_a_failing_handler_is_recorded(self, queue: str) -> None:
        broker = CeleryBroker(result_backend=REDIS_URL)

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

            recorded = in_memory_backend(broker).results["task-id-1"]

        failure = cast("dict[str, Any]", recorded["result"])

        assert recorded["status"] == "FAILURE"
        assert failure["exc_type"] == "ValueError"
        assert "ValueError: boom" in str(recorded["traceback"])

    @pytest.mark.asyncio()
    async def test_ignore_result_records_nothing(self, queue: str) -> None:
        broker = CeleryBroker(result_backend=REDIS_URL)

        @broker.subscriber(queue, task="proj.tasks.add")
        async def handler() -> int:
            return 3

        async with TestCeleryBroker(broker):
            await broker.publish(
                CeleryTask("proj.tasks.add"),
                queue=queue,
                headers={"ignore_result": True},
            )

            assert in_memory_backend(broker).results == {}


class TestTheTestBrokerFakesTheBackend:
    """`TestCeleryBroker` keeps a backend-configured broker working offline.

    Without the swap, `CeleryResultMiddleware` would report an outcome to a
    Redis backend that fake mode never connected, and every publish would fail
    with `IncorrectState`.
    """

    @pytest.mark.asyncio()
    async def test_a_configured_backend_is_replaced(self, queue: str) -> None:
        broker = CeleryBroker(result_backend=REDIS_URL)

        @broker.subscriber(queue, task="proj.tasks.add")
        async def handler() -> int:
            return 3

        async with TestCeleryBroker(broker):
            await broker.publish(CeleryTask("proj.tasks.add"), queue=queue)

            assert isinstance(
                broker.config.broker_config.result_backend,
                InMemoryResultBackend,
            )

    @pytest.mark.asyncio()
    async def test_the_configured_backend_is_restored(self, queue: str) -> None:
        broker = CeleryBroker(result_backend=REDIS_URL)
        original = broker.config.broker_config.result_backend

        @broker.subscriber(queue, task="proj.tasks.add")
        async def handler() -> int:
            return 3

        async with TestCeleryBroker(broker):
            pass

        assert broker.config.broker_config.result_backend is original

    @pytest.mark.asyncio()
    async def test_a_broker_without_a_backend_keeps_having_none(
        self,
        queue: str,
    ) -> None:
        """`request()` must stay on its reply path, which the fake answers."""
        broker = CeleryBroker()

        @broker.subscriber(queue, task="proj.tasks.add")
        async def handler() -> int:
            return 3

        async with TestCeleryBroker(broker):
            assert broker.config.broker_config.result_backend is None

            response = await broker.request(CeleryTask("proj.tasks.add"), queue=queue)

            # The handler's return value, not a result envelope.
            assert await response.decode() == 3

    @pytest.mark.asyncio()
    async def test_request_reads_the_result_out_of_the_backend(
        self,
        queue: str,
    ) -> None:
        """With a backend, `request()` waits for the recorded envelope."""
        broker = CeleryBroker(result_backend=REDIS_URL)

        @broker.subscriber(queue, task="proj.tasks.add")
        async def handler() -> int:
            return 3

        async with TestCeleryBroker(broker):
            response = await broker.request(
                CeleryTask("proj.tasks.add"),
                queue=queue,
                correlation_id="task-id-1",
                timeout=REQUEST_TIMEOUT,
            )
            envelope = await response.decode()

        assert isinstance(envelope, dict)
        assert envelope["task_id"] == "task-id-1"
        assert envelope["status"] == "SUCCESS"
        assert envelope["result"] == 3


class TestInMemoryBackend:
    @pytest.mark.asyncio()
    async def test_a_stored_result_is_loaded_back(self) -> None:
        backend = InMemoryResultBackend()
        envelope = build_success("task-id-1", 3)

        await backend.store("task-id-1", envelope)

        assert await backend.load("task-id-1") == envelope

    @pytest.mark.asyncio()
    async def test_an_unknown_task_loads_as_none(self) -> None:
        assert await InMemoryResultBackend().load("task-id-1") is None

    @pytest.mark.asyncio()
    async def test_wait_returns_a_result_stored_before_it(self) -> None:
        backend = InMemoryResultBackend()
        envelope = build_success("task-id-1", 3)
        await backend.store("task-id-1", envelope)

        assert await backend.wait("task-id-1", timeout=REQUEST_TIMEOUT) == envelope

    @pytest.mark.asyncio()
    async def test_wait_returns_a_result_stored_after_it(self) -> None:
        """A waiter is woken by the store, rather than polling for it."""
        backend = InMemoryResultBackend()
        envelope = build_success("task-id-1", 3)

        async with anyio.create_task_group() as tg:
            tg.start_soon(partial(backend.store, "task-id-1", envelope))

            assert await backend.wait("task-id-1", timeout=REQUEST_TIMEOUT) == envelope

    @pytest.mark.asyncio()
    async def test_wait_times_out_without_a_result(self) -> None:
        backend = InMemoryResultBackend()

        with pytest.raises(TimeoutError, match="task-id-1"):
            await backend.wait("task-id-1", timeout=0.01)

    @pytest.mark.asyncio()
    async def test_a_result_is_serialized_on_the_way_in(self) -> None:
        """As a real backend stores it: a model comes back as JSON, not itself.

        A result a Celery client could not read fails in the test, rather than
        against a live broker.
        """
        backend = InMemoryResultBackend()

        await backend.store("task-id-1", build_success("task-id-1", _Report(rows=3)))
        recorded = await backend.load("task-id-1")

        assert recorded is not None
        assert recorded["result"] == {"rows": 3}

    @pytest.mark.asyncio()
    async def test_disconnect_forgets_everything(self) -> None:
        backend = InMemoryResultBackend()
        await backend.store("task-id-1", build_success("task-id-1", 3))

        await backend.disconnect()

        assert await backend.load("task-id-1") is None


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


def in_memory_backend(broker: CeleryBroker) -> InMemoryResultBackend:
    """The backend `TestCeleryBroker` put in place of the configured one."""
    backend = broker.config.broker_config.result_backend

    assert isinstance(backend, InMemoryResultBackend)
    return backend


async def _identity(msg: Any) -> Any:
    return msg
