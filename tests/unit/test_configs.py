import pytest
from faststream.exceptions import IncorrectState
from faststream.middlewares import AckPolicy
from faststream.response import PublishType

from faststream_celery import CeleryBroker, CeleryRouter
from faststream_celery.configs import CeleryRouterConfig
from faststream_celery.response import CeleryPublishCommand
from tests.helpers import MEMORY_URL, running


class TestConnectionOptions:
    def test_the_url_reaches_kombu(self) -> None:
        broker = CeleryBroker("amqp://guest:guest@example:5672/vhost")

        assert broker.config.broker_config.make_connection().hostname == "example"

    def test_transport_options_reach_kombu(self) -> None:
        broker = CeleryBroker(
            "redis://localhost:6379",
            transport_options={"visibility_timeout": 3600},
        )

        connection = broker.config.broker_config.make_connection()

        assert connection.transport_options == {"visibility_timeout": 3600}

    def test_every_call_builds_a_fresh_connection(self) -> None:
        """Each consumer thread owns its own; kombu is not thread-safe."""
        config = CeleryBroker(MEMORY_URL).config.broker_config

        assert config.make_connection() is not config.make_connection()

    @pytest.mark.parametrize(
        ("url", "expected"),
        (
            ("amqp://guest:guest@localhost:5672//", "/"),
            ("amqp://guest:guest@localhost:5672/", "/"),
            ("amqp://guest:guest@localhost:5672/myvhost", "myvhost"),
            ("redis://localhost:6379", "/"),
        ),
    )
    def test_virtual_host_is_read_from_the_url(self, url: str, expected: str) -> None:
        assert CeleryBroker(url).config.broker_config.virtual_host == expected


class TestRouterConfig:
    def test_a_router_has_no_connection_of_its_own(self) -> None:
        with pytest.raises(IncorrectState):
            CeleryRouterConfig().make_connection()


class TestDefaults:
    def test_broker_defaults(self) -> None:
        broker = CeleryBroker()

        assert broker.config.max_workers == 1
        assert broker.config.prefetch_count is None

    def test_options_are_stored(self) -> None:
        broker = CeleryBroker(
            "amqp://guest:guest@localhost:5672/",
            transport_options={"visibility_timeout": 3600},
            max_workers=4,
            prefetch_count=8,
        )

        assert broker.config.url == "amqp://guest:guest@localhost:5672/"
        assert broker.config.transport_options == {"visibility_timeout": 3600}
        assert broker.config.max_workers == 4
        assert broker.config.prefetch_count == 8

    def test_prefetch_defaults_to_max_workers(self) -> None:
        subscriber = CeleryBroker(max_workers=3).subscriber("celery")

        assert subscriber.config.prefetch_count == 3

    def test_a_subscriber_overrides_the_broker_prefetch(self) -> None:
        broker = CeleryBroker(max_workers=3, prefetch_count=10)

        assert broker.subscriber("celery", prefetch_count=5).config.prefetch_count == 5

    def test_the_broker_prefetch_beats_max_workers(self) -> None:
        broker = CeleryBroker(max_workers=3, prefetch_count=10)

        assert broker.subscriber("celery").config.prefetch_count == 10


class TestAckPolicy:
    def test_the_default_is_reject_on_error(self, broker: CeleryBroker) -> None:
        assert broker.subscriber("celery").ack_policy is AckPolicy.REJECT_ON_ERROR

    def test_the_broker_policy_is_inherited(self) -> None:
        broker = CeleryBroker(ack_policy=AckPolicy.ACK_FIRST)

        assert broker.subscriber("celery").ack_policy is AckPolicy.ACK_FIRST

    def test_a_subscriber_overrides_the_broker(self) -> None:
        broker = CeleryBroker(ack_policy=AckPolicy.ACK_FIRST)
        subscriber = broker.subscriber("celery", ack_policy=AckPolicy.MANUAL)

        assert subscriber.ack_policy is AckPolicy.MANUAL

    def test_a_router_policy_reaches_its_subscribers(self) -> None:
        router = CeleryRouter(ack_policy=AckPolicy.NACK_ON_ERROR)
        router.subscriber("celery")

        broker = CeleryBroker()
        broker.include_router(router)

        (subscriber,) = broker.subscribers
        assert subscriber.ack_policy is AckPolicy.NACK_ON_ERROR


class TestConnectionLifecycle:
    @pytest.mark.asyncio()
    async def test_ping_is_false_before_connecting(
        self,
        memory_broker: CeleryBroker,
    ) -> None:
        assert not await memory_broker.ping(timeout=1.0)

    @pytest.mark.asyncio()
    async def test_ping_is_true_once_connected(
        self,
        memory_broker: CeleryBroker,
    ) -> None:
        async with running(memory_broker):
            assert await memory_broker.ping(timeout=5.0)

    @pytest.mark.asyncio()
    async def test_ping_is_false_again_after_stopping(
        self,
        memory_broker: CeleryBroker,
    ) -> None:
        async with running(memory_broker):
            pass

        assert not await memory_broker.ping(timeout=1.0)

    @pytest.mark.asyncio()
    async def test_publishing_before_connecting_is_refused(
        self,
        broker: CeleryBroker,
    ) -> None:
        with pytest.raises(IncorrectState, match="connect the broker"):
            await broker.config.producer.publish(_any_command())

    @pytest.mark.asyncio()
    async def test_requesting_before_connecting_is_refused(
        self,
        broker: CeleryBroker,
    ) -> None:
        with pytest.raises(IncorrectState, match="connect the broker"):
            await broker.config.producer.request(_any_command())


def _any_command() -> CeleryPublishCommand:
    return CeleryPublishCommand(
        "payload",
        queue="celery",
        _publish_type=PublishType.PUBLISH,
    )
