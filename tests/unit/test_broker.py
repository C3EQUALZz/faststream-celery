import logging
from unittest.mock import AsyncMock, patch

import pytest
from faststream.exceptions import SetupError
from faststream.response import PublishType

from faststream_celery import CeleryBroker, CeleryRouter, CeleryTask
from faststream_celery._internal import ContextRepo
from faststream_celery.broker import CeleryRoute
from faststream_celery.broker.logging import CeleryParamsStorage
from faststream_celery.response import CeleryPublishCommand
from faststream_celery.subscriber import CelerySubscriber


def test_broker_constructs_with_defaults() -> None:
    broker = CeleryBroker()

    assert broker.config.max_workers == 1
    assert broker.config.prefetch_count is None


def test_broker_constructs_with_url_and_options() -> None:
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


def test_the_default_broker_logs() -> None:
    """No `logger=` means the broker's own logger, as in every FastStream broker.

    Passing `None` through instead of the `EMPTY` sentinel would install
    `EmptyLoggerStorage`: no subscriber lines, no message lines, and a `Logger`
    annotation resolving to `None` inside every handler.
    """
    storage = CeleryBroker().config.logger.params_storage

    assert isinstance(storage, CeleryParamsStorage)


def test_logging_can_be_turned_off() -> None:
    """`logger=None` is how FastStream spells "no logging at all"."""
    storage = CeleryBroker(logger=None).config.logger.params_storage

    assert not isinstance(storage, CeleryParamsStorage)
    assert storage.get_logger(context=ContextRepo()) is None


def test_a_custom_logger_is_used_as_given() -> None:
    logger = logging.getLogger("custom")

    storage = CeleryBroker(logger=logger).config.logger.params_storage

    assert storage.get_logger(context=ContextRepo()) is logger


def test_subscriber_prefetch_defaults_to_max_workers() -> None:
    broker = CeleryBroker(max_workers=3)

    subscriber = broker.subscriber("celery")

    assert subscriber.config.prefetch_count == 3


def test_subscriber_prefetch_override() -> None:
    broker = CeleryBroker(max_workers=3, prefetch_count=10)

    subscriber = broker.subscriber("celery", prefetch_count=5)

    assert subscriber.config.prefetch_count == 5


def test_include_router_rejects_foreign_router() -> None:
    broker = CeleryBroker()

    with pytest.raises(SetupError, match="CeleryRegistrator"):
        broker.include_router(object())  # type: ignore[arg-type]


def test_router_prefix_is_applied_to_subscriber_queue() -> None:
    async def handler() -> None: ...

    router = CeleryRouter(
        handlers=(CeleryRoute(handler, "celery", task="proj.tasks.add"),),
    )
    broker = CeleryBroker()

    broker.include_router(router, prefix="test.")

    (subscriber,) = broker.subscribers
    assert isinstance(subscriber, CelerySubscriber)
    assert subscriber.queue == "test.celery"


def test_router_registration() -> None:
    router = CeleryRouter()
    router.subscriber("celery", task="proj.tasks.add")

    broker = CeleryBroker(routers=(router,))

    assert len(broker.subscribers) == 1


@pytest.mark.asyncio()
async def test_broker_publish_builds_task_command() -> None:
    broker = CeleryBroker()

    with patch.object(broker.config.producer, "publish", new=AsyncMock()) as publish:
        await broker.publish(
            CeleryTask("proj.tasks.add", args=[1, 2]),
            queue="celery",
            correlation_id="task-id-1",
        )

    publish.assert_called_once()
    (cmd,), _ = publish.call_args
    assert isinstance(cmd, CeleryPublishCommand)
    assert isinstance(cmd.body, CeleryTask)
    assert cmd.body.task == "proj.tasks.add"
    assert cmd.queue == "celery"
    assert cmd.correlation_id == "task-id-1"
    assert cmd.publish_type is PublishType.PUBLISH


@pytest.mark.asyncio()
async def test_broker_publish_generates_correlation_id() -> None:
    broker = CeleryBroker()

    with patch.object(broker.config.producer, "publish", new=AsyncMock()) as publish:
        await broker.publish(CeleryTask("proj.tasks.add"), queue="celery")

    (cmd,), _ = publish.call_args
    assert cmd.correlation_id


@pytest.mark.asyncio()
async def test_publisher_publishes_to_its_queue() -> None:
    broker = CeleryBroker()
    publisher = broker.publisher("celery")

    with patch.object(broker.config.producer, "publish", new=AsyncMock()) as publish:
        await publisher.publish(CeleryTask("proj.tasks.add"))

    (cmd,), _ = publish.call_args
    assert isinstance(cmd, CeleryPublishCommand)
    assert cmd.queue == "celery"


@pytest.mark.asyncio()
async def test_publisher_merges_headers() -> None:
    broker = CeleryBroker()
    publisher = broker.publisher("celery", headers={"base": "1"})

    with patch.object(broker.config.producer, "publish", new=AsyncMock()) as publish:
        await publisher.publish(CeleryTask("proj.tasks.add"), headers={"extra": "2"})

    (cmd,), _ = publish.call_args
    assert cmd.headers == {"base": "1", "extra": "2"}


@pytest.mark.asyncio()
async def test_publish_response_goes_to_reply_queue() -> None:
    """The reply flow uses the publisher's ``_publish`` (subscriber flow)."""
    broker = CeleryBroker()
    publisher = broker.publisher("celery")

    response_cmd = CeleryPublishCommand(
        {"result": 1},
        queue="reply-queue",
        exchange="",
        declare=False,
        _publish_type=PublishType.REPLY,
    )

    with patch.object(broker.config.producer, "publish", new=AsyncMock()) as publish:
        await publisher._publish(response_cmd, _extra_middlewares=())

    (cmd,), _ = publish.call_args
    assert cmd.exchange == ""
    assert cmd.reply_to == ""
