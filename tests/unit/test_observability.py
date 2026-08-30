"""The OpenTelemetry and Prometheus settings providers."""

import pytest
from faststream.response import PublishType
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_DESTINATION_NAME,
    MESSAGING_MESSAGE_BODY_SIZE,
    MESSAGING_MESSAGE_CONVERSATION_ID,
    MESSAGING_MESSAGE_ID,
    MESSAGING_SYSTEM,
)
from prometheus_client import CollectorRegistry

from faststream_celery import CeleryBroker, CeleryTask, TestCeleryBroker
from faststream_celery.message import CeleryMessage, ConsumerMessage
from faststream_celery.opentelemetry import (
    CeleryTelemetryMiddleware,
    CeleryTelemetrySettingsProvider,
)
from faststream_celery.opentelemetry.provider import CELERY_TASK_ID, CELERY_TASK_NAME
from faststream_celery.prometheus import (
    CeleryMetricsSettingsProvider,
    CeleryPrometheusMiddleware,
)
from faststream_celery.response import CeleryPublishCommand
from tests.helpers import raw_message, run_inline


def stream_message(
    *,
    task: str = "proj.tasks.add",
    exchange: str = "celery",
    routing_key: str = "celery",
) -> CeleryMessage:
    """A parsed message carrying the delivery info the providers read."""
    raw = raw_message(
        [[1, 2], {}, {}],
        headers={"task": task, "id": "task-id-1"},
    )
    raw.delivery_info = {"exchange": exchange, "routing_key": routing_key}

    return CeleryMessage(
        raw_message=ConsumerMessage(raw, run_inline),
        body=msg_body(),
        ack_executor=run_inline,
        headers={"task": task, "id": "task-id-1"},
        correlation_id="task-id-1",
        message_id="task-id-1",
    )


def msg_body() -> bytes:
    return b'{"args": [1, 2], "kwargs": {}}'


def publish_command(**kwargs: object) -> CeleryPublishCommand:
    return CeleryPublishCommand(
        CeleryTask("proj.tasks.add"),
        queue="celery",
        correlation_id="task-id-1",
        _publish_type=PublishType.PUBLISH,
        **kwargs,  # type: ignore[arg-type]
    )


class TestTelemetryProvider:
    @pytest.fixture()
    def provider(self) -> CeleryTelemetrySettingsProvider:
        return CeleryTelemetrySettingsProvider()

    def test_the_messaging_system_is_celery(
        self,
        provider: CeleryTelemetrySettingsProvider,
    ) -> None:
        assert provider.messaging_system == "celery"

    def test_consume_attributes_carry_the_task(
        self,
        provider: CeleryTelemetrySettingsProvider,
    ) -> None:
        attrs = provider.get_consume_attrs_from_message(stream_message())

        assert attrs[MESSAGING_SYSTEM] == "celery"
        assert attrs[MESSAGING_MESSAGE_ID] == "task-id-1"
        assert attrs[MESSAGING_MESSAGE_CONVERSATION_ID] == "task-id-1"
        assert attrs[CELERY_TASK_NAME] == "proj.tasks.add"
        assert attrs[CELERY_TASK_ID] == "task-id-1"
        assert attrs[MESSAGING_MESSAGE_BODY_SIZE] == len(msg_body())

    def test_consume_destination_joins_exchange_and_routing_key(
        self,
        provider: CeleryTelemetrySettingsProvider,
    ) -> None:
        message = stream_message(exchange="tasks", routing_key="high")

        assert provider.get_consume_destination_name(message) == "tasks.high"

    def test_a_missing_exchange_reads_as_default(
        self,
        provider: CeleryTelemetrySettingsProvider,
    ) -> None:
        message = stream_message(exchange="", routing_key="celery")

        assert provider.get_consume_destination_name(message) == "default.celery"

    def test_publish_attributes_carry_the_task(
        self,
        provider: CeleryTelemetrySettingsProvider,
    ) -> None:
        attrs = provider.get_publish_attrs_from_cmd(publish_command())

        assert attrs[MESSAGING_SYSTEM] == "celery"
        assert attrs[MESSAGING_DESTINATION_NAME] == "celery"
        assert attrs[CELERY_TASK_NAME] == "proj.tasks.add"

    def test_a_raw_payload_has_no_task_attribute(
        self,
        provider: CeleryTelemetrySettingsProvider,
    ) -> None:
        cmd = CeleryPublishCommand(
            {"payload": 1},
            queue="celery",
            _publish_type=PublishType.PUBLISH,
        )

        assert CELERY_TASK_NAME not in provider.get_publish_attrs_from_cmd(cmd)

    def test_publish_destination_follows_the_queue(
        self,
        provider: CeleryTelemetrySettingsProvider,
    ) -> None:
        assert provider.get_publish_destination_name(publish_command()) == (
            "celery.celery"
        )

    def test_publish_destination_honors_an_explicit_exchange(
        self,
        provider: CeleryTelemetrySettingsProvider,
    ) -> None:
        cmd = publish_command(exchange="tasks", routing_key="high")

        assert provider.get_publish_destination_name(cmd) == "tasks.high"

    def test_a_reply_goes_to_the_default_exchange(
        self,
        provider: CeleryTelemetrySettingsProvider,
    ) -> None:
        cmd = publish_command(exchange="")

        assert provider.get_publish_destination_name(cmd) == "default.celery"


class TestMetricsProvider:
    @pytest.fixture()
    def provider(self) -> CeleryMetricsSettingsProvider:
        return CeleryMetricsSettingsProvider()

    def test_the_messaging_system_is_celery(
        self,
        provider: CeleryMetricsSettingsProvider,
    ) -> None:
        assert provider.messaging_system == "celery"

    def test_consume_attributes_name_the_queue(
        self,
        provider: CeleryMetricsSettingsProvider,
    ) -> None:
        attrs = provider.get_consume_attrs_from_message(stream_message())

        assert attrs == {
            "destination_name": "celery",
            "message_size": len(msg_body()),
            "messages_count": 1,
        }

    def test_publish_destination_is_the_queue(
        self,
        provider: CeleryMetricsSettingsProvider,
    ) -> None:
        assert provider.get_publish_destination_name_from_cmd(publish_command()) == (
            "celery"
        )


class TestMiddlewaresRunEndToEnd:
    @pytest.mark.asyncio()
    async def test_telemetry_middleware_does_not_disturb_delivery(
        self,
        queue: str,
    ) -> None:
        broker = CeleryBroker(middlewares=(CeleryTelemetryMiddleware(),))

        @broker.subscriber(queue, task="proj.tasks.add")
        async def handler() -> None: ...

        async with TestCeleryBroker(broker):
            await broker.publish(CeleryTask("proj.tasks.add"), queue=queue)

            handler.mock.assert_called_once()

    @pytest.mark.asyncio()
    async def test_prometheus_middleware_records_a_message(
        self,
        queue: str,
    ) -> None:
        registry = CollectorRegistry()
        broker = CeleryBroker(
            middlewares=(CeleryPrometheusMiddleware(registry=registry),),
        )

        @broker.subscriber(queue, task="proj.tasks.add")
        async def handler() -> None: ...

        async with TestCeleryBroker(broker):
            await broker.publish(CeleryTask("proj.tasks.add"), queue=queue)

            handler.mock.assert_called_once()

        received = registry.get_sample_value(
            "faststream_received_messages_total",
            {
                "app_name": "faststream",
                "broker": "celery",
                "handler": queue,
            },
        )
        assert received == pytest.approx(1.0)
