from typing import TYPE_CHECKING, Any, Final

from faststream.opentelemetry import TelemetrySettingsProvider

# `opentelemetry.semconv.trace.SpanAttributes` has been deprecated since
# semconv 1.25.0 in favour of the per-signal attribute modules.
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_DESTINATION_NAME,
    MESSAGING_DESTINATION_PUBLISH_NAME,
    MESSAGING_MESSAGE_BODY_SIZE,
    MESSAGING_MESSAGE_CONVERSATION_ID,
    MESSAGING_MESSAGE_ID,
    MESSAGING_SYSTEM,
)
from typing_extensions import override

from faststream_celery.message import ConsumerMessage
from faststream_celery.response import CeleryPublishCommand
from faststream_celery.schemas.task import CeleryTask

if TYPE_CHECKING:
    from faststream.message import StreamMessage

# Celery routes by task name inside a queue, so the task is the dimension
# that tells two spans on the same queue apart.
CELERY_TASK_NAME: Final[str] = "messaging.celery.task_name"
CELERY_TASK_ID: Final[str] = "messaging.celery.task_id"

DEFAULT_EXCHANGE_LABEL: Final[str] = "default"


class CeleryTelemetrySettingsProvider(
    TelemetrySettingsProvider[ConsumerMessage, CeleryPublishCommand],
):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "celery"

    @override
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[ConsumerMessage]",
    ) -> dict[str, Any]:
        return {
            MESSAGING_SYSTEM: self.messaging_system,
            MESSAGING_MESSAGE_ID: msg.message_id,
            MESSAGING_MESSAGE_CONVERSATION_ID: msg.correlation_id,
            MESSAGING_MESSAGE_BODY_SIZE: len(msg.body),
            MESSAGING_DESTINATION_PUBLISH_NAME: self.get_consume_destination_name(msg),
            CELERY_TASK_NAME: msg.headers.get("task"),
            CELERY_TASK_ID: msg.headers.get("id"),
        }

    @override
    def get_consume_destination_name(
        self,
        msg: "StreamMessage[ConsumerMessage]",
    ) -> str:
        delivery = msg.raw_message.message.delivery_info
        exchange = delivery.get("exchange") or DEFAULT_EXCHANGE_LABEL
        routing_key = delivery.get("routing_key") or ""

        return f"{exchange}.{routing_key}"

    @override
    def get_publish_attrs_from_cmd(
        self,
        cmd: CeleryPublishCommand,
    ) -> dict[str, Any]:
        attrs = {
            MESSAGING_SYSTEM: self.messaging_system,
            MESSAGING_DESTINATION_NAME: cmd.queue,
            MESSAGING_MESSAGE_CONVERSATION_ID: cmd.correlation_id,
        }

        if isinstance(cmd.body, CeleryTask):
            attrs[CELERY_TASK_NAME] = cmd.body.task

        return attrs

    @override
    def get_publish_destination_name(
        self,
        cmd: CeleryPublishCommand,
    ) -> str:
        exchange = cmd.exchange if cmd.exchange is not None else cmd.queue
        routing_key = cmd.routing_key or cmd.queue

        return f"{exchange or DEFAULT_EXCHANGE_LABEL}.{routing_key}"
