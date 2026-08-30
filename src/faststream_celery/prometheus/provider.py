from typing import TYPE_CHECKING

from faststream.prometheus import ConsumeAttrs, MetricsSettingsProvider
from typing_extensions import override

from faststream_celery.message import ConsumerMessage
from faststream_celery.response import CeleryPublishCommand

if TYPE_CHECKING:
    from faststream.message import StreamMessage


class CeleryMetricsSettingsProvider(
    MetricsSettingsProvider[ConsumerMessage, CeleryPublishCommand],
):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "celery"

    @override
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[ConsumerMessage]",
    ) -> ConsumeAttrs:
        # Celery names a queue, its exchange and its routing key alike, so the
        # routing key is the queue for every message a Celery client sends.
        routing_key = msg.raw_message.message.delivery_info.get("routing_key")

        return {
            "destination_name": str(routing_key or ""),
            "message_size": len(msg.body),
            "messages_count": 1,
        }

    @override
    def get_publish_destination_name_from_cmd(
        self,
        cmd: CeleryPublishCommand,
    ) -> str:
        return cmd.queue
