from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec
from faststream.specification.schema.bindings import (
    ChannelBinding,
    OperationBinding,
    amqp,
)
from typing_extensions import override

from faststream_celery._internal import SubscriberSpecification
from faststream_celery.configs import CeleryBrokerConfig
from faststream_celery.schemas.topology import build_topology

from .config import CelerySubscriberSpecificationConfig


class CelerySubscriberSpecification(
    SubscriberSpecification[CeleryBrokerConfig, CelerySubscriberSpecificationConfig],
):
    @property
    def queue(self) -> str:
        return f"{self._outer_config.prefix}{self.config.queue}"

    @property
    @override
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.queue}:{self.call_name}"

    @override
    def get_schema(self) -> dict[str, SubscriberSpec]:
        payloads = self.get_payloads()
        topology = build_topology(self.queue)
        channel_name = self.name

        return {
            channel_name: SubscriberSpec(
                description=self.description,
                operation=Operation(
                    bindings=OperationBinding(
                        amqp=amqp.OperationBinding(
                            routing_key=topology.routing_key,
                            queue=topology.queue,
                            exchange=topology.exchange,
                            ack=True,
                            reply_to=None,
                            persist=None,
                            mandatory=None,
                            priority=None,
                        ),
                    ),
                    message=Message(
                        title=f"{channel_name}:Message",
                        payload=resolve_payloads(payloads),
                    ),
                ),
                bindings=ChannelBinding(
                    amqp=amqp.ChannelBinding(
                        virtual_host=self._outer_config.virtual_host,
                        queue=topology.queue,
                        exchange=topology.exchange,
                    ),
                ),
            ),
        }
