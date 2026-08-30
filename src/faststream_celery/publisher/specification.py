from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, PublisherSpec
from faststream.specification.schema.bindings import (
    ChannelBinding,
    OperationBinding,
    amqp,
)
from typing_extensions import override

from faststream_celery._internal import PublisherSpecification
from faststream_celery.configs import CeleryBrokerConfig
from faststream_celery.schemas.topology import build_topology

from .config import CeleryPublisherSpecificationConfig


class CeleryPublisherSpecification(
    PublisherSpecification[CeleryBrokerConfig, CeleryPublisherSpecificationConfig],
):
    @property
    def queue(self) -> str:
        return f"{self._outer_config.prefix}{self.config.queue}"

    @property
    @override
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        topology = build_topology(self.queue, exchange=self.config.exchange)
        return f"{self.queue}:{topology.exchange_label}:Publisher"

    @override
    def get_schema(self) -> dict[str, PublisherSpec]:
        payloads = self.get_payloads()
        topology = build_topology(
            self.queue,
            exchange=self.config.exchange,
            routing_key=self.config.routing_key,
        )
        channel_name = self.name

        return {
            channel_name: PublisherSpec(
                description=self.config.description_,
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
