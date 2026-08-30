from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, PublisherSpec

from faststream_celery._internal import PublisherSpecification
from faststream_celery.configs import CeleryBrokerConfig

from .config import CeleryPublisherSpecificationConfig


class CeleryPublisherSpecification(
    PublisherSpecification[CeleryBrokerConfig, CeleryPublisherSpecificationConfig],
):
    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        queue = f"{self._outer_config.prefix}{self.config.queue}"
        return f"{queue}:{self.config.exchange or '_'}:Publisher"

    def get_schema(self) -> dict[str, PublisherSpec]:
        payloads = self.get_payloads()

        return {
            self.name: PublisherSpec(
                description=self.config.description_,
                operation=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads),
                    ),
                    bindings=None,
                ),
                bindings=None,
            ),
        }
