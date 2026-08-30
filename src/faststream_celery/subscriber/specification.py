from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec

from faststream_celery._internal import SubscriberSpecification
from faststream_celery.configs import CeleryBrokerConfig

from .config import CelerySubscriberSpecificationConfig


class CelerySubscriberSpecification(
    SubscriberSpecification[CeleryBrokerConfig, CelerySubscriberSpecificationConfig],
):
    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        queue = f"{self._outer_config.prefix}{self.config.queue}"
        return f"{queue}:{self.call_name}"

    def get_schema(self) -> dict[str, SubscriberSpec]:
        payloads = self.get_payloads()

        return {
            self.name: SubscriberSpec(
                description=self.description,
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
