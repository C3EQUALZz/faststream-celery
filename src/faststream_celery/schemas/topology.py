"""AsyncAPI bindings for Celery's queue topology.

Celery declares one direct exchange per queue, named after the queue, with
the queue name as the routing key (``celery.app.amqp.Queues``). That maps
cleanly onto the AMQP bindings FastStream already knows how to render.
"""

from typing import NamedTuple

from faststream.specification.schema.bindings import amqp

from .constants import DEFAULT_EXCHANGE


class Topology(NamedTuple):
    """The exchange / queue / routing key triple a channel is bound to."""

    exchange: amqp.Exchange
    queue: amqp.Queue
    routing_key: str

    @property
    def exchange_label(self) -> str:
        """Exchange part of a channel name (``_`` for the default exchange)."""
        return self.exchange.name or "_"


def build_topology(
    queue: str,
    *,
    exchange: str | None = None,
    routing_key: str | None = None,
) -> Topology:
    """Resolve Celery's defaults: exchange and routing key follow the queue."""
    exchange_name = exchange if exchange is not None else queue

    return Topology(
        exchange=(
            amqp.Exchange(type="default")
            if exchange_name == DEFAULT_EXCHANGE
            else amqp.Exchange(
                type="direct",
                name=exchange_name,
                durable=True,
                auto_delete=False,
            )
        ),
        queue=amqp.Queue(
            name=queue,
            durable=True,
            exclusive=False,
            auto_delete=False,
        ),
        routing_key=routing_key or queue,
    )
