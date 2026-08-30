import json
from typing import Any

import pytest
from faststream.specification import AsyncAPI

from faststream_celery import CeleryBroker, CeleryRouter
from faststream_celery.schemas.topology import build_topology


def _schema(broker: CeleryBroker) -> dict[str, Any]:
    spec = AsyncAPI(broker, schema_version="3.0.0").to_specification()
    jsonable: dict[str, Any] = spec.to_jsonable()
    return jsonable


def test_topology_follows_the_queue_by_default() -> None:
    topology = build_topology("celery")

    assert topology.exchange.name == "celery"
    assert topology.exchange.type == "direct"
    assert topology.queue.name == "celery"
    assert topology.queue.durable
    assert topology.routing_key == "celery"


def test_topology_honors_explicit_exchange_and_routing_key() -> None:
    topology = build_topology("celery", exchange="tasks", routing_key="high")

    assert topology.exchange.name == "tasks"
    assert topology.routing_key == "high"
    assert topology.queue.name == "celery"


def test_empty_exchange_is_the_amqp_default_exchange() -> None:
    topology = build_topology("reply-queue", exchange="")

    assert topology.exchange.type == "default"
    assert topology.exchange.name is None
    assert topology.exchange_label == "_"


def test_schema_is_generated_and_json_serializable() -> None:
    broker = CeleryBroker("amqp://guest:guest@localhost:5672//")

    @broker.subscriber("celery", task="proj.tasks.add")
    async def handler(args: list[int], kwargs: dict[str, int]) -> None: ...

    broker.publisher("results")

    schema = _schema(broker)

    assert json.loads(json.dumps(schema)) == schema
    assert schema["asyncapi"] == "3.0.0"


def test_subscriber_channel_carries_the_queue_binding() -> None:
    broker = CeleryBroker("amqp://guest:guest@localhost:5672//")

    @broker.subscriber("celery", task="proj.tasks.add")
    async def handler() -> None: ...

    channels = _schema(broker)["channels"]

    assert "celery:Handler" in channels
    binding = channels["celery:Handler"]["bindings"]["amqp"]
    assert binding["queue"]["name"] == "celery"
    assert binding["queue"]["durable"] is True
    assert binding["queue"]["vhost"] == "/"


def test_subscriber_operation_carries_the_routing_key() -> None:
    broker = CeleryBroker()

    @broker.subscriber("celery", task="proj.tasks.add")
    async def handler() -> None: ...

    operations = _schema(broker)["operations"]

    (operation,) = operations.values()
    assert operation["action"] == "receive"
    assert operation["bindings"]["amqp"]["cc"] == ["celery"]


def test_publisher_channel_carries_the_exchange_binding() -> None:
    broker = CeleryBroker()
    broker.publisher("results")

    channels = _schema(broker)["channels"]

    assert "results:results:Publisher" in channels
    binding = channels["results:results:Publisher"]["bindings"]["amqp"]
    assert binding["exchange"]["name"] == "results"
    assert binding["exchange"]["type"] == "direct"


def test_publisher_channel_name_uses_a_custom_exchange() -> None:
    broker = CeleryBroker()
    broker.publisher("results", exchange="tasks", routing_key="high")

    channels = _schema(broker)["channels"]

    assert "results:tasks:Publisher" in channels


def test_router_prefix_reaches_the_schema() -> None:
    router = CeleryRouter(prefix="pre-")

    @router.subscriber("celery", task="proj.tasks.add")
    async def handler() -> None: ...

    router.publisher("results")

    broker = CeleryBroker()
    broker.include_router(router)

    channels = _schema(broker)["channels"]

    assert "pre-celery:Handler" in channels
    assert "pre-results:pre-results:Publisher" in channels


def test_custom_titles_win() -> None:
    broker = CeleryBroker()

    @broker.subscriber("celery", task="proj.tasks.add", title="AddTask")
    async def handler() -> None: ...

    broker.publisher("results", title="Results")

    channels = _schema(broker)["channels"]

    assert "AddTask" in channels
    assert "Results" in channels


@pytest.mark.parametrize(
    ("url", "expected"),
    (
        ("amqp://guest:guest@localhost:5672//", "/"),
        ("amqp://guest:guest@localhost:5672/", "/"),
        ("amqp://guest:guest@localhost:5672/myvhost", "myvhost"),
        ("redis://localhost:6379", "/"),
    ),
)
def test_virtual_host_is_read_from_the_url(url: str, expected: str) -> None:
    broker = CeleryBroker(url)

    assert broker.config.broker_config.virtual_host == expected
