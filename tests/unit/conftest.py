import pytest

from faststream_celery import CeleryBroker, CeleryRouter
from tests.helpers import MEMORY_URL


@pytest.fixture()
def broker() -> CeleryBroker:
    """An unstarted broker for tests that never touch a transport."""
    return CeleryBroker()


@pytest.fixture()
def memory_broker() -> CeleryBroker:
    """An unstarted broker on kombu's in-process transport.

    Real consumer threads, real acks, real `drain_events` — everything the
    in-memory `TestCeleryBroker` fake deliberately skips.
    """
    return CeleryBroker(MEMORY_URL)


@pytest.fixture()
def router() -> CeleryRouter:
    return CeleryRouter()
