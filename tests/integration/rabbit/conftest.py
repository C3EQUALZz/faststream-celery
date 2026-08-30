import pytest


@pytest.fixture()
def broker_url(rabbit_url: str) -> str:
    """The transport every test in this package runs on."""
    return rabbit_url
