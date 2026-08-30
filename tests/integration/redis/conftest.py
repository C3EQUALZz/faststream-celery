from collections.abc import Iterator

import pytest
import redis


@pytest.fixture()
def broker_url(redis_url: str) -> str:
    """The transport every test in this package runs on."""
    return redis_url


@pytest.fixture()
def redis_client(redis_url: str) -> Iterator[redis.Redis]:
    """A direct client, for asserting on the keys kombu writes."""
    client = redis.Redis.from_url(redis_url)
    try:
        yield client
    finally:
        client.close()
