import asyncio
import uuid

import pytest


@pytest.fixture()
def queue() -> str:
    """Generate a unique queue name per test."""
    return f"faststream-celery-test-{uuid.uuid4().hex}"


@pytest.fixture()
def event() -> asyncio.Event:
    return asyncio.Event()
