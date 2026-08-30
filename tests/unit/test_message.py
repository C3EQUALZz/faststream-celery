from collections.abc import Callable
from unittest.mock import MagicMock

import pytest
from faststream.message import AckStatus

from faststream_celery.message import CeleryMessage


def _make_celery_message() -> tuple[CeleryMessage, MagicMock]:
    raw = MagicMock()

    async def executor(action: Callable[[], None]) -> None:
        action()

    return CeleryMessage(raw_message=raw, body=b"", ack_executor=executor), raw


@pytest.mark.asyncio()
async def test_ack_calls_kombu_ack() -> None:
    message, raw = _make_celery_message()

    await message.ack()

    raw.ack.assert_called_once_with()
    assert message.committed is AckStatus.ACKED


@pytest.mark.asyncio()
async def test_ack_is_idempotent() -> None:
    message, raw = _make_celery_message()

    await message.ack()
    await message.ack()

    raw.ack.assert_called_once_with()


@pytest.mark.asyncio()
async def test_nack_requeues() -> None:
    """Nack maps to kombu reject with requeue (Celery task retry semantics)."""
    message, raw = _make_celery_message()

    await message.nack()

    raw.reject.assert_called_once_with(requeue=True)
    assert message.committed is AckStatus.NACKED


@pytest.mark.asyncio()
async def test_reject_does_not_requeue() -> None:
    message, raw = _make_celery_message()

    await message.reject()

    raw.reject.assert_called_once_with(requeue=False)
    assert message.committed is AckStatus.REJECTED


@pytest.mark.asyncio()
async def test_nack_after_ack_is_ignored() -> None:
    message, raw = _make_celery_message()

    await message.ack()
    await message.nack()

    raw.ack.assert_called_once_with()
    raw.reject.assert_not_called()
