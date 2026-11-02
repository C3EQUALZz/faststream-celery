"""Delivery settlement shared by real consumers and in-memory messages."""

from collections.abc import Awaitable
from typing import Protocol


class Acknowledgement(Protocol):
    """Settle a delivery locally or await its transport's acknowledgement.

    Synchronous implementations return None. A consumer-thread implementation
    returns an awaitable that completes after the wire operation succeeds.
    """

    def ack(self) -> Awaitable[None] | None: ...

    def reject(self, *, requeue: bool = False) -> Awaitable[None] | None: ...
