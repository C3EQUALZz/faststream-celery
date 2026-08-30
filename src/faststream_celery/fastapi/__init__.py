"""FastAPI integration.

Requires the `fastapi` extra:

    pip install "faststream-celery[fastapi]"
"""

from ._internal import Context, ContextRepo, Logger
from .annotations import CeleryBroker, CeleryMessage, CeleryProducer
from .fastapi import CeleryRouter

__all__ = (
    "CeleryBroker",
    "CeleryMessage",
    "CeleryProducer",
    "CeleryRouter",
    "Context",
    "ContextRepo",
    "Logger",
)
