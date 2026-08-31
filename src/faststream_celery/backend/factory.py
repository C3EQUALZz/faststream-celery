from typing import Final
from urllib.parse import urlparse

from .proto import ResultBackend

# `rpc://` is Celery's name for "the result comes back over the broker",
# which is what `broker.request()` already does without a backend.
RPC_SCHEMES: Final[frozenset[str]] = frozenset({"rpc"})
REDIS_SCHEMES: Final[frozenset[str]] = frozenset({"redis", "rediss", "unix"})


def make_result_backend(url: str | None) -> ResultBackend | None:
    """Build the backend a result url names.

    Returns ``None`` for no backend and for ``rpc://``, both of which leave
    `broker.request()` on its AMQP reply-queue path.
    """
    if not url:
        return None

    scheme = urlparse(url).scheme

    if scheme in RPC_SCHEMES:
        return None

    if scheme in REDIS_SCHEMES:
        # Imported here so `redis` stays an optional dependency.
        from .redis import RedisResultBackend  # ruff: ignore[import-outside-top-level]

        return RedisResultBackend(url)

    msg = (
        f"CeleryBroker does not support the {scheme!r} result backend. "
        f"Supported: redis://, rediss://, rpc://."
    )
    raise NotImplementedError(msg)
