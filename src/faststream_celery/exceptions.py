"""The failures this broker reacts to, grouped by what it does about them.

Named groups rather than a bare ``except Exception``: each ``except`` clause
says which failures it is prepared for, so anything else keeps travelling up
instead of being quietly absorbed.
"""

from amqp.exceptions import AMQPError
from kombu.exceptions import KombuError

__all__ = (
    "CONNECTION_ERRORS",
    "DECODE_ERRORS",
    "SETTLE_ERRORS",
)

# Turning a wire body into Python failed: a serializer we do not accept, a
# truncated payload, or a body that is not shaped like a Celery envelope.
# `ValueError` covers `json`, `UnicodeDecodeError` and our own envelope
# checks; `TypeError` covers a body of the wrong shape entirely.
DECODE_ERRORS: tuple[type[Exception], ...] = (
    KombuError,
    ValueError,
    TypeError,
)

# Acknowledging a message failed: the channel is gone, or the message was
# already settled by someone else.
SETTLE_ERRORS: tuple[type[Exception], ...] = (
    KombuError,
    AMQPError,
    OSError,
)

# Reaching the broker failed.
CONNECTION_ERRORS: tuple[type[Exception], ...] = (
    KombuError,
    AMQPError,
    OSError,
)
