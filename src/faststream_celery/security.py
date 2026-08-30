from typing import TYPE_CHECKING

from faststream.security import (
    SASLGSSAPI,
    BaseSecurity,
    SASLOAuthBearer,
    SASLPlaintext,
    SASLScram256,
    SASLScram512,
)

if TYPE_CHECKING:
    from faststream_celery.types import MutableHeaders

# Mechanisms that carry credentials kombu has nowhere to put. Falling back to
# the SSL settings, as the built-in FastStream brokers do, would drop those
# credentials silently and fail against the broker instead.
UNSUPPORTED_MECHANISMS = (
    SASLScram256,
    SASLScram512,
    SASLGSSAPI,
    SASLOAuthBearer,
)


def parse_security(security: BaseSecurity | None) -> "MutableHeaders":
    """Map a FastStream security object to kombu ``Connection`` kwargs."""
    if security is None:
        return {}

    if isinstance(security, UNSUPPORTED_MECHANISMS):
        msg = (
            f"CeleryBroker does not support {type(security).__name__}: kombu "
            f"authenticates with a user id and a password. Use SASLPlaintext, "
            f"or pass transport options through `CeleryBroker(transport_options=...)`."
        )
        raise NotImplementedError(msg)

    if isinstance(security, SASLPlaintext):
        return {
            **_parse_base_security(security),
            "userid": security.username,
            "password": security.password,
        }

    if isinstance(security, BaseSecurity):
        return _parse_base_security(security)

    msg = f"CeleryBroker does not support {type(security)}"
    raise NotImplementedError(msg)


def _parse_base_security(security: BaseSecurity) -> "MutableHeaders":
    if security.ssl_context is not None:
        msg = (
            "CeleryBroker cannot take an `ssl_context`: kombu expects its own "
            "ssl options. Pass them as `CeleryBroker(ssl={...})` instead."
        )
        raise NotImplementedError(msg)

    if security.use_ssl:
        return {"ssl": True}

    return {}
