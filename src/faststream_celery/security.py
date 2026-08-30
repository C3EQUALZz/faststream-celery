from typing import Any

from faststream.security import BaseSecurity, SASLPlaintext


def parse_security(security: BaseSecurity | None) -> dict[str, Any]:
    """Map a FastStream security object to kombu ``Connection`` kwargs."""
    if security is None:
        return {}

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


def _parse_base_security(security: BaseSecurity) -> dict[str, Any]:
    if security.use_ssl:
        return {"ssl": security.ssl_context or True}
    return {}
