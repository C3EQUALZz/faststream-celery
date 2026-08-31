"""Credentials and TLS.

kombu authenticates with a user id and a password, and takes its own dict of
TLS options — so of the FastStream security objects, `SASLPlaintext` (username
and password) and `BaseSecurity` (TLS on or off) are the ones that map onto it.
The SCRAM, GSSAPI and OAuth mechanisms have nowhere to go and raise
`NotImplementedError` rather than dropping the credentials silently.

Two ways to say the same thing:

* credentials in the URL — the plain kombu way;
* a `SASLPlaintext` object — the FastStream way, which keeps the secret out of
  the connection string in your logs.

Run:
    faststream run faststream_app.py:app          # or: python faststream_app.py
"""

import asyncio
import os
import ssl
from typing import Any

from faststream import FastStream
from faststream.security import BaseSecurity, SASLPlaintext

from faststream_celery import CeleryBroker
from faststream_celery.annotations import Logger

# No credentials in the URL: they come from the security object.
BROKER_URL = "amqp://rabbitmq.internal:5671//"
QUEUE = "celery"

CA_CERTS = "/etc/ssl/certs/ca-certificates.crt"


def make_security() -> BaseSecurity:
    """Username, password and TLS, with the secret read from the environment."""
    return SASLPlaintext(
        username=os.environ.get("BROKER_USER", "guest"),
        password=os.environ.get("BROKER_PASSWORD", "guest"),
        use_ssl=True,
    )


broker = CeleryBroker(
    BROKER_URL,
    security=make_security(),
    # kombu's own `ssl` option. `security=BaseSecurity(use_ssl=True)` turns TLS
    # on; anything more specific — a CA bundle, a client certificate, peer
    # verification — is said here.
    ssl={
        "ca_certs": CA_CERTS,
        "cert_reqs": ssl.CERT_REQUIRED,
    },
)
app = FastStream(broker)


@broker.subscriber(QUEUE, task="examples.ping")
async def ping(args: tuple[()], kwargs: dict[str, Any], logger: Logger) -> str:
    logger.info("pong")
    return "pong"


if __name__ == "__main__":
    asyncio.run(app.run())
