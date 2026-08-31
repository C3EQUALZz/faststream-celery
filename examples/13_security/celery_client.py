"""The Celery side of the same connection settings.

Both ends have to agree, and this is where the two vocabularies meet: the
FastStream security object becomes kombu connection options, and Celery spells
the same options `broker_use_ssl` and credentials in the URL.

Run:
    python celery_client.py
"""

import os
import ssl
import sys

from celery import Celery

BROKER_HOST = "rabbitmq.internal:5671"
QUEUE = "celery"
RESULT_TIMEOUT = 15.0

CA_CERTS = "/etc/ssl/certs/ca-certificates.crt"

BROKER_URL = (
    f"amqp://{os.environ.get('BROKER_USER', 'guest')}:"
    f"{os.environ.get('BROKER_PASSWORD', 'guest')}@{BROKER_HOST}//"
)

app = Celery("examples.client", broker=BROKER_URL, backend="rpc://")
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # Celery's name for the same kombu `ssl` option.
    broker_use_ssl={
        "ca_certs": CA_CERTS,
        "cert_reqs": ssl.CERT_REQUIRED,
    },
)


def main() -> int:
    result = app.send_task("examples.ping", args=[], kwargs={}, queue=QUEUE)
    print("examples.ping ->", result.get(timeout=RESULT_TIMEOUT))

    return 0


if __name__ == "__main__":
    sys.exit(main())
