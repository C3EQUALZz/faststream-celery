from typing import Final

"""Celery wire defaults.

The values Celery itself uses, so a message we publish looks like one a
Celery client would have published.
"""

# `task_default_delivery_mode` — persistent messages.
PERSISTENT_DELIVERY_MODE: Final[int] = 2

# `accept_content`.
SERIALIZATION_ACCEPT: Final[list[str]] = ["json"]

# `task_serializer`.
SERIALIZER: Final[str] = "json"

CONTENT_TYPE: Final[str] = "application/json"
CONTENT_ENCODING: Final[str] = "utf-8"

# The AMQP default exchange, used for RPC replies.
DEFAULT_EXCHANGE: Final[str] = ""
