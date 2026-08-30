"""Celery wire defaults.

The values Celery itself uses, so a message we publish looks like one a
Celery client would have published.
"""

# `task_default_delivery_mode` — persistent messages.
PERSISTENT_DELIVERY_MODE = 2

# `accept_content`.
SERIALIZATION_ACCEPT = ["json"]

# `task_serializer`.
SERIALIZER = "json"

CONTENT_TYPE = "application/json"
CONTENT_ENCODING = "utf-8"

# The AMQP default exchange, used for RPC replies.
DEFAULT_EXCHANGE = ""
