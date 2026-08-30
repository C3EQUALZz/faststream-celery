from typing import TYPE_CHECKING, Any

from faststream.message import decode_message

from ._internal import DecodedMessage, dump_json
from .message import CeleryMessage, ConsumerMessage

if TYPE_CHECKING:
    from kombu import Message


class CeleryParser:
    """Parse a kombu message into a ``CeleryMessage`` (protocol v2).

    Protocol v2 is detected by the presence of the ``task`` header; the wire
    body ``(args, kwargs, embed)`` is normalized to
    ``{"args": ..., "kwargs": ...}`` so handlers can declare ``args`` /
    ``kwargs`` parameters.
    """

    async def parse_message(self, message: ConsumerMessage) -> CeleryMessage:
        """Convert a raw consumer message to a FastStream message."""
        raw = message.message

        headers = dict(raw.headers or {})
        if "task" not in headers:
            msg = "Celery protocol v1 messages are not supported yet"
            raise ValueError(msg)

        args, kwargs = _parse_body(raw)

        properties: dict[str, Any] = raw.properties or {}

        return CeleryMessage(
            raw_message=raw,
            ack_executor=message.executor,
            body=dump_json({"args": args, "kwargs": kwargs}),
            headers=headers,
            reply_to=properties.get("reply_to") or "",
            content_type=raw.content_type,
            correlation_id=properties.get("correlation_id"),
            message_id=headers.get("id"),
        )

    async def decode_message(self, msg: CeleryMessage) -> DecodedMessage:
        """Decode the message body to a Python object."""
        return decode_message(msg)


def _parse_body(raw: "Message") -> tuple[Any, Any]:
    decoded = raw.decode()

    if isinstance(decoded, (list, tuple)):
        try:
            args, kwargs, *_embed = decoded
        except ValueError:
            pass
        else:
            return args, kwargs

    msg = f"Invalid Celery protocol v2 body: {decoded!r}"
    raise ValueError(msg)
