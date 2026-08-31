from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Final, NamedTuple

from faststream.message import decode_message

from ._internal import DecodedMessage, dump_json
from .exceptions import DECODE_ERRORS
from .message import CeleryMessage, ConsumerMessage
from .schemas.task import ensure_aware
from .types import HeadersType, MutableHeaders, TaskEmbed

if TYPE_CHECKING:
    from kombu import Message


# `(args, kwargs, embed)`.
_V2_BODY_SLOTS: Final[int] = 3

_NO_CANVAS: Final[TaskEmbed] = TaskEmbed(
    callbacks=None,
    errbacks=None,
    chain=None,
    chord=None,
)


class Schedule(NamedTuple):
    """When a task becomes due and when it stops being worth running."""

    eta: datetime | None
    expires: datetime | None


class CeleryParser:
    """Parse a kombu message into a ``CeleryMessage``.

    Celery has two wire protocols and this parser normalizes both onto the
    same ``StreamMessage``: task metadata always ends up in ``headers`` and
    the body always becomes ``{"args": ..., "kwargs": ...}``, so a handler
    can declare ``args`` / ``kwargs`` parameters regardless of the protocol
    the sender used.

    - **v2** (what Celery 4+ publishes, and all we publish) is detected by
      the ``task`` header; the wire body is the ``(args, kwargs, embed)``
      triple.
    - **v1** has no headers at all — the flat body carries ``task``, ``id``,
      ``args``, ``kwargs``, ``retries``, ``eta``, ``expires``.
    - Anything else (a Celery result envelope arriving on a reply queue, a
      raw payload published by a non-Celery producer) is passed through
      untouched for the decoder to handle.
    """

    async def parse_message(self, message: ConsumerMessage) -> CeleryMessage:
        """Convert a raw consumer message to a FastStream message."""
        raw = message.message

        headers, body = parse_envelope(raw)
        properties: MutableHeaders = raw.properties or {}

        return CeleryMessage(
            raw_message=message,
            ack_executor=message.executor,
            body=body,
            headers=headers,
            reply_to=properties.get("reply_to") or "",
            content_type=raw.content_type,
            correlation_id=properties.get("correlation_id"),
            message_id=headers.get("id"),
        )

    async def decode_message(self, msg: CeleryMessage) -> DecodedMessage:
        """Decode the message body to a Python object."""
        return decode_message(msg)


def parse_envelope(raw: "Message") -> tuple[MutableHeaders, bytes]:
    """Split a kombu message into FastStream headers and body."""
    headers: MutableHeaders = dict(raw.headers or {})

    if headers.get("task"):
        args, kwargs = _split_v2_body(raw.decode())
        return headers, dump_json({"args": args, "kwargs": kwargs})

    decoded = _try_decode(raw)
    if isinstance(decoded, Mapping) and "task" in decoded:
        return _parse_v1_body(decoded)

    return headers, _as_bytes(raw.body)


def read_embed(raw: "Message") -> TaskEmbed:
    """The canvas slot of a protocol v2 body: what runs after this task.

    Empty for every other kind of message, so a caller can ask without
    knowing which protocol the sender used.
    """
    if not (raw.headers or {}).get("task"):
        return _NO_CANVAS

    decoded = _try_decode(raw)
    if not isinstance(decoded, (list, tuple)) or len(decoded) < _V2_BODY_SLOTS:
        return _NO_CANVAS

    embed = decoded[2]
    if not isinstance(embed, Mapping):
        return _NO_CANVAS

    return TaskEmbed(
        callbacks=embed.get("callbacks"),
        errbacks=embed.get("errbacks"),
        chain=embed.get("chain"),
        chord=embed.get("chord"),
    )


def read_headers(raw: "Message") -> MutableHeaders:
    """Read the normalized headers without building a ``CeleryMessage``.

    Used by the subscriber to inspect ``eta`` / ``expires`` before handing
    the message to the FastStream pipeline.
    """
    return parse_envelope(raw)[0]


def extract_schedule(headers: HeadersType) -> Schedule:
    """Read the ``eta`` / ``expires`` headers as timezone-aware datetimes."""
    return Schedule(
        eta=parse_iso8601(headers.get("eta")),
        expires=parse_iso8601(headers.get("expires")),
    )


def parse_iso8601(value: object) -> datetime | None:
    """Parse a Celery ISO8601 timestamp; unreadable values become ``None``."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return ensure_aware(value)

    try:
        # `fromisoformat` only learned to read the "Z" suffix in 3.11.
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None

    return ensure_aware(parsed)


def _split_v2_body(decoded: DecodedMessage) -> tuple[DecodedMessage, DecodedMessage]:
    if isinstance(decoded, (list, tuple)):
        try:
            args, kwargs, *_embed = decoded
        except ValueError:
            pass
        else:
            return args, kwargs

    msg = f"Invalid Celery protocol v2 body: {decoded!r}"
    raise ValueError(msg)


def _parse_v1_body(body: HeadersType) -> tuple[MutableHeaders, bytes]:
    """Map the flat protocol v1 body onto v2-shaped headers and body."""
    headers: MutableHeaders = {
        "lang": "py",
        "task": body["task"],
        "id": body.get("id"),
        "eta": body.get("eta"),
        "expires": body.get("expires"),
        # v1 called a group a "taskset".
        "group": body.get("taskset"),
        "group_index": None,
        "retries": body.get("retries") or 0,
        "timelimit": body.get("timelimit") or [None, None],
        "root_id": None,
        "parent_id": None,
        "origin": None,
        "ignore_result": False,
    }

    return headers, dump_json(
        {
            "args": body.get("args") or [],
            "kwargs": body.get("kwargs") or {},
        },
    )


def _try_decode(raw: "Message") -> DecodedMessage:
    try:
        decoded: DecodedMessage = raw.decode()
    except DECODE_ERRORS:
        # A body we cannot decode is not a task message; the user's decoder
        # gets the raw bytes and decides what to do with them.
        return None
    return decoded


def _as_bytes(body: object) -> bytes:
    if isinstance(body, bytes):
        return body
    if isinstance(body, str):
        return body.encode()
    return dump_json(body)
