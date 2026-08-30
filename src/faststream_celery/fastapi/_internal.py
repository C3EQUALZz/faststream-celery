"""Adapter over the private ``faststream._internal`` FastAPI API.

Separate from the package-level adapter because importing it pulls in
FastAPI, which is an optional dependency (see ``docs/design.md`` §12). It is
also where the missing extra is reported, since it is the first thing the
subpackage imports.
"""

try:
    from faststream._internal.fastapi.context import Context, ContextRepo, Logger
    from faststream._internal.fastapi.router import StreamRouter

except ImportError as exc:  # pragma: no cover - depends on the install
    msg = (
        "The FastAPI integration needs FastAPI installed. "
        'Install it with `pip install "faststream-celery[fastapi]"`.'
    )
    raise ImportError(msg) from exc

__all__ = (
    "Context",
    "ContextRepo",
    "Logger",
    "StreamRouter",
)
