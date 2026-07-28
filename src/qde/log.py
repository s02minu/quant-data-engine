"""Structured logging setup for qde.

One call to configure() wires structlog to emit leveled, timestamped events with
key-value context. Console rendering by default (readable in `docker logs` and
cron output); JSON when QDE_LOG_FORMAT=json, for machine parsing later.
"""

import os
from typing import Any

import structlog


def configure() -> None:
    """Configure structlog once, at process start (entry points call this)."""
    json_mode = os.getenv("QDE_LOG_FORMAT", "").lower() == "json"
    renderer = structlog.processors.JSONRenderer() if json_mode else structlog.dev.ConsoleRenderer()
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            renderer,
        ]
    )


def get_logger(name: str | None = None) -> Any:
    """Return a structlog logger bound to `name` (module-level use is fine)."""
    return structlog.get_logger(name)
