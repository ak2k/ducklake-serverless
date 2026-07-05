"""Canonical logging configuration. Call `configure(...)` once at startup.

Demonstrates:
- structlog setup with an ordered processor chain.
- JSON output for production; ConsoleRenderer for dev.
- Type-safe level mapping (no `getattr` -> Any).
- Idempotent — safe to call from tests and app entry points.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from structlog.typing import Processor

    from myproject.config import LogLevel

LEVELS: dict[str, int] = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


def configure(level: LogLevel = "info", json: bool = False) -> None:
    """Configure structlog + stdlib logging.

    Args:
        level: Minimum log level to emit; below-level calls are dropped.
        json: True for JSON output (production); False for ConsoleRenderer (dev).

    """
    processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
    ]
    if json:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(LEVELS[level]),
        cache_logger_on_first_use=True,
    )
