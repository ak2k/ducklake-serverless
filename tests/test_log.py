"""Canonical log-configuration test pattern."""

from __future__ import annotations

from typing import TYPE_CHECKING

from myproject.log import configure

if TYPE_CHECKING:
    from myproject.config import LogLevel


def test_configure_runs_without_error() -> None:
    """Smoke test: the default configuration constructs the structlog chain."""
    configure(level="info", json=False)


def test_configure_idempotent_across_levels_and_renderers() -> None:
    """Reconfiguration mid-process should not raise."""
    levels: list[LogLevel] = ["debug", "info", "warning", "error"]
    for level in levels:
        for json_mode in (True, False):
            configure(level=level, json=json_mode)
