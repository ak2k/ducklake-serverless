"""Shared test fixtures and hypothesis profiles."""

from __future__ import annotations

import os

from hypothesis import HealthCheck, settings

# CI needs reproducible property tests: ephemeral runners have no persisted
# example database, so derandomize — every run explores the same
# deterministic sequence and failures reproduce locally by loading "ci".
settings.register_profile(
    "ci",
    derandomize=True,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.register_profile("dev", settings.default)
settings.load_profile("ci" if os.environ.get("CI") else "dev")
