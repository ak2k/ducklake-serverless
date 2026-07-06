"""Shared test fixtures and hypothesis profiles."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from hypothesis import HealthCheck, settings

if TYPE_CHECKING:
    from ducklake_serverless.session import Lake

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


def lake_churn(lake: Lake) -> None:
    """Shared setup: real Parquet churn — large insert fully deleted, replaced.

    Produces expirable snapshot history plus one dead and one live data file.
    """
    lake.bootstrap()
    with lake.transaction() as tx:
        tx.sql("CREATE TABLE t (id INTEGER)")
    with lake.transaction() as tx:
        tx.sql("INSERT INTO t SELECT range FROM range(100000)")
    with lake.transaction() as tx:
        tx.sql("DELETE FROM t")
    with lake.transaction() as tx:
        tx.sql("INSERT INTO t SELECT range FROM range(50000)")
