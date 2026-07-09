"""Structural lock: the engine core imports no duckdb.

The whole point of the payload-agnostic split is that the
generation/marker/CAS/lease/GC/commit machinery never depends on DuckDB — so a
plain blob (or any future payload) rides the same engine. This asserts that
invariant mechanically, in a fresh interpreter per module (the test process has
duckdb loaded via the DuckLake tests, so sys.modules must be checked in
isolation). If someone imports duckdb into a core module, this fails loudly.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

# Modules that MUST stay duckdb-free: the engine core + the generic blob adapter.
# (DuckLake's engine.py/session.py/recorder.py/generation.py legitimately import
# duckdb and are excluded.)
ENGINE_CORE = [
    "commit",
    "blob",
    "models",
    "root",
    "lease",
    "objectstore",
    "payload",
    "errors",
    "config",
    "log",
]


def _pulls_duckdb(module: str) -> bool:
    code = f"import {module}, sys; sys.exit(1 if 'duckdb' in sys.modules else 0)"
    result = subprocess.run([sys.executable, "-c", code], check=False)  # noqa: S603
    return result.returncode == 1


@pytest.mark.parametrize("mod", ENGINE_CORE)
def test_engine_core_is_duckdb_free(mod: str) -> None:
    full = f"ducklake_serverless.{mod}"
    assert not _pulls_duckdb(full), f"{full} transitively imports duckdb — boundary violated"
