"""Catalog generation transport: fetch pristine copies, publish with hygiene.

Generations are immutable once published, so fetches cache aggressively —
but the cache hands out *copies*: a writer mutates its copy in place, and a
poisoned pristine would corrupt every later transaction in this process.

Publish is gated: a catalog file with a live WAL sidecar, wrong magic
bytes, or an implausible size must never reach the bucket.
"""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

from ducklake_serverless.engine import MAGIC, MAGIC_OFFSET
from ducklake_serverless.errors import CatalogHygieneError
from ducklake_serverless.models import format_catalog_key

if TYPE_CHECKING:
    from pathlib import Path
    from uuid import UUID

    from ducklake_serverless.objectstore import ObjectStore

# Catalogs hold metadata only; a file this large means runaway inlined data
# or a bug. Fail loudly rather than absorb the per-commit upload cost.
MAX_CATALOG_BYTES = 256 * 1024 * 1024


class GenerationCache:
    """Fetches catalog generations into a local directory and vends copies."""

    def __init__(self, store: ObjectStore, workdir: Path) -> None:
        self._store = store
        self._workdir = workdir
        self._pristine: dict[str, Path] = {}

    def fetch_copy(self, generation: int, catalog_uuid: UUID) -> Path:
        """Return a private, mutable copy of the given generation's catalog."""
        key = format_catalog_key(generation, catalog_uuid)
        pristine = self._pristine.get(key)
        if pristine is None or not pristine.exists():
            result = self._store.get(key)
            pristine = self._workdir / f"pristine-{generation:08d}-{catalog_uuid}.duckdb"
            pristine.write_bytes(result.body)
            self._pristine[key] = pristine
        copy = self._workdir / f"work-{generation:08d}-{catalog_uuid}.duckdb"
        shutil.copyfile(pristine, copy)
        return copy


def check_hygiene(catalog_path: Path) -> None:
    """Refuse to publish a catalog file that could corrupt the lake."""
    for suffix in (".wal", ".tmp"):
        sidecar = catalog_path.with_name(catalog_path.name + suffix)
        if sidecar.exists():
            raise CatalogHygieneError(
                f"{sidecar.name} exists — catalog was not cleanly checkpointed"
            )
    size = catalog_path.stat().st_size
    if size > MAX_CATALOG_BYTES:
        raise CatalogHygieneError(
            f"catalog is {size} bytes (cap {MAX_CATALOG_BYTES}) — "
            "runaway inlined data or a bug; refusing to publish"
        )
    with catalog_path.open("rb") as f:
        header = f.read(MAGIC_OFFSET + len(MAGIC))
    if header[MAGIC_OFFSET:] != MAGIC:
        raise CatalogHygieneError(f"{catalog_path.name} does not look like a DuckDB file")


def publish_generation(
    store: ObjectStore, catalog_path: Path, generation: int, catalog_uuid: UUID
) -> str:
    """Hygiene-check and upload a new catalog generation (create-only).

    Runs BEFORE the root CAS: a lost race strands only this orphan object,
    never a root pointing at a missing or dirty catalog.
    """
    check_hygiene(catalog_path)
    key = format_catalog_key(generation, catalog_uuid)
    return store.put_if_absent(key, catalog_path.read_bytes())
