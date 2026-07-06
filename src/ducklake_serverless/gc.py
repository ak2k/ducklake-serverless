"""Garbage collection: retention-ordered, lease-guarded, dry-run first.

Order is the whole design. A generation older than the retention window
may still be pinned by a reader, and Parquet referenced only by retained
older generations looks orphaned to the current one — so:

1. Sweep expired catalog/ objects FIRST (never the current generation,
   never anything inside the retention window). This also collects
   lost-CAS orphan catalogs.
2. Only then run DuckLake's own snapshot expiry + orphan-file cleanup,
   committed through the normal CAS path like any other transaction.

The lease makes concurrent GC runners a no-op rather than a hazard.
Overlap safety does NOT come from CAS (delete is unconditional) — it
comes from sweep monotonicity: swept keys are immutable garbage outside
the retention window of a root that only moves forward, so deleting one
twice, or from two runners, is idempotent and never touches live state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ducklake_serverless.errors import ExternalServiceError, InputValidationError
from ducklake_serverless.lease import Lease
from ducklake_serverless.models import CATALOG_PREFIX, parse_catalog_key
from ducklake_serverless.root import read_root

if TYPE_CHECKING:
    from ducklake_serverless.objectstore import ObjectStore

DEFAULT_RETAIN_GENERATIONS = 10


@dataclass(frozen=True)
class GcReport:
    """What a GC pass did (or, under dry_run, would have done)."""

    dry_run: bool
    swept_catalogs: list[str] = field(default_factory=list)
    kept_catalogs: list[str] = field(default_factory=list)
    snapshots_expired: bool = False


def collect(
    store: ObjectStore,
    holder_id: str,
    *,
    retain_generations: int = DEFAULT_RETAIN_GENERATIONS,
    dry_run: bool = True,
    lease_ttl_seconds: float = 300.0,
) -> GcReport | None:
    """Run one GC pass. Returns None if another runner holds the lease.

    `retain_generations` must exceed the maximum age (in commits) of any
    reader pin — a reader attached to generation N is unaffected as long
    as N stays inside the window.
    """
    if retain_generations < 1:
        raise InputValidationError("retain_generations must be >= 1")

    lease = Lease(store, holder_id, ttl_seconds=lease_ttl_seconds)
    if not lease.acquire():
        return None
    try:
        return _collect_locked(store, lease, retain_generations, dry_run=dry_run)
    finally:
        lease.release()


# Renew the lease every N deletions so a large backlog can't outlive the TTL.
_RENEW_EVERY = 50


def _generation_of(key: str) -> int | None:
    """Generation encoded in a canonical catalog key, or None."""
    try:
        return parse_catalog_key(key)[0]
    except InputValidationError:
        return None


def _collect_locked(
    store: ObjectStore, lease: Lease, retain_generations: int, *, dry_run: bool
) -> GcReport:
    current, _ = read_root(store)
    floor = current.generation - retain_generations + 1

    listed = store.list_prefix(CATALOG_PREFIX)
    # Fail-safe guards against a corrupt or absurd root before any delete:
    # a poisoned generation number must degrade to keep-everything, never
    # amplify into sweeping the whole catalog history.
    if not dry_run:
        if current.catalog_key not in listed:
            raise ExternalServiceError(
                f"root names {current.catalog_key} but it is not in the "
                "catalog listing — refusing to sweep against a root that "
                "cannot be verified"
            )
        parseable = [g for g in (_generation_of(k) for k in listed) if g is not None]
        if parseable and current.generation > max(parseable):
            raise ExternalServiceError(
                f"root generation {current.generation} exceeds the highest "
                f"listed generation {max(parseable)} — root looks corrupt; "
                "refusing to sweep"
            )

    swept: list[str] = []
    kept: list[str] = []
    for key in listed:
        generation = _generation_of(key)
        if generation is None:
            kept.append(key)  # unknown object under catalog/ — never touch
            continue
        # The current generation is always kept, even if the window math
        # would exclude it; lost-CAS orphans share a generation number with
        # a retained winner and are kept until the window passes them.
        if key == current.catalog_key or generation >= floor:
            kept.append(key)
            continue
        swept.append(key)
        if not dry_run:
            store.delete(key)
            if len(swept) % _RENEW_EVERY == 0 and not lease.renew():
                raise ExternalServiceError(
                    "lost the maintenance lease mid-sweep — another runner "
                    "may be active; stopping (deletes so far are safe: "
                    "swept keys are immutable garbage)"
                )

    # Snapshot expiry + orphan-Parquet cleanup (ducklake_expire_snapshots /
    # ducklake_delete_orphaned_files) is deferred to the integration lane:
    # its DATA_PATH semantics must be verified against a real store first
    # (upstream ducklake#815 is a live data-loss bug in orphan detection).
    # Generation sweeping alone already bounds catalog-storage growth.
    return GcReport(
        dry_run=dry_run,
        swept_catalogs=sorted(swept),
        kept_catalogs=sorted(kept),
        snapshots_expired=False,
    )
