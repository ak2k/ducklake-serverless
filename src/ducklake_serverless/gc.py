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
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from ducklake_serverless.errors import ExternalServiceError, InputValidationError
from ducklake_serverless.lease import Lease
from ducklake_serverless.models import (
    CATALOG_PREFIX,
    MaintenanceReport,
    parse_catalog_key,
)
from ducklake_serverless.root import read_root

if TYPE_CHECKING:
    from ducklake_serverless.objectstore import ObjectStore
    from ducklake_serverless.session import Lake

DEFAULT_RETAIN_GENERATIONS = 10

# Snapshots older than this are expired (time travel beyond it is given up).
DEFAULT_EXPIRE_AGE = timedelta(days=7)
# Physical deletion lags scheduling by this much. Must exceed the wall-clock
# span of the catalog retention window plus the longest plausible commit —
# see maintain_data's docstring for why this makes out-of-band deletes safe.
DEFAULT_PHYSICAL_DELAY = timedelta(days=1)


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

    return GcReport(
        dry_run=dry_run,
        swept_catalogs=sorted(swept),
        kept_catalogs=sorted(kept),
        snapshots_expired=False,
    )


def maintain_data(
    lake: Lake,
    store: ObjectStore,
    holder_id: str,
    *,
    expire_older_than: timedelta = DEFAULT_EXPIRE_AGE,
    physical_delete_delay: timedelta = DEFAULT_PHYSICAL_DELAY,
    dry_run: bool = True,
    lease_ttl_seconds: float = 300.0,
) -> MaintenanceReport | None:
    """Run one data-plane maintenance pass. None if another runner holds the lease.

    One transaction, three DuckLake maintenance calls, committed through the
    normal CAS path:

    1. ``ducklake_expire_snapshots(older_than => now - expire_older_than)`` —
       catalog-only: marks old snapshots unreachable and SCHEDULES their
       exclusive files (timestamped). Deletes nothing.
    2. ``ducklake_cleanup_old_files(older_than => now - physical_delete_delay)``
       — physically deletes files scheduled at least ``physical_delete_delay``
       ago. Files scheduled by step 1 of THIS run are too fresh to qualify;
       a later run reclaims them.
    3. ``ducklake_delete_orphaned_files`` with the same age gate — deletes
       never-referenced Parquet (aborted/lost transactions).

    Why the age gate is load-bearing: the physical deletes are side effects
    OUTSIDE the CAS transaction. If this run's commit loses its race, files
    it deleted must already have been dead in every retained generation and
    invisible to every in-flight commit — which holds exactly when
    ``physical_delete_delay`` exceeds the wall-clock span of the catalog
    retention window plus the longest plausible commit. The upstream #815
    mis-orphaning bug is fixed (ducklake PR #863) and our pins refuse older
    extensions, but dry_run stays the default: inspect before deleting.

    A lost CAS race aborts cleanly (maintenance CALLs are state-dependent —
    never replayed); the next scheduled run simply retries.
    """
    lease = Lease(store, holder_id, ttl_seconds=lease_ttl_seconds)
    if not lease.acquire():
        return None
    try:
        return _maintain_locked(
            lake,
            expire_older_than=expire_older_than,
            physical_delete_delay=physical_delete_delay,
            dry_run=dry_run,
        )
    finally:
        lease.release()


def _maintain_locked(
    lake: Lake,
    *,
    expire_older_than: timedelta,
    physical_delete_delay: timedelta,
    dry_run: bool,
) -> MaintenanceReport:
    now = datetime.now(tz=UTC)
    expire_before = now - expire_older_than
    physical_before = now - physical_delete_delay

    if dry_run:
        # The ducklake CALLs delete nothing under dry_run => true, and
        # scratch() guarantees the connection's copy is never published.
        with lake.scratch() as con:
            expired = con.execute(
                "CALL ducklake_expire_snapshots('lake', dry_run => true, older_than => ?)",
                (expire_before,),
            )
            cleaned = con.execute(
                "CALL ducklake_cleanup_old_files('lake', dry_run => true, older_than => ?)",
                (physical_before,),
            )
            orphans = con.execute(
                "CALL ducklake_delete_orphaned_files('lake', dry_run => true, older_than => ?)",
                (physical_before,),
            )
        return MaintenanceReport(
            dry_run=True,
            snapshots_expired=_column(expired),
            files_cleaned=_column(cleaned),
            orphans_deleted=_column(orphans),
        )

    with lake.transaction() as tx:
        expired = tx.sql(
            "CALL ducklake_expire_snapshots('lake', older_than => ?)", (expire_before,)
        )
        cleaned = tx.sql(
            "CALL ducklake_cleanup_old_files('lake', older_than => ?)", (physical_before,)
        )
        orphans = tx.sql(
            "CALL ducklake_delete_orphaned_files('lake', older_than => ?)",
            (physical_before,),
        )
    return MaintenanceReport(
        dry_run=False,
        snapshots_expired=_column(expired),
        files_cleaned=_column(cleaned),
        orphans_deleted=_column(orphans),
    )


def _column(rows: list[tuple[object, ...]]) -> tuple[str, ...]:
    return tuple(str(r[0]) for r in rows)
