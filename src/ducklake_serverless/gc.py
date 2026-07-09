"""Garbage collection: retention-ordered, lease-guarded, dry-run first.

Two independent, separately-invocable passes share one fleet-wide lease
(same LEASE_KEY — catalog GC and data maintenance are intentionally
mutually exclusive; a co-scheduled run returns None and retries later):

- ``collect`` — sweeps expired payload/ generation objects (never the
  current generation, never anything inside the count-based retention
  window). Also collects lost-CAS orphan catalogs.
- ``maintain_data`` — DuckLake's own snapshot expiry + physical file
  cleanup + orphan-Parquet deletion, committed through the normal CAS
  path. Time-based gates; see its docstring for the three safety
  arguments and how the two windows compose for reader pins.

Overlap safety for ``collect`` does NOT come from CAS (delete is
unconditional) — it comes from sweep monotonicity: swept keys are
immutable garbage outside the retention window of a root that only moves
forward, so deleting one twice, or from two runners, is idempotent and
never touches live state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from ducklake_serverless.errors import ExternalServiceError, InputValidationError
from ducklake_serverless.lease import Lease
from ducklake_serverless.models import (
    PAYLOAD_PREFIX,
    MaintenanceReport,
    parse_payload_key,
)
from ducklake_serverless.root import resolve_head, write_hint

if TYPE_CHECKING:
    from ducklake_serverless.objectstore import ObjectStore
    from ducklake_serverless.session import Lake

DEFAULT_RETAIN_GENERATIONS = 10

# Snapshots older than this are expired (time travel beyond it is given up).
DEFAULT_EXPIRE_AGE = timedelta(days=7)
# Physical deletion lags scheduling by this much. Protects in-flight writers
# (staged-but-uncommitted Parquet is 'orphaned' until its commit lands) and
# extends reader-pin durability — see maintain_data's safety notes.
DEFAULT_PHYSICAL_DELAY = timedelta(days=1)


@dataclass(frozen=True)
class GcReport:
    """What a GC pass did (or, under dry_run, would have done)."""

    dry_run: bool
    swept_catalogs: list[str] = field(default_factory=list)
    kept_catalogs: list[str] = field(default_factory=list)


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
        return parse_payload_key(key)[0]
    except InputValidationError:
        return None


def _collect_locked(
    store: ObjectStore, lease: Lease, retain_generations: int, *, dry_run: bool
) -> GcReport:
    current, head_gen = resolve_head(store)
    floor = current.generation - retain_generations + 1

    listed = store.list_prefix(PAYLOAD_PREFIX)
    # Head is an extant, immutable marker (never a fabricated pointer in v2),
    # so a "corrupt head generation" is unrepresentable — the v1 absurd-root
    # guard is retired. One fail-safe survives: the head's catalog must be
    # present before we sweep, or a listing anomaly could hide live data.
    if not dry_run:
        if current.payload_key not in listed:
            raise ExternalServiceError(
                f"head names {current.payload_key} but it is not in the "
                "catalog listing — refusing to sweep against a head that "
                "cannot be verified"
            )
        # Advance the advisory hint to head before sweeping catalogs, so the
        # window where a stale hint coexists with swept catalogs is minimal
        # (readers still recover via probe + the catalog-fetch retry — this
        # is belt-and-suspenders). Never touch roots/: markers are immortal.
        write_hint(store, head_gen)

    swept: list[str] = []
    kept: list[str] = []
    for key in listed:
        generation = _generation_of(key)
        if generation is None:
            kept.append(key)  # unknown object under payload/ — never touch
            continue
        # The current generation is always kept, even if the window math
        # would exclude it; lost-CAS orphans share a generation number with
        # a retained winner and are kept until the window passes them.
        if key == current.payload_key or generation >= floor:
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
    )


# Floor for non-dry-run physical deletion. Below this, the age gate cannot
# be trusted to outlast an in-flight writer's stage-Parquet-then-commit
# window, and the orphan pass could delete a live transaction's staged
# files (see maintain_data's safety notes). Tests use dry_run or accept
# the risk explicitly via _unsafe_allow_short_delay.
MIN_PHYSICAL_DELAY = timedelta(minutes=15)


def maintain_data(
    lake: Lake,
    store: ObjectStore,
    holder_id: str,
    *,
    expire_older_than: timedelta = DEFAULT_EXPIRE_AGE,
    physical_delete_delay: timedelta = DEFAULT_PHYSICAL_DELAY,
    dry_run: bool = True,
    lease_ttl_seconds: float = 300.0,
    _unsafe_allow_short_delay: bool = False,
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

    Safety notes (the physical deletes are side effects OUTSIDE the CAS
    transaction — three distinct arguments cover the three hazards):

    - **Lost CAS race**: safe by schedule-table semantics, not by the age
      gate. ``cleanup_old_files`` only deletes files whose *scheduling*
      committed in an ancestor generation, so any writer that beats our
      commit built on a catalog that already considered them dead.
    - **In-flight writers**: a writer stages Parquet BEFORE its commit; those
      files are never-referenced until the commit lands, so only the orphan
      pass's age gate protects them. ``physical_delete_delay`` must exceed
      the longest plausible stage-to-commit window — enforced by
      ``MIN_PHYSICAL_DELAY`` in non-dry-run mode.
    - **Pinned readers**: expiring a snapshot gives up data reads on catalog
      generations that still reference it. The catalog ATTACH keeps working
      (generation files are swept by count-based ``collect``), but Parquet
      scans fail once cleanup reclaims the files. The real contract: a
      reader pin is durable for ``min(catalog retention window,
      expire_older_than + physical_delete_delay)`` — plan
      ``expire_older_than`` around the longest reader pin, not around the
      generation count.

    The upstream #815 misorphaning bug is fixed (ducklake PR #863) and our
    version pins refuse older extensions, but dry_run stays the default:
    inspect before deleting. A lost CAS race aborts cleanly (maintenance
    CALLs are state-dependent — never replayed); the next run retries. A
    pass that would change nothing skips its commit entirely, so idle-lake
    maintenance ticks do not churn generations.
    """
    if expire_older_than < timedelta(0) or physical_delete_delay < timedelta(0):
        raise InputValidationError(
            "expire_older_than and physical_delete_delay must be non-negative "
            "(a negative delay collapses the two-phase schedule/delete gate)"
        )
    if not dry_run and physical_delete_delay < MIN_PHYSICAL_DELAY and not _unsafe_allow_short_delay:
        raise InputValidationError(
            f"physical_delete_delay {physical_delete_delay} is below the "
            f"{MIN_PHYSICAL_DELAY} floor — the orphan pass could delete an "
            "in-flight writer's staged Parquet. Raise the delay (or, in "
            "tests with no concurrent writers, pass _unsafe_allow_short_delay=True)."
        )
    lease = Lease(store, holder_id, ttl_seconds=lease_ttl_seconds)
    if not lease.acquire():
        return None
    try:
        return _maintain_locked(
            lake,
            lease,
            expire_older_than=expire_older_than,
            physical_delete_delay=physical_delete_delay,
            dry_run=dry_run,
        )
    finally:
        lease.release()


def _maintain_locked(
    lake: Lake,
    lease: Lease,
    *,
    expire_older_than: timedelta,
    physical_delete_delay: timedelta,
    dry_run: bool,
) -> MaintenanceReport:
    now = datetime.now(tz=UTC)
    expire_before = now - expire_older_than
    physical_before = now - physical_delete_delay

    # Probe first (dry CALLs delete nothing; scratch()'s catalog copy is
    # never published). Serves both modes: it IS the dry-run result, and in
    # wet mode a nothing-to-do probe lets us skip the commit entirely — an
    # idle lake's maintenance tick must not churn catalog generations.
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
    probe = MaintenanceReport(
        dry_run=True,
        snapshots_expired=_column(expired),
        files_cleaned=_column(cleaned),
        orphans_deleted=_column(orphans),
    )
    if dry_run or not (probe.snapshots_expired or probe.files_cleaned or probe.orphans_deleted):
        return probe

    # The probe took real time on a large lake; prove we still hold the
    # lease before the deleting pass, and again before publishing.
    if not lease.renew():
        raise ExternalServiceError(
            "lost the maintenance lease after the probe — another runner "
            "may be active; aborting before any physical deletion"
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
        if not lease.renew():
            # Deletes already happened (safe: schedule-table semantics for
            # cleaned files, age gate for orphans) — but publishing a
            # generation while another runner may also be publishing invites
            # needless CAS churn. Abort; the catalog schedule state is
            # unchanged, so the next holder's pass converges.
            raise ExternalServiceError(
                "lost the maintenance lease before commit — aborting the "
                "catalog publish; physical deletes already done are safe "
                "and the next pass converges"
            )
    return MaintenanceReport(
        dry_run=False,
        snapshots_expired=_column(expired),
        files_cleaned=_column(cleaned),
        orphans_deleted=_column(orphans),
    )


def _column(rows: list[tuple[object, ...]]) -> tuple[str, ...]:
    return tuple(str(r[0]) for r in rows)
