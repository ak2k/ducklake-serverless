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

import contextlib
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Literal

import pydantic
from pydantic import BaseModel, ConfigDict, Field

from ducklake_serverless.chunk import (
    MAX_MANIFEST_BYTES,
    PACKS_PREFIX,
    Manifest,
    sniff_manifest,
)
from ducklake_serverless.errors import (
    ExternalServiceError,
    InputValidationError,
    ObjectNotFoundError,
)
from ducklake_serverless.lease import Lease
from ducklake_serverless.models import (
    PAYLOAD_PREFIX,
    MaintenanceReport,
    RootDoc,
    parse_payload_key,
)
from ducklake_serverless.root import read_marker, resolve_head, write_hint

if TYPE_CHECKING:
    from ducklake_serverless.objectstore import ObjectMeta, ObjectStore
    from ducklake_serverless.session import Lake

DEFAULT_RETAIN_GENERATIONS = 10

# Snapshots older than this are expired (time travel beyond it is given up).
DEFAULT_EXPIRE_AGE = timedelta(days=7)
# Physical deletion lags scheduling by this much. Protects in-flight writers
# (staged-but-uncommitted Parquet is 'orphaned' until its commit lands) and
# extends reader-pin durability — see maintain_data's safety notes.
DEFAULT_PHYSICAL_DELAY = timedelta(days=1)

# Unreferenced packs younger than this (STORE clock) are never even
# tombstoned — the window in which an in-flight commit's packs exist before
# their manifest lands. A tombstoned pack must then STAY cold this long again
# before deletion, so total time-to-delete ≈ 2x grace across ≥2 GC runs.
DEFAULT_PACK_GRACE = timedelta(hours=24)
# Floor for non-dry-run pack deletion, mirroring MIN_PHYSICAL_DELAY: below
# this the grace cannot be trusted to outlast a stalled writer's
# packs-landed→manifest-landed gap.
MIN_PACK_GRACE = timedelta(hours=1)

# Mass-delete circuit breaker: refuse a sweep that would delete more than
# this fraction of all listed packs (only when there are at least
# _BREAKER_MIN packs — tiny lakes legitimately delete "most" of very few).
# A bad listing or a broken mark pass must not mass-mutate.
DEFAULT_MAX_DELETE_FRACTION = 0.4
_BREAKER_MIN = 10

# The tombstone ledger (one JSON object) and the store-clock probe.
_TOMBSTONE_KEY = "gc/pack-tombstones"
_CLOCK_PROBE_KEY = "gc/clock-probe"


@dataclass(frozen=True)
class GcReport:
    """What a GC pass did (or, under dry_run, would have done)."""

    dry_run: bool
    swept_catalogs: list[str] = field(default_factory=list)
    kept_catalogs: list[str] = field(default_factory=list)
    swept_packs: list[str] = field(default_factory=list)
    kept_packs: list[str] = field(default_factory=list)
    tombstoned_packs: list[str] = field(default_factory=list)


def collect(
    store: ObjectStore,
    holder_id: str,
    *,
    retain_generations: int = DEFAULT_RETAIN_GENERATIONS,
    dry_run: bool = True,
    lease_ttl_seconds: float = 300.0,
    pack_grace: timedelta = DEFAULT_PACK_GRACE,
    _unsafe_allow_short_grace: bool = False,
) -> GcReport | None:
    """Run one GC pass. Returns None if another runner holds the lease.

    `retain_generations` must exceed the maximum age (in commits) of any
    reader pin — a reader attached to generation N is unaffected as long
    as N stays inside the window. `pack_grace` gates the pack sweep's age
    checks (floored by MIN_PACK_GRACE in non-dry-run mode: below it a
    stalled writer's packs-landed→manifest-landed gap could be swept out
    from under a commit; tests with no concurrent writers may override).
    """
    if retain_generations < 1:
        raise InputValidationError("retain_generations must be >= 1")
    if pack_grace < timedelta(0):
        raise InputValidationError("pack_grace must be non-negative")
    if not dry_run and pack_grace < MIN_PACK_GRACE and not _unsafe_allow_short_grace:
        raise InputValidationError(
            f"pack_grace {pack_grace} is below the {MIN_PACK_GRACE} floor — "
            "an in-flight writer's packs could be swept before its manifest "
            "lands. Raise the grace (or, in tests with no concurrent "
            "writers, pass _unsafe_allow_short_grace=True)."
        )

    lease = Lease(store, holder_id, ttl_seconds=lease_ttl_seconds)
    if not lease.acquire():
        return None
    try:
        return _collect_locked(
            store, lease, retain_generations, dry_run=dry_run, pack_grace=pack_grace
        )
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
    store: ObjectStore,
    lease: Lease,
    retain_generations: int,
    *,
    dry_run: bool,
    pack_grace: timedelta = DEFAULT_PACK_GRACE,
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

    pack_report = _pack_sweep(store, lease, current, kept, dry_run=dry_run, grace=pack_grace)
    return GcReport(
        dry_run=dry_run,
        swept_catalogs=sorted(swept),
        kept_catalogs=sorted(kept),
        swept_packs=pack_report.swept,
        kept_packs=pack_report.kept,
        tombstoned_packs=pack_report.tombstoned,
    )


# --- pack mark-sweep -------------------------------------------------------
#
# Deleting a shared pack destroys EVERY generation referencing it, so this
# path carries two independent defenses that must BOTH fail before data loss:
#
# 1. The mark-set induction (why no committed generation can reference an
#    unmarked pack): manifest entries are FULL (chunk.py invariant) and the
#    dedup source is strictly the base generation's manifest, so any manifest
#    landing after our listing descends through committed bases to an ancestor
#    IN the listing at gen >= floor (marked) — everything novel above that is
#    younger than the run and grace-protected.
# 2. Two-cycle tombstones (the bincache/niks3 first_deleted_at shape): an
#    unreferenced+aged pack is only TOMBSTONED this cycle; deletion requires
#    it to still be unreferenced a full cycle later, with a pre-delete re-HEAD
#    skipping anything whose mtime went young. A pack referenced again is
#    resurrected (tombstone cleared).
#
# All decisions are made by the PURE `decide_pack_sweep` over discriminated
# unions — store I/O happens strictly before (resolve/list) or after
# (execute) the decision, so the safety logic is testable without a store.


@dataclass(frozen=True)
class CommittedWhole:
    """Retained payload key whose marker says transport=whole. No packs."""

    doc: RootDoc


@dataclass(frozen=True)
class CommittedChunked:
    """Retained payload key whose marker says transport=chunked."""

    doc: RootDoc
    manifest: Manifest


@dataclass(frozen=True)
class OrphanManifest:
    """Unmarkered payload object (lost-race orphan) that parses as a manifest.

    Its packs are marked (over-retention is safe); it can never be a dedup
    base, so nothing else depends on it.
    """

    key: str
    manifest: Manifest


@dataclass(frozen=True)
class OrphanOpaque:
    """Unmarkered payload object that is not a manifest. No packs."""

    key: str


ResolvedPayload = CommittedWhole | CommittedChunked | OrphanManifest | OrphanOpaque


@dataclass(frozen=True)
class RefuseSweep:
    """The pack sweep must not run this cycle. Mutually exclusive with deletes."""

    reason: str


@dataclass(frozen=True)
class SweepActions:
    """What this cycle does to packs. Deletion only via a prior-cycle tombstone."""

    marked: frozenset[str]  # pack keys referenced by retained manifests
    tombstone: frozenset[str]  # newly cold: record, do NOT delete
    resurrect: frozenset[str]  # previously cold, now referenced or young again
    delete: frozenset[str]  # cold for a full prior cycle: eligible


PackSweepPlan = RefuseSweep | SweepActions


class TombstoneDoc(BaseModel):
    """The `gc/pack-tombstones` ledger: pack key -> store-clock first-cold time.

    Single-writer under the GC lease. Losing it merely resets coldness —
    packs wait extra cycles; it can never cause a delete.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_id: Literal["ducklake-serverless-tombstones/1"] = Field(
        default="ducklake-serverless-tombstones/1", alias="schema"
    )
    cold_since: dict[str, datetime] = Field(default_factory=dict)

    def to_json_bytes(self) -> bytes:
        """Serialize for the ledger object body."""
        return self.model_dump_json(by_alias=True).encode()

    @classmethod
    def from_json_bytes(cls, data: bytes) -> TombstoneDoc:
        """Parse the ledger; InputValidationError on anything else."""
        try:
            return cls.model_validate_json(data)
        except pydantic.ValidationError as exc:
            raise InputValidationError("tombstone ledger failed validation") from exc


def resolve_payloads(
    store: ObjectStore, current: RootDoc, kept_keys: list[str]
) -> list[ResolvedPayload] | RefuseSweep:
    """Classify every retained payload key: marker-first, sniff-fallback.

    Markers are the exact transport source (zero false negatives, no need to
    GET whole-file bodies). Only unmarkered leftovers — lost-race orphans
    sharing a retained generation number — are sniffed by content. A
    committed CHUNKED generation whose manifest cannot be fetched/parsed is
    grounds for refusal (analog of the missing-payload check); an unreadable
    ORPHAN is logged-and-skipped as opaque (it must not wedge GC forever).
    """
    resolved: list[ResolvedPayload] = []
    for key in kept_keys:
        generation = _generation_of(key)
        if generation is None:
            continue  # unknown object under payload/ — no packs to mark
        doc = _marker_for(store, current, generation)
        if doc is not None and doc.payload_key == key:
            match doc.transport:
                case "whole":
                    resolved.append(CommittedWhole(doc=doc))
                case "chunked":
                    try:
                        manifest = Manifest.from_bytes(store.get(key).body)
                    except (ObjectNotFoundError, InputValidationError) as exc:
                        return RefuseSweep(
                            reason=f"committed chunked generation {generation} at {key} "
                            f"has no readable manifest ({exc}) — refusing to sweep packs"
                        )
                    resolved.append(CommittedChunked(doc=doc, manifest=manifest))
            continue
        orphan = _resolve_orphan(store, key)
        if orphan is not None:
            resolved.append(orphan)
    return resolved


def _resolve_orphan(store: ObjectStore, key: str) -> OrphanManifest | OrphanOpaque | None:
    """Sniff an unmarkered payload object; None if it vanished mid-resolve.

    Size-capped: a multi-GB orphan blob is never a manifest worth GETting.
    """
    try:
        meta = store.head_meta(key)
        body = store.get(key).body if meta.size <= MAX_MANIFEST_BYTES else None
    except ObjectNotFoundError:
        return None  # vanished between listing and now — nothing to mark
    manifest = sniff_manifest(body) if body is not None else None
    if manifest is not None:
        return OrphanManifest(key=key, manifest=manifest)
    return OrphanOpaque(key=key)


def _marker_for(store: ObjectStore, current: RootDoc, generation: int) -> RootDoc | None:
    """The committed marker for `generation`, or None if none exists."""
    if generation == current.generation:
        return current
    try:
        return read_marker(store, generation)
    except (ObjectNotFoundError, pydantic.ValidationError):
        return None


def decide_pack_sweep(
    resolved: list[ResolvedPayload],
    pack_metas: list[ObjectMeta],
    tombstones: TombstoneDoc,
    store_now: datetime,
    *,
    grace: timedelta,
    max_delete_fraction: float = DEFAULT_MAX_DELETE_FRACTION,
) -> PackSweepPlan:
    """The PURE sweep decision. No store I/O — callers resolve/list first.

    Per-pack FSM: absent -> tombstoned(t) -> deleted | resurrected. A pack is
    deleted only when (unreferenced) AND (store-clock age > grace) AND
    (tombstoned at least `grace` ago and still unreferenced). Referenced or
    young packs with a stale tombstone are resurrected. Refusal (a committed
    manifest referencing a pack missing from the listing, or a mass-delete
    anomaly) is mutually exclusive with deletion by type.
    """
    marked: set[str] = set()
    for item in resolved:
        match item:
            case CommittedChunked(manifest=manifest) | OrphanManifest(manifest=manifest):
                marked |= manifest.pack_keys()
            case CommittedWhole() | OrphanOpaque():
                pass

    listed = {m.key: m for m in pack_metas}
    committed_refs = {
        key
        for item in resolved
        if isinstance(item, CommittedChunked)
        for key in item.manifest.pack_keys()
    }
    missing_committed = committed_refs - set(listed)
    if missing_committed:
        # Caller re-HEADs these before refusing (listings are non-atomic);
        # reaching the decision with them still missing is a stop-the-world
        # anomaly: a committed generation cannot be reconstructed.
        return RefuseSweep(
            reason=f"committed manifests reference packs missing from the "
            f"listing: {sorted(missing_committed)[:5]} — refusing to sweep"
        )

    tombstone: set[str] = set()
    resurrect: set[str] = set()
    delete: set[str] = set()
    for key, meta in listed.items():
        cold_since = tombstones.cold_since.get(key)
        # No LastModified means age is unknowable: treat as young (never
        # delete on an unknown age — under-deletion is the safe direction).
        young = meta.last_modified is None or (store_now - meta.last_modified) <= grace
        if key in marked or young:
            if cold_since is not None:
                resurrect.add(key)  # referenced/young again — clear coldness
            continue
        if cold_since is None:
            tombstone.add(key)  # newly cold: record only, never delete now
        elif (store_now - cold_since) > grace:
            delete.add(key)  # cold a full cycle — eligible
        # else: tombstoned recently; leave it aging.

    if len(listed) >= _BREAKER_MIN and len(delete) > max_delete_fraction * len(listed):
        return RefuseSweep(
            reason=f"sweep would delete {len(delete)}/{len(listed)} packs "
            f"(> {max_delete_fraction:.0%}) — a bad listing or mark pass "
            "must not mass-mutate; refusing"
        )
    return SweepActions(
        marked=frozenset(marked),
        tombstone=frozenset(tombstone),
        resurrect=frozenset(resurrect),
        delete=frozenset(delete),
    )


@dataclass(frozen=True)
class _PackReport:
    swept: list[str]
    kept: list[str]
    tombstoned: list[str]


def _store_now(store: ObjectStore) -> datetime:
    """The store's clock, read from a freshly PUT probe object.

    The whole age gate is a comparison between store-issued timestamps —
    the runner's clock never participates (lease.py discipline).
    """
    store.put(_CLOCK_PROBE_KEY, b"t")
    probed = store.head_meta(_CLOCK_PROBE_KEY).last_modified
    if probed is None:
        raise ExternalServiceError(
            "store returns no LastModified — pack age gates cannot work; "
            "disable chunked transport or fix the backend"
        )
    return probed


def _load_tombstones(store: ObjectStore) -> TombstoneDoc:
    try:
        return TombstoneDoc.from_json_bytes(store.get(_TOMBSTONE_KEY).body)
    except ObjectNotFoundError:
        return TombstoneDoc()
    except InputValidationError:
        # A corrupt ledger resets coldness — packs wait extra cycles (safe).
        return TombstoneDoc()


def _pack_sweep(
    store: ObjectStore,
    lease: Lease,
    current: RootDoc,
    kept_keys: list[str],
    *,
    dry_run: bool,
    grace: timedelta = DEFAULT_PACK_GRACE,
) -> _PackReport:
    """Resolve -> decide (pure) -> execute the pack mark-sweep.

    No empty-listing fast path on purpose: an empty packs/ listing while a
    committed chunked manifest references packs is the WORST anomaly, and it
    must hit the refusal check, not a shortcut.
    """
    pack_metas = store.list_meta(PACKS_PREFIX)
    resolved = resolve_payloads(store, current, kept_keys)
    if isinstance(resolved, RefuseSweep):
        raise ExternalServiceError(resolved.reason)

    # Re-HEAD committed references missing from the listing BEFORE deciding:
    # the payload/ and packs/ listings are non-atomic, so a manifest that
    # landed between them legitimately references newer packs.
    committed_refs = {
        key
        for item in resolved
        if isinstance(item, CommittedChunked)
        for key in item.manifest.pack_keys()
    }
    if not pack_metas and not committed_refs:
        # Whole-file-only lake: nothing to sweep, and no tombstone/clock
        # churn. (Empty packs/ WITH committed chunked references still flows
        # through to the refusal check below — the worst anomaly must never
        # take a shortcut.)
        return _PackReport(swept=[], kept=[], tombstoned=[])
    listed_keys = {m.key for m in pack_metas}
    for key in sorted(committed_refs - listed_keys):
        with contextlib.suppress(ObjectNotFoundError):  # decide_pack_sweep refuses on it
            pack_metas.append(store.head_meta(key))

    tombstones = _load_tombstones(store)
    plan = decide_pack_sweep(resolved, pack_metas, tombstones, _store_now(store), grace=grace)
    match plan:
        case RefuseSweep(reason=reason):
            raise ExternalServiceError(reason)
        case SweepActions():
            pass

    kept = sorted(k.key for k in pack_metas if k.key not in plan.delete)
    if dry_run:
        return _PackReport(swept=sorted(plan.delete), kept=kept, tombstoned=sorted(plan.tombstone))

    # Tombstone write is the commit point: it precedes any delete, and a
    # failure aborts the cycle with zero deletes.
    new_cold = {k: v for k, v in tombstones.cold_since.items() if k not in plan.resurrect}
    for key in plan.tombstone:
        new_cold[key] = _store_now(store)
    for key in plan.delete:
        new_cold.pop(key, None)
    store.put(_TOMBSTONE_KEY, TombstoneDoc(cold_since=new_cold).to_json_bytes())

    swept: list[str] = []
    for key in sorted(plan.delete):
        # Pre-delete re-HEAD: skip (and keep) any pack whose mtime went young
        # — a stalled writer's refresh-PUT raced us (the GC half of the
        # stalled-writer defense).
        try:
            meta = store.head_meta(key)
        except ObjectNotFoundError:
            continue  # already gone
        if meta.last_modified is not None and (_store_now(store) - meta.last_modified) <= grace:
            continue
        store.delete(key)
        swept.append(key)
        if len(swept) % _RENEW_EVERY == 0 and not lease.renew():
            raise ExternalServiceError(
                "lost the maintenance lease mid-pack-sweep — stopping "
                "(deletes so far are tombstoned-cold packs; safe)"
            )
    return _PackReport(
        swept=swept,
        kept=sorted(set(m.key for m in pack_metas) - set(swept)),
        tombstoned=sorted(plan.tombstone),
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
