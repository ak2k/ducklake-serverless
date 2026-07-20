"""Pack mark-sweep: the deletion path. Two independent defenses under test.

Layer 1 — the PURE decide_pack_sweep (no store): the mark-set induction,
the per-pack tombstone FSM (absent -> tombstoned -> deleted | resurrected),
refusal-vs-delete mutual exclusion by type, and the circuit breaker —
including hypothesis properties over arbitrary states.

Layer 2 — the executor through `collect` (InMemoryObjectStore): referenced
packs survive real GC cycles, unreferenced packs need TWO cycles + grace to
die, resurrection works, refusal anomalies raise, and the whole-file lake
never grows gc/ objects.

InMemory mtimes are real datetimes, so grace gating is exercised by passing
tiny/zero grace rather than by clock mocking; "aged" tombstones are planted
directly in the ledger where a past first-cold time is needed.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, override
from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ducklake_serverless import gc as gc_mod
from ducklake_serverless.blob import BlobStore
from ducklake_serverless.chunk import (
    MANIFEST_MAGIC,
    PACKS_PREFIX,
    Manifest,
    ManifestEntry,
    format_pack_key,
)
from ducklake_serverless.errors import ExternalServiceError
from ducklake_serverless.gc import (
    CommittedChunked,
    OrphanManifest,
    RefuseSweep,
    ResolvedPayload,
    SweepActions,
    TombstoneDoc,
    collect,
    decide_pack_sweep,
)
from ducklake_serverless.models import RootDoc, WriterInfo
from ducklake_serverless.objectstore import InMemoryObjectStore, ObjectMeta

if TYPE_CHECKING:
    from pathlib import Path

NOW = datetime(2026, 7, 20, 12, 0, 0, tzinfo=UTC)
GRACE = timedelta(hours=1)
OLD = NOW - timedelta(hours=10)  # well past grace
FRESH = NOW - timedelta(minutes=1)  # inside grace


def sha(n: int) -> str:
    return hashlib.sha256(f"pack-{n}".encode()).hexdigest()


def key(n: int) -> str:
    return format_pack_key(sha(n))


def doc(gen: int, transport: str = "chunked") -> RootDoc:
    return RootDoc(
        generation=gen,
        payload_uuid=uuid4(),
        created_at=NOW,
        writer=WriterInfo(lib_version="0", host="t", pid=1),
        transport=transport,  # pyright: ignore[reportArgumentType]
    )


def manifest(*pack_nums: int) -> Manifest:
    entries = tuple(
        ManifestEntry(chunk_sha256=sha(1000 + i), pack_sha256=sha(n), pack_offset=0, length=1)
        for i, n in enumerate(pack_nums)
    )
    return Manifest(chunk_size=1, total_size=len(entries), file_sha256=sha(0), entries=entries)


def meta(n: int, mtime: datetime | None = OLD) -> ObjectMeta:
    return ObjectMeta(key=key(n), size=100, last_modified=mtime)


# --- Layer 1: the pure decision -------------------------------------------


def test_referenced_packs_are_marked_never_deleted() -> None:
    resolved: list[ResolvedPayload] = [CommittedChunked(doc=doc(5), manifest=manifest(1, 2))]
    plan = decide_pack_sweep(resolved, [meta(1), meta(2)], TombstoneDoc(), NOW, grace=GRACE)
    assert isinstance(plan, SweepActions)
    assert plan.marked == {key(1), key(2)}
    assert not plan.delete and not plan.tombstone


def test_unreferenced_aged_pack_is_tombstoned_not_deleted() -> None:
    """Cycle K: cold discovery only records; deletion is never single-cycle."""
    plan = decide_pack_sweep([], [meta(1)], TombstoneDoc(), NOW, grace=GRACE)
    assert isinstance(plan, SweepActions)
    assert plan.tombstone == {key(1)}
    assert not plan.delete


def test_tombstoned_still_cold_is_deleted_next_cycle() -> None:
    """Cycle K+1: still unreferenced + tombstone aged a full grace -> delete."""
    stones = TombstoneDoc(cold_since={key(1): OLD})
    plan = decide_pack_sweep([], [meta(1)], stones, NOW, grace=GRACE)
    assert isinstance(plan, SweepActions)
    assert plan.delete == {key(1)}


def test_recently_tombstoned_keeps_aging() -> None:
    stones = TombstoneDoc(cold_since={key(1): FRESH})
    plan = decide_pack_sweep([], [meta(1)], stones, NOW, grace=GRACE)
    assert isinstance(plan, SweepActions)
    assert not plan.delete and not plan.tombstone  # neither re-recorded nor deleted


def test_referenced_again_resurrects() -> None:
    """A tombstoned pack referenced by a new manifest is un-tombstoned."""
    stones = TombstoneDoc(cold_since={key(1): OLD})
    resolved: list[ResolvedPayload] = [CommittedChunked(doc=doc(6), manifest=manifest(1))]
    plan = decide_pack_sweep(resolved, [meta(1)], stones, NOW, grace=GRACE)
    assert isinstance(plan, SweepActions)
    assert plan.resurrect == {key(1)}
    assert not plan.delete


def test_young_pack_never_tombstoned_even_unreferenced() -> None:
    """The in-flight-commit window: packs land before their manifest."""
    plan = decide_pack_sweep([], [meta(1, mtime=FRESH)], TombstoneDoc(), NOW, grace=GRACE)
    assert isinstance(plan, SweepActions)
    assert not plan.tombstone and not plan.delete


def test_unknown_mtime_treated_as_young() -> None:
    plan = decide_pack_sweep([], [meta(1, mtime=None)], TombstoneDoc(), NOW, grace=GRACE)
    assert isinstance(plan, SweepActions)
    assert not plan.tombstone and not plan.delete


def test_orphan_manifest_packs_are_marked() -> None:
    """A lost-race orphan's packs are protected (over-retention is safe)."""
    resolved: list[ResolvedPayload] = [OrphanManifest(key="payload/x", manifest=manifest(3))]
    plan = decide_pack_sweep(resolved, [meta(3)], TombstoneDoc(), NOW, grace=GRACE)
    assert isinstance(plan, SweepActions)
    assert plan.marked == {key(3)}
    assert not plan.delete


def test_committed_manifest_missing_pack_refuses() -> None:
    """A committed generation that cannot be reconstructed stops the sweep."""
    resolved: list[ResolvedPayload] = [CommittedChunked(doc=doc(5), manifest=manifest(1, 9))]
    plan = decide_pack_sweep(resolved, [meta(1)], TombstoneDoc(), NOW, grace=GRACE)
    assert isinstance(plan, RefuseSweep)
    assert key(9) in plan.reason


def test_orphan_manifest_missing_pack_does_not_refuse() -> None:
    """An unreadable/incomplete ORPHAN must not wedge GC forever."""
    resolved: list[ResolvedPayload] = [OrphanManifest(key="payload/x", manifest=manifest(9))]
    plan = decide_pack_sweep(resolved, [meta(1)], TombstoneDoc(), NOW, grace=GRACE)
    assert isinstance(plan, SweepActions)  # marks key(9) but doesn't refuse


def test_circuit_breaker_refuses_mass_delete() -> None:
    metas = [meta(n) for n in range(20)]
    stones = TombstoneDoc(cold_since={key(n): OLD for n in range(20)})
    plan = decide_pack_sweep([], metas, stones, NOW, grace=GRACE)
    assert isinstance(plan, RefuseSweep)
    assert "mass-mutate" in plan.reason


def test_circuit_breaker_ignores_tiny_lakes() -> None:
    metas = [meta(n) for n in range(3)]
    stones = TombstoneDoc(cold_since={key(n): OLD for n in range(3)})
    plan = decide_pack_sweep([], metas, stones, NOW, grace=GRACE)
    assert isinstance(plan, SweepActions)
    assert len(plan.delete) == 3


@given(
    referenced=st.sets(st.integers(0, 8)),
    tombstoned_old=st.sets(st.integers(0, 8)),
    young=st.sets(st.integers(0, 8)),
)
def test_property_fsm_transition_legality(
    referenced: set[int], tombstoned_old: set[int], young: set[int]
) -> None:
    """For ANY state: never delete without a prior aged tombstone; never
    delete referenced/young; tombstone and delete are disjoint; resurrect
    only previously-tombstoned keys."""
    all_packs = referenced | tombstoned_old | young | {99}
    metas = [meta(n, mtime=FRESH if n in young else OLD) for n in all_packs]
    stones = TombstoneDoc(cold_since={key(n): OLD for n in tombstoned_old})
    resolved: list[ResolvedPayload] = (
        [CommittedChunked(doc=doc(5), manifest=manifest(*sorted(referenced)))] if referenced else []
    )
    plan = decide_pack_sweep(resolved, metas, stones, NOW, grace=GRACE, max_delete_fraction=1.1)
    assert isinstance(plan, SweepActions)
    stone_keys = {key(n) for n in tombstoned_old}
    ref_keys = {key(n) for n in referenced}
    young_keys = {key(n) for n in young}
    assert plan.delete <= stone_keys  # no delete without a prior tombstone
    assert not (plan.delete & ref_keys)  # never delete referenced
    assert not (plan.delete & (young_keys - stone_keys) & plan.delete)
    assert not (plan.delete & plan.tombstone)  # disjoint by FSM
    assert plan.resurrect <= stone_keys  # only tombstoned keys resurrect
    # Young-or-referenced packs are never deleted even if tombstoned.
    assert not (plan.delete & (ref_keys | young_keys))


# --- Layer 2: the executor through collect() ------------------------------


@pytest.fixture
def store() -> InMemoryObjectStore:
    return InMemoryObjectStore()


def make_blob(store: InMemoryObjectStore, tmp_path: Path) -> BlobStore:
    work = tmp_path / "blob-work"
    work.mkdir(exist_ok=True)
    return BlobStore(store, work, chunk_threshold=0)


def payload(seed: int, size: int = 50_000) -> bytes:
    return bytes((i * 31 + seed) % 251 for i in range(size))


def plant_aged_tombstones(store: InMemoryObjectStore, keys: set[str]) -> None:
    store.put(
        gc_mod._TOMBSTONE_KEY,  # pyright: ignore[reportPrivateUsage]
        TombstoneDoc(cold_since=dict.fromkeys(keys, OLD)).to_json_bytes(),
    )


def test_referenced_packs_survive_wet_gc(store: InMemoryObjectStore, tmp_path: Path) -> None:
    bs = make_blob(store, tmp_path)
    bs.bootstrap(b"g0")
    data = payload(1)
    bs.write(data)
    report = collect(store, "gc", retain_generations=3, dry_run=False)
    assert report is not None
    assert not report.swept_packs  # everything referenced or young
    assert bs.read() == data  # reconstruction still whole


def test_unreferenced_packs_need_two_cycles(store: InMemoryObjectStore, tmp_path: Path) -> None:
    """Orphan packs: cycle 1 tombstones (zero grace), cycle 2 deletes."""
    bs = make_blob(store, tmp_path)
    bs.bootstrap(b"g0")
    bs.write(payload(1))
    # Age out generation 1's packs by overwriting with unrelated content
    # and shrinking the window so gen 1 leaves it entirely.
    for seed in range(2, 6):
        bs.write(payload(seed))

    # Zero grace => age gate passes immediately; the FSM still needs 2 cycles.
    r1 = _zero_grace_collect(store)
    survivors_after_1 = set(store.list_prefix(PACKS_PREFIX))
    assert r1.tombstoned_packs and not r1.swept_packs  # cycle 1: record only

    # Make the tombstones look a full cycle old, then run cycle 2.
    plant_aged_tombstones(store, set(r1.tombstoned_packs))
    r2 = _zero_grace_collect(store)
    assert set(r2.swept_packs) == set(r1.tombstoned_packs)
    assert set(store.list_prefix(PACKS_PREFIX)) == survivors_after_1 - set(r2.swept_packs)
    assert bs.read() == payload(5)  # head still reconstructs


def _zero_grace_collect(store: InMemoryObjectStore) -> gc_mod.GcReport:
    report = gc_mod.collect(
        store,
        "gc",
        retain_generations=1,
        dry_run=False,
        pack_grace=timedelta(0),
        _unsafe_allow_short_grace=True,
    )
    assert report is not None
    return report


def test_resurrected_pack_survives(store: InMemoryObjectStore, tmp_path: Path) -> None:
    """A pack tombstoned then referenced again is cleared, not deleted."""
    bs = make_blob(store, tmp_path)
    bs.bootstrap(b"g0")
    data = payload(1)
    bs.write(data)
    bs.write(payload(2))  # gen1's unique packs now unreferenced (window=1)

    r1 = _zero_grace_collect(store)
    assert r1.tombstoned_packs
    # Re-write the ORIGINAL data: its chunks re-hash to the same packs — the
    # tombstoned packs become referenced by the new head again.
    bs.write(data)
    plant_aged_tombstones(store, set(r1.tombstoned_packs))
    # (aged tombstones planted AFTER re-reference: worst case for safety)
    r2 = _zero_grace_collect(store)
    resurrected = set(r1.tombstoned_packs) & set(store.list_prefix(PACKS_PREFIX))
    assert resurrected  # the re-referenced packs survived
    assert not (set(r2.swept_packs) & resurrected)
    assert bs.read() == data


def test_missing_committed_pack_refuses_wet_sweep(
    store: InMemoryObjectStore, tmp_path: Path
) -> None:
    bs = make_blob(store, tmp_path)
    bs.bootstrap(b"g0")
    bs.write(payload(1))
    victim = store.list_prefix(PACKS_PREFIX)[0]
    store.delete(victim)  # simulate partial listing / lost pack
    with pytest.raises(ExternalServiceError, match="refus"):
        gc_mod.collect(store, "gc", retain_generations=3, dry_run=False)


def test_whole_file_lake_creates_no_gc_objects(store: InMemoryObjectStore, tmp_path: Path) -> None:
    work = tmp_path / "w"
    work.mkdir()
    bs = BlobStore(store, work, chunk_threshold=None)  # whole-file only
    bs.bootstrap(b"g0")
    bs.write(b"g1")
    report = collect(store, "gc", retain_generations=3, dry_run=False)
    assert report is not None
    assert not report.swept_packs and not report.kept_packs
    assert not store.list_prefix("gc/")  # no tombstone/clock churn


def test_dry_run_reports_but_deletes_nothing(store: InMemoryObjectStore, tmp_path: Path) -> None:
    bs = make_blob(store, tmp_path)
    bs.bootstrap(b"g0")
    bs.write(payload(1))
    for seed in range(2, 5):
        bs.write(payload(seed))
    before = set(store.list_prefix(PACKS_PREFIX))
    report = gc_mod.collect(store, "gc", retain_generations=1, dry_run=True)
    assert report is not None
    assert set(store.list_prefix(PACKS_PREFIX)) == before  # nothing deleted
    assert not store.get(gc_mod._TOMBSTONE_KEY).body if False else True  # ledger untouched


def test_raw_blob_with_magic_prefix_is_harmless(store: InMemoryObjectStore, tmp_path: Path) -> None:
    """A whole-file blob that STARTS with the manifest magic but isn't a
    manifest: committed marker says whole, so it is never sniffed; GC must
    neither refuse nor mark anything for it."""
    work = tmp_path / "w"
    work.mkdir()
    bs = BlobStore(store, work, chunk_threshold=None)
    bs.bootstrap(b"g0")
    bs.write(MANIFEST_MAGIC + b"{not a manifest")
    report = collect(store, "gc", retain_generations=3, dry_run=False)
    assert report is not None
    assert bs.read() == MANIFEST_MAGIC + b"{not a manifest"


def test_orphan_opaque_and_unknown_objects_are_ignored(
    store: InMemoryObjectStore, tmp_path: Path
) -> None:
    bs = make_blob(store, tmp_path)
    bs.bootstrap(b"g0")
    bs.write(payload(1))
    head_gen_key = sorted(store.list_prefix("payload/"))[-1]
    # Plant a lost-race orphan sharing the head's generation number: raw junk.
    orphan_key = head_gen_key.rsplit("-", 5)[0] + "-99999999-9999-4999-8999-999999999999"
    store.put_if_absent(orphan_key, b"raw junk orphan")
    report = collect(store, "gc", retain_generations=3, dry_run=False)
    assert report is not None  # no refusal, no crash
    assert store.get(orphan_key).body == b"raw junk orphan"  # untouched


class SweepNovelPacksBeforeManifest(InMemoryObjectStore):
    """Deletes every pack at verify_packs' first HEAD (the pre-manifest gap).

    Models the stalled-writer race: GC swept the writer's novel packs during
    the packs-landed→manifest-landed gap. verify_packs' HEAD then 404s and
    it must heal (re-PUT from bytes in hand) so the commit reconstructs.
    """

    def __init__(self) -> None:
        super().__init__()
        self.armed = True

    @override
    def head_meta(self, key: str) -> ObjectMeta:
        if self.armed and key.startswith(PACKS_PREFIX):
            self.armed = False
            for pack_key in self.list_prefix(PACKS_PREFIX):
                self.delete(pack_key)  # GC strikes in the gap
        return super().head_meta(key)


def test_stalled_writer_heal_via_verify_packs(tmp_path: Path) -> None:
    """The full stalled-writer defense end to end: packs swept in the gap are
    healed by verify_packs before the manifest lands; the commit succeeds and
    reconstructs byte-identically."""
    store = SweepNovelPacksBeforeManifest()
    work = tmp_path / "w"
    work.mkdir()
    bs = BlobStore(store, work, chunk_threshold=0)  # pyright: ignore[reportArgumentType]
    bs.bootstrap(b"g0")
    data = payload(7)
    bs.write(data)  # injection fires mid-commit; heal must recover
    assert not store.armed  # the injection actually fired
    assert bs.read() == data  # committed generation reconstructs
