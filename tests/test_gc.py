"""GC contract: retention window, current-generation immunity, dry-run safety."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from ducklake_serverless.engine import LakeConnection
from ducklake_serverless.errors import ExternalServiceError
from ducklake_serverless.gc import collect
from ducklake_serverless.lease import Lease
from ducklake_serverless.models import HintDoc, parse_catalog_key
from ducklake_serverless.objectstore import InMemoryObjectStore
from ducklake_serverless.root import ROOT_HINT_KEY, resolve_head
from ducklake_serverless.session import Lake

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def store() -> InMemoryObjectStore:
    return InMemoryObjectStore()


@pytest.fixture
def lake(tmp_path: Path, store: InMemoryObjectStore) -> Lake:
    data = tmp_path / "data"
    data.mkdir()
    work = tmp_path / "work"
    work.mkdir()
    return Lake(store, workdir=work, data_path=str(data))


def commit_n(lake: Lake, n: int) -> None:
    for i in range(n):
        with lake.transaction() as tx:
            tx.sql("INSERT INTO t VALUES (?)", (i,))


def setup_lake_with_history(lake: Lake, commits: int) -> None:
    lake.bootstrap()
    with lake.transaction() as tx:
        tx.sql("CREATE TABLE t (v INTEGER)")
    commit_n(lake, commits)


def test_dry_run_deletes_nothing(lake: Lake, store: InMemoryObjectStore) -> None:
    setup_lake_with_history(lake, commits=8)  # generations 0..9
    before = set(store.list_prefix("catalog/"))
    report = collect(store, "gc-test", retain_generations=3, dry_run=True)
    assert report is not None and report.dry_run
    assert set(store.list_prefix("catalog/")) == before
    assert len(report.swept_catalogs) == 7  # 0..6 outside window {7,8,9}


def test_sweep_respects_retention_window(lake: Lake, store: InMemoryObjectStore) -> None:
    setup_lake_with_history(lake, commits=8)  # generations 0..9
    report = collect(store, "gc-test", retain_generations=3, dry_run=False)
    assert report is not None
    remaining = store.list_prefix("catalog/")
    generations = sorted(parse_catalog_key(k)[0] for k in remaining)
    assert generations == [7, 8, 9]


def test_current_generation_never_swept(lake: Lake, store: InMemoryObjectStore) -> None:
    setup_lake_with_history(lake, commits=3)
    report = collect(store, "gc-test", retain_generations=1, dry_run=False)
    assert report is not None
    current, _ = resolve_head(store)
    assert store.list_prefix("catalog/") == [current.catalog_key]


def test_lost_cas_orphans_outside_window_are_swept(
    lake: Lake, store: InMemoryObjectStore, tmp_path: Path
) -> None:
    setup_lake_with_history(lake, commits=8)
    # Plant an orphan: a catalog uploaded by a loser whose CAS never landed.
    current, _ = resolve_head(store)
    orphan_key = "catalog/cat-00000002-99999999-9999-4999-8999-999999999999.duckdb"
    store.put_if_absent(orphan_key, store.get(current.catalog_key).body)

    report = collect(store, "gc-test", retain_generations=3, dry_run=False)
    assert report is not None
    assert orphan_key in report.swept_catalogs
    assert orphan_key not in store.list_prefix("catalog/")


def test_unknown_objects_under_catalog_prefix_kept(lake: Lake, store: InMemoryObjectStore) -> None:
    setup_lake_with_history(lake, commits=2)
    store.put_if_absent("catalog/README.txt", b"do not delete me")
    report = collect(store, "gc-test", retain_generations=1, dry_run=False)
    assert report is not None
    assert "catalog/README.txt" in report.kept_catalogs
    assert "catalog/README.txt" in store.list_prefix("catalog/")


def test_gc_yields_to_lease_holder(lake: Lake, store: InMemoryObjectStore) -> None:
    setup_lake_with_history(lake, commits=2)
    rival = Lease(store, "rival", ttl_seconds=60)
    assert rival.acquire()
    assert collect(store, "gc-test", dry_run=True) is None


def test_reader_pinned_inside_window_survives_gc(
    lake: Lake, store: InMemoryObjectStore, tmp_path: Path
) -> None:
    """The P3 gate: a reader on an old-but-retained generation still works."""
    setup_lake_with_history(lake, commits=5)  # generations 0..6
    pinned, _ = resolve_head(store)  # pin generation 6 (current)
    commit_n(lake, 2)  # advance to 8; pinned is now 2 behind

    report = collect(store, "gc-test", retain_generations=3, dry_run=False)
    assert report is not None  # window {6,7,8} — pinned generation retained

    # The pinned generation must still be fetchable and attachable.
    verify = Lake(store, workdir=tmp_path / "pinned", data_path=str(tmp_path / "data"))
    (tmp_path / "pinned").mkdir()
    path = verify._cache.fetch_copy(pinned.generation, pinned.catalog_uuid)  # pyright: ignore[reportPrivateUsage]
    con = LakeConnection(path, data_path=None, read_only=True)
    rows = con.execute("SELECT count(*) FROM t")
    assert rows == [(5,)]  # the state as of the pinned generation
    con.abandon()


def test_sweep_refuses_when_root_names_missing_catalog(
    lake: Lake, store: InMemoryObjectStore
) -> None:
    """A root pointing at a nonexistent catalog means the listing (or the

    root) cannot be trusted — non-dry-run must refuse, not sweep.
    """
    setup_lake_with_history(lake, commits=3)
    current, _ = resolve_head(store)
    store.delete(current.catalog_key)  # simulate corruption/partial listing

    with pytest.raises(ExternalServiceError, match="refusing to sweep"):
        collect(store, "gc-test", retain_generations=1, dry_run=False)


def test_absurd_hint_does_not_cause_a_wrong_sweep(lake: Lake, store: InMemoryObjectStore) -> None:
    """A poison-high hint names no marker; resolve_head rediscovers the true

    head, so GC sweeps against reality — never against a fabricated head.
    """
    setup_lake_with_history(lake, commits=8)  # generations 0..9
    store.put(ROOT_HINT_KEY, HintDoc(generation=9999).to_json_bytes())
    report = collect(store, "gc-test", retain_generations=3, dry_run=False)
    assert report is not None
    remaining = sorted(parse_catalog_key(k)[0] for k in store.list_prefix("catalog/"))
    assert remaining == [7, 8, 9]  # correct window despite the poison hint


def test_gc_never_sweeps_markers(lake: Lake, store: InMemoryObjectStore) -> None:
    """Markers are immortal — GC touches catalog/ only, never roots/."""
    setup_lake_with_history(lake, commits=8)
    markers_before = set(store.list_prefix("roots/"))
    collect(store, "gc-test", retain_generations=1, dry_run=False)
    assert set(store.list_prefix("roots/")) == markers_before  # all 10 markers survive
    # And every generation 0..9 is still resolvable/attachable via its marker.
    for gen in range(10):
        assert f"roots/{gen:08d}" in markers_before


def test_stale_hint_reader_recovers_after_catalog_sweep(
    lake: Lake, store: InMemoryObjectStore
) -> None:
    """A reader whose hint lags below the retention floor still resolves head:

    GC advanced the hint, and forward-probe over immortal markers finds it.
    """
    setup_lake_with_history(lake, commits=8)  # 0..9
    collect(store, "gc-test", retain_generations=3, dry_run=False)  # sweeps catalogs 0..6
    # Simulate a reader arriving with a hint pointing at a swept generation.
    store.put(ROOT_HINT_KEY, HintDoc(generation=2).to_json_bytes())
    with lake.reader() as con:
        assert con.execute("SELECT count(*) FROM t") == [(8,)]  # head data, recovered
