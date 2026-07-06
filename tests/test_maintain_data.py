"""Data-plane maintenance: expire + cleanup + orphan deletion, safely ordered."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from ducklake_serverless.gc import maintain_data
from ducklake_serverless.lease import Lease
from ducklake_serverless.objectstore import InMemoryObjectStore
from ducklake_serverless.root import read_root
from ducklake_serverless.session import Lake

if TYPE_CHECKING:
    from pathlib import Path

NOW = timedelta(0)  # age gates disabled: everything is old enough


@pytest.fixture
def store() -> InMemoryObjectStore:
    return InMemoryObjectStore()


@pytest.fixture
def data(tmp_path: Path) -> Path:
    d = tmp_path / "data"
    d.mkdir()
    return d


@pytest.fixture
def lake(tmp_path: Path, store: InMemoryObjectStore, data: Path) -> Lake:
    work = tmp_path / "work"
    work.mkdir()
    return Lake(store, workdir=work, data_path=str(data))


def churn(lake: Lake) -> None:
    """Create real Parquet churn: a large insert fully deleted and replaced."""
    lake.bootstrap()
    with lake.transaction() as tx:
        tx.sql("CREATE TABLE t (id INTEGER)")
    with lake.transaction() as tx:
        tx.sql("INSERT INTO t SELECT range FROM range(100000)")
    with lake.transaction() as tx:
        tx.sql("DELETE FROM t")
    with lake.transaction() as tx:
        tx.sql("INSERT INTO t SELECT range FROM range(50000)")


def parquets(data: Path) -> int:
    return len(list(data.rglob("*.parquet")))


def test_dry_run_reports_but_deletes_nothing(
    lake: Lake, store: InMemoryObjectStore, data: Path
) -> None:
    churn(lake)
    before = parquets(data)
    report = maintain_data(
        lake, store, "gc", expire_older_than=NOW, physical_delete_delay=NOW, dry_run=True
    )
    assert report is not None and report.dry_run
    assert report.snapshots_expired  # churn produced expirable history
    assert parquets(data) == before  # nothing deleted


def test_two_phase_reclaim_preserves_live_rows(
    lake: Lake, store: InMemoryObjectStore, data: Path
) -> None:
    """Run 1 expires + schedules; run 2 physically reclaims. Live data intact."""
    churn(lake)
    before = parquets(data)

    r1 = maintain_data(
        lake, store, "gc", expire_older_than=NOW, physical_delete_delay=NOW, dry_run=False
    )
    assert r1 is not None and r1.snapshots_expired
    r2 = maintain_data(
        lake, store, "gc", expire_older_than=NOW, physical_delete_delay=NOW, dry_run=False
    )
    assert r2 is not None and r2.files_cleaned  # run 1's schedule, reclaimed

    assert parquets(data) < before
    with lake.reader() as con:
        assert con.execute("SELECT count(*) FROM t") == [(50000,)]


def test_orphan_parquet_from_aborted_transaction_reclaimed(
    lake: Lake, store: InMemoryObjectStore, data: Path
) -> None:
    """A Parquet file referenced by NO catalog (aborted/lost commit) is deleted."""
    churn(lake)
    table_dir = next(data.rglob("*.parquet")).parent
    orphan = table_dir / "ducklake-fake-orphan.parquet"
    orphan.write_bytes(b"not really parquet, just unreferenced bytes")

    report = maintain_data(
        lake, store, "gc", expire_older_than=NOW, physical_delete_delay=NOW, dry_run=False
    )
    assert report is not None
    assert any("fake-orphan" in path for path in report.orphans_deleted)
    assert not orphan.exists()
    with lake.reader() as con:
        assert con.execute("SELECT count(*) FROM t") == [(50000,)]


def test_maintenance_advances_the_root(lake: Lake, store: InMemoryObjectStore, data: Path) -> None:
    """Maintenance is a normal commit: readers resolve the maintained catalog."""
    churn(lake)
    before, _ = read_root(store)
    report = maintain_data(
        lake, store, "gc", expire_older_than=NOW, physical_delete_delay=NOW, dry_run=False
    )
    assert report is not None
    after, _ = read_root(store)
    assert after.generation == before.generation + 1
    with lake.reader() as con:
        # Expired snapshots are gone from the published catalog.
        assert len(con.snapshot_ids()) < 6


def test_yields_to_lease_holder(lake: Lake, store: InMemoryObjectStore) -> None:
    churn(lake)
    rival = Lease(store, "rival", ttl_seconds=60)
    assert rival.acquire()
    assert maintain_data(lake, store, "gc", dry_run=True) is None


def test_default_age_gates_touch_nothing_fresh(
    lake: Lake, store: InMemoryObjectStore, data: Path
) -> None:
    """With production defaults, just-written history is untouchable."""
    churn(lake)
    before = parquets(data)
    report = maintain_data(lake, store, "gc", dry_run=False)  # default gates
    assert report is not None
    assert not report.snapshots_expired
    assert not report.files_cleaned
    assert not report.orphans_deleted
    assert parquets(data) == before
