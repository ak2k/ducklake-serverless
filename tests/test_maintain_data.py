"""Data-plane maintenance: expire + cleanup + orphan deletion, safely ordered."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from ducklake_serverless.engine import LakeConnection
from ducklake_serverless.errors import ConflictAbortError, InputValidationError
from ducklake_serverless.gc import maintain_data
from ducklake_serverless.lease import Lease
from ducklake_serverless.models import ROOTS_PREFIX
from ducklake_serverless.objectstore import GetResult, InMemoryObjectStore
from ducklake_serverless.root import resolve_head
from ducklake_serverless.session import Lake
from tests.conftest import lake_churn as churn

if TYPE_CHECKING:
    from pathlib import Path

NOW = timedelta(0)  # age gates disabled: everything is old enough

# Zero physical delay is below the production floor; these tests have no
# concurrent writers, so the in-flight-staging hazard the floor guards
# against cannot occur.
UNSAFE = {"_unsafe_allow_short_delay": True}


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
        lake, store, "gc", expire_older_than=NOW, physical_delete_delay=NOW, dry_run=False, **UNSAFE
    )
    assert r1 is not None and r1.snapshots_expired
    r2 = maintain_data(
        lake, store, "gc", expire_older_than=NOW, physical_delete_delay=NOW, dry_run=False, **UNSAFE
    )
    assert r2 is not None and r2.files_cleaned  # run 1's schedule, reclaimed

    assert parquets(data) < before
    with lake.reader() as con:
        # sum() forces real Parquet scans; count(*) answers from catalog
        # metadata and would pass even if the files were wrongly deleted.
        assert con.execute("SELECT count(*), sum(id) FROM t") == [(50000, 1249975000)]


def test_orphan_parquet_from_aborted_transaction_reclaimed(
    lake: Lake, store: InMemoryObjectStore, data: Path
) -> None:
    """A Parquet file referenced by NO catalog (aborted/lost commit) is deleted."""
    churn(lake)
    table_dir = next(data.rglob("*.parquet")).parent
    orphan = table_dir / "ducklake-fake-orphan.parquet"
    orphan.write_bytes(b"not really parquet, just unreferenced bytes")

    report = maintain_data(
        lake, store, "gc", expire_older_than=NOW, physical_delete_delay=NOW, dry_run=False, **UNSAFE
    )
    assert report is not None
    assert any("fake-orphan" in path for path in report.orphans_deleted)
    assert not orphan.exists()
    with lake.reader() as con:
        # sum() forces real Parquet scans; count(*) answers from catalog
        # metadata and would pass even if the files were wrongly deleted.
        assert con.execute("SELECT count(*), sum(id) FROM t") == [(50000, 1249975000)]


def test_maintenance_advances_the_root(lake: Lake, store: InMemoryObjectStore, data: Path) -> None:
    """Maintenance is a normal commit: readers resolve the maintained catalog."""
    churn(lake)
    with lake.reader() as con:
        snapshots_before = len(con.snapshot_ids())
    before, _ = resolve_head(store)
    report = maintain_data(
        lake, store, "gc", expire_older_than=NOW, physical_delete_delay=NOW, dry_run=False, **UNSAFE
    )
    assert report is not None
    after, _ = resolve_head(store)
    assert after.generation == before.generation + 1
    with lake.reader() as con:
        # Expired snapshots are gone from the published catalog.
        assert len(con.snapshot_ids()) < snapshots_before


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


def test_negative_timedeltas_rejected(lake: Lake, store: InMemoryObjectStore) -> None:
    churn(lake)
    with pytest.raises(InputValidationError, match="non-negative"):
        maintain_data(lake, store, "gc", expire_older_than=timedelta(seconds=-1))
    with pytest.raises(InputValidationError, match="non-negative"):
        maintain_data(lake, store, "gc", physical_delete_delay=timedelta(seconds=-1))


def test_short_delay_refused_without_explicit_opt_in(
    lake: Lake, store: InMemoryObjectStore
) -> None:
    """The floor guards in-flight writers' staged Parquet from the orphan pass."""
    churn(lake)
    with pytest.raises(InputValidationError, match="floor"):
        maintain_data(lake, store, "gc", physical_delete_delay=timedelta(0), dry_run=False)
    # dry_run needs no opt-in: it deletes nothing.
    assert maintain_data(lake, store, "gc", physical_delete_delay=timedelta(0)) is not None


def test_noop_pass_does_not_mint_a_generation(lake: Lake, store: InMemoryObjectStore) -> None:
    """Idle-lake maintenance ticks must not churn the count-based retention

    window that reader pins depend on.
    """
    churn(lake)
    before, _ = resolve_head(store)
    # Default gates: nothing is old enough to touch -> no-op -> no commit.
    report = maintain_data(lake, store, "gc", dry_run=False)
    assert report is not None
    assert not (report.snapshots_expired or report.files_cleaned or report.orphans_deleted)
    after, _ = resolve_head(store)
    assert after.generation == before.generation


def test_pinned_reader_within_contract_survives_maintenance(
    lake: Lake, store: InMemoryObjectStore, tmp_path: Path
) -> None:
    """A reader pinned to an older RETAINED generation keeps its DATA reads

    as long as maintenance gates (expire_older_than) haven't passed the
    snapshots that generation references — the documented pin contract.
    """
    churn(lake)  # generations 0..4; snapshots referencing live parquet
    pinned_root, _ = resolve_head(store)

    # Advance the lake past the pinned generation.
    with lake.transaction() as tx:
        tx.sql("INSERT INTO t SELECT range FROM range(1000)")

    # Maintenance with DEFAULT gates: nothing the pinned generation
    # references is old enough to expire, so its reads must survive.
    report = maintain_data(lake, store, "gc", dry_run=False)
    assert report is not None

    verify = Lake(store, workdir=tmp_path / "pin", data_path=str(tmp_path / "data"))
    (tmp_path / "pin").mkdir()
    path = verify._cache.fetch_copy(  # pyright: ignore[reportPrivateUsage]
        pinned_root.generation, pinned_root.payload_uuid
    )
    con = LakeConnection(path, data_path=None, read_only=True)
    # Value-forcing read through the PINNED generation's catalog.
    assert con.execute("SELECT count(*), sum(id) FROM t") == [(50000, 1249975000)]
    con.abandon()


def test_lost_cas_after_physical_deletes_stays_consistent(
    lake: Lake, store: InMemoryObjectStore, data: Path
) -> None:
    """Deterministic version of the dangerous race: maintenance's physical

    deletes land, then its commit LOSES the CAS to a rival writer. The lake
    must stay fully readable (schedule-table semantics make the deleted
    files dead in the rival's lineage too).
    """
    churn(lake)

    class RivalOnRootCas:
        """Wraps the store; injects a rival commit before maintenance's CAS."""

        def __init__(self, inner: InMemoryObjectStore, tmp: Path) -> None:
            self._inner = inner
            self._tmp = tmp
            self._armed = False

        def arm(self) -> None:
            self._armed = True

        def get(self, key: str) -> GetResult:
            return self._inner.get(key)

        def put_if_absent(self, key: str, body: bytes) -> str:
            if key.startswith(ROOTS_PREFIX) and self._armed:
                self._armed = False
                # A rival wins THIS generation first, so maintenance's marker
                # create 412s and (state-dependent CALLs) aborts — after its
                # physical deletes already landed.
                rival_work = self._tmp / "rival"
                rival_work.mkdir(exist_ok=True)
                rival = Lake(self._inner, workdir=rival_work, data_path=str(data))
                with rival.transaction() as tx:
                    tx.sql("INSERT INTO t VALUES (999999)")
            return self._inner.put_if_absent(key, body)

        def put_if_match(self, key: str, body: bytes, etag: str) -> str:
            return self._inner.put_if_match(key, body, etag)

        def list_prefix(self, prefix: str) -> list[str]:
            return self._inner.list_prefix(prefix)

        def put(self, key: str, body: bytes) -> str:
            return self._inner.put(key, body)

        def delete(self, key: str) -> None:
            self._inner.delete(key)

    wrapper = RivalOnRootCas(store, data.parent)
    mlake = Lake(
        wrapper,  # pyright: ignore[reportArgumentType]
        workdir=data.parent / "mwork",
        data_path=str(data),
    )
    (data.parent / "mwork").mkdir(exist_ok=True)

    # Two-phase: first pass schedules (commit unopposed), second pass
    # physically deletes and loses its CAS to the injected rival.
    r1 = maintain_data(
        mlake,
        wrapper,
        "gc",  # pyright: ignore[reportArgumentType]
        expire_older_than=NOW,
        physical_delete_delay=NOW,
        dry_run=False,
        **UNSAFE,
    )
    assert r1 is not None
    wrapper.arm()
    with pytest.raises(ConflictAbortError):
        maintain_data(
            mlake,
            wrapper,
            "gc",  # pyright: ignore[reportArgumentType]
            expire_older_than=NOW,
            physical_delete_delay=NOW,
            dry_run=False,
            **UNSAFE,
        )

    # Files are gone AND the commit recording that fact was lost — yet the
    # surviving lineage must be fully readable end to end.
    verify = Lake(store, workdir=data.parent / "vwork", data_path=str(data))
    (data.parent / "vwork").mkdir(exist_ok=True)
    with verify.reader() as con:
        rows = con.execute("SELECT count(*), sum(id) FROM t WHERE id < 999999")
        assert rows == [(50000, 1249975000)]
        assert con.execute("SELECT count(*) FROM t WHERE id = 999999") == [(1,)]
