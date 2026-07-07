"""P1 gate: hermetic end-to-end over the in-memory store.

DATA_PATH is a local directory, so DuckDB never talks to the object store
— only the root and catalog transport do. The reader path is deliberately
also exercised with a raw stock-duckdb attach (no library code) to prove
any DuckLake-aware tool can read published generations.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, override

import duckdb
import pytest

from ducklake_serverless.engine import DUCKDB_VERSION
from ducklake_serverless.errors import (
    BackendUnsafeError,
    CatalogHygieneError,
    ConflictAbortError,
    VersionMismatchError,
)
from ducklake_serverless.generation import check_hygiene
from ducklake_serverless.objectstore import InMemoryObjectStore
from ducklake_serverless.root import resolve_head
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


def test_bootstrap_creates_generation_zero(lake: Lake, store: InMemoryObjectStore) -> None:
    doc = lake.bootstrap()
    assert doc.generation == 0
    assert doc.duckdb_storage_version == DUCKDB_VERSION
    current, _ = resolve_head(store)
    assert current == doc


def test_bootstrap_twice_adopts_existing(lake: Lake) -> None:
    """A second bootstrap adopts the extant generation-0 lake, not an error."""
    first = lake.bootstrap()
    second = lake.bootstrap()
    assert second.generation == 0
    assert second.catalog_uuid == first.catalog_uuid  # the marker that won


class _NonAtomicCreate(InMemoryObjectStore):
    """Create-only is last-writer-wins (E2-shaped): concurrent creates all win."""

    @override
    def put_if_absent(self, key: str, body: bytes) -> str:
        with self._lock:  # pyright: ignore[reportPrivateUsage]
            etag = self._next_etag()  # pyright: ignore[reportPrivateUsage]
            self._objects[key] = (body, etag, datetime.now(tz=UTC))  # pyright: ignore[reportPrivateUsage]
            return etag


def test_bootstrap_refuses_backend_without_atomic_create(tmp_path: Path) -> None:
    """The safety gate: bootstrap() probes the backend and refuses one whose

    create-only isn't atomic under concurrency (it would silently lose
    commits), while verify_backend=False opts a single-writer lake past it.
    """
    data = tmp_path / "data"
    data.mkdir()
    store = _NonAtomicCreate()

    def make(name: str) -> Lake:
        work = tmp_path / name
        work.mkdir()
        return Lake(store, workdir=work, data_path=str(data))

    with pytest.raises(BackendUnsafeError, match="atomically under"):
        make("gated").bootstrap()

    # The same unsafe backend is accepted for an explicit single-writer lake.
    single = make("single")
    doc = single.bootstrap(verify_backend=False)
    assert doc.generation == 0
    assert resolve_head(store)[1] == 0


def test_two_commits_reader_sees_both(lake: Lake, store: InMemoryObjectStore) -> None:
    """The gate: create lake, commit twice, reader sees both commits."""
    lake.bootstrap()

    with lake.transaction() as tx:
        tx.sql("CREATE TABLE events (id INTEGER, msg VARCHAR)")
        tx.sql("INSERT INTO events VALUES (1, 'first')")

    with lake.transaction() as tx:
        tx.sql("INSERT INTO events VALUES (2, 'second')")

    doc, _ = resolve_head(store)
    assert doc.generation == 2

    with lake.reader() as con:
        rows = con.execute("SELECT id, msg FROM events ORDER BY id")
        assert rows == [(1, "first"), (2, "second")]
        # Snapshot history is linear and complete across generations.
        assert con.snapshot_ids() == sorted(con.snapshot_ids())
        assert len(con.snapshot_ids()) >= 3  # initial + create+insert + insert


def test_stock_duckdb_reads_published_generation(
    lake: Lake, store: InMemoryObjectStore, tmp_path: Path
) -> None:
    """No library code on the read path: any DuckLake tool can attach."""
    lake.bootstrap()
    with lake.transaction() as tx:
        tx.sql("CREATE TABLE t (v INTEGER)")
        tx.sql("INSERT INTO t VALUES (42)")

    doc, _ = resolve_head(store)
    raw = store.get(doc.catalog_key).body
    catalog = tmp_path / "stock-reader.duckdb"
    catalog.write_bytes(raw)

    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute(f"ATTACH 'ducklake:{catalog}' AS lake (READ_ONLY)")
    assert con.execute("SELECT v FROM lake.t").fetchall() == [(42,)]
    con.close()


def test_failed_transaction_publishes_nothing(lake: Lake, store: InMemoryObjectStore) -> None:
    lake.bootstrap()
    with pytest.raises(RuntimeError, match="boom"), lake.transaction() as tx:
        tx.sql("CREATE TABLE doomed (v INTEGER)")
        raise RuntimeError("boom")
    doc, _ = resolve_head(store)
    assert doc.generation == 0
    assert len(store.list_prefix("catalog/")) == 1  # only generation 0


def test_lost_ddl_race_aborts(lake: Lake, store: InMemoryObjectStore, tmp_path: Path) -> None:
    """A DDL transaction that loses its generation aborts — DDL never replays.

    (A lost APPEND rebases and commits; that is covered in the torture and
    fault-injection suites. DDL is the case that still aborts.)
    """
    lake.bootstrap()

    with pytest.raises(ConflictAbortError), lake.transaction() as tx:
        tx.sql("CREATE TABLE mine (v INTEGER)")  # DDL
        # A rival commits the generation we are targeting, mid-transaction.
        rival = Lake(store, workdir=tmp_path / "rival", data_path=str(tmp_path / "data"))
        (tmp_path / "rival").mkdir()
        with rival.transaction() as rtx:
            rtx.sql("CREATE TABLE theirs (v INTEGER)")

    doc, gen = resolve_head(store)
    assert gen == 1  # the rival's commit, not ours
    assert doc.generation == 1


def test_version_mismatch_refused(lake: Lake, store: InMemoryObjectStore) -> None:
    lake.bootstrap()
    head, _ = resolve_head(store)
    # Plant a foreign-version marker as the new head; the writer's pre-attach
    # version check must refuse to build on it.
    pinned = head.model_copy(update={"generation": 1, "duckdb_storage_version": "v0.0.1-other"})
    store.put_if_absent(pinned.marker_key, pinned.to_json_bytes())

    with pytest.raises(VersionMismatchError), lake.transaction() as tx:
        tx.sql("SELECT 1")


def test_planted_wal_blocks_publish(tmp_path: Path) -> None:
    catalog = tmp_path / "cat.duckdb"
    con = duckdb.connect(str(catalog))
    con.close()
    catalog.with_name("cat.duckdb.wal").write_bytes(b"leftover")
    with pytest.raises(CatalogHygieneError, match="wal"):
        check_hygiene(catalog)


def test_non_duckdb_file_blocks_publish(tmp_path: Path) -> None:
    fake = tmp_path / "cat.duckdb"
    fake.write_bytes(b"\x00" * 64)
    with pytest.raises(CatalogHygieneError, match="DuckDB"):
        check_hygiene(fake)
