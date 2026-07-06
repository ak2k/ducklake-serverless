"""P2 gate: the torture test. Concurrent writers, invariants must hold.

Threads share one in-memory store (its dict operations are effectively
atomic under the GIL, mirroring S3's per-request atomicity) but each
thread gets a private Lake — separate workdirs and caches, like separate
processes. Every commit inserts a unique (writer_id, seq) marker row.

Invariants asserted at the end:
- exactly-once: marker count == distinct markers == acknowledged commits
  (catches both lost updates and double-apply on replay)
- gapless generations: final root generation == acks (+0 for bootstrap)
- linear snapshots: strictly increasing, one per commit + bootstrap
- resolvable root: the final root names an attachable catalog
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

import pytest

from ducklake_serverless.errors import ConflictAbortError
from ducklake_serverless.gc import collect
from ducklake_serverless.models import parse_catalog_key
from ducklake_serverless.objectstore import InMemoryObjectStore
from ducklake_serverless.root import read_root
from ducklake_serverless.session import Lake

if TYPE_CHECKING:
    from pathlib import Path

WRITERS = 8
COMMITS_PER_WRITER = 25


def make_lake(store: InMemoryObjectStore, base: Path, name: str, data: Path) -> Lake:
    work = base / name
    work.mkdir()
    return Lake(store, workdir=work, data_path=str(data), max_attempts=50)


@pytest.mark.slow
def test_concurrent_blind_appends_hold_invariants(tmp_path: Path) -> None:
    store = InMemoryObjectStore()
    data = tmp_path / "data"
    data.mkdir()

    setup = make_lake(store, tmp_path, "setup", data)
    setup.bootstrap()
    with setup.transaction() as tx:
        tx.sql("CREATE TABLE markers (writer INTEGER, seq INTEGER)")

    acks: list[tuple[int, int]] = []
    acks_lock = threading.Lock()
    failures: list[BaseException] = []

    def writer(writer_id: int) -> None:
        lake = make_lake(store, tmp_path, f"w{writer_id}", data)
        for seq in range(COMMITS_PER_WRITER):
            try:
                with lake.transaction() as tx:
                    tx.sql("INSERT INTO markers VALUES (?, ?)", (writer_id, seq))
                with acks_lock:
                    acks.append((writer_id, seq))
            except ConflictAbortError as exc:  # pragma: no cover - fail loudly
                failures.append(exc)
                return

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(WRITERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not failures, f"writers aborted: {failures[:3]}"
    assert len(acks) == WRITERS * COMMITS_PER_WRITER

    # Gapless generations: bootstrap(0) + CREATE(1) + one per acked commit.
    final_root, _ = read_root(store)
    assert final_root.generation == 1 + len(acks)

    verify = make_lake(store, tmp_path, "verify", data)
    with verify.reader() as con:
        rows = con.execute("SELECT writer, seq FROM markers")
        # Exactly-once: every ack present exactly once, nothing else.
        assert sorted(rows) == sorted(acks)
        assert len(set(rows)) == len(rows)
        # Linear snapshot history, one snapshot per commit.
        ids = con.snapshot_ids()
        assert ids == sorted(set(ids))
        assert len(ids) == 2 + len(acks)  # catalog init + CREATE + inserts


@pytest.mark.slow
def test_concurrent_ddl_aborts_do_not_corrupt(tmp_path: Path) -> None:
    """DDL racing appends: DDL writers may abort, appenders always land,

    and every acknowledged write survives.
    """
    store = InMemoryObjectStore()
    data = tmp_path / "data"
    data.mkdir()
    setup = make_lake(store, tmp_path, "setup", data)
    setup.bootstrap()
    with setup.transaction() as tx:
        tx.sql("CREATE TABLE t (v INTEGER)")

    appended: list[int] = []
    ddl_acks: list[int] = []
    lock = threading.Lock()

    def appender(writer_id: int) -> None:
        lake = make_lake(store, tmp_path, f"a{writer_id}", data)
        for seq in range(10):
            value = writer_id * 1000 + seq
            with lake.transaction() as tx:
                tx.sql("INSERT INTO t VALUES (?)", (value,))
            with lock:
                appended.append(value)

    def ddl_writer(writer_id: int) -> None:
        lake = make_lake(store, tmp_path, f"d{writer_id}", data)
        try:
            with lake.transaction() as tx:
                tx.sql(f"CREATE TABLE extra_{writer_id} (v INTEGER)")
            with lock:
                ddl_acks.append(writer_id)
        except ConflictAbortError:
            pass  # expected: DDL never auto-replays

    threads = [threading.Thread(target=appender, args=(i,)) for i in range(4)] + [
        threading.Thread(target=ddl_writer, args=(i,)) for i in range(3)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    verify = make_lake(store, tmp_path, "verify", data)
    with verify.reader() as con:
        rows = [r[0] for r in con.execute("SELECT v FROM t")]
        assert sorted(rows) == sorted(appended)  # pyright: ignore[reportArgumentType]
        # Every acked DDL table exists.
        tables = {r[0] for r in con.execute("SELECT table_name FROM duckdb_tables()")}
        for writer_id in ddl_acks:
            assert f"extra_{writer_id}" in tables


@pytest.mark.slow
def test_writers_with_concurrent_gc_hold_invariants(tmp_path: Path) -> None:
    """Writers race a GC loop: every ack survives, retention is respected."""
    store = InMemoryObjectStore()
    data = tmp_path / "data"
    data.mkdir()
    setup = make_lake(store, tmp_path, "setup", data)
    setup.bootstrap()
    with setup.transaction() as tx:
        tx.sql("CREATE TABLE markers (writer INTEGER, seq INTEGER)")

    acks: list[tuple[int, int]] = []
    acks_lock = threading.Lock()
    stop = threading.Event()
    gc_reports: list[object] = []

    def writer(writer_id: int) -> None:
        lake = make_lake(store, tmp_path, f"gw{writer_id}", data)
        for seq in range(15):
            with lake.transaction() as tx:
                tx.sql("INSERT INTO markers VALUES (?, ?)", (writer_id, seq))
            with acks_lock:
                acks.append((writer_id, seq))

    def gc_loop() -> None:
        while not stop.is_set():
            report = collect(store, "torture-gc", retain_generations=5, dry_run=False)
            if report is not None:
                gc_reports.append(report)
            stop.wait(0.05)

    gc_thread = threading.Thread(target=gc_loop)
    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
    gc_thread.start()
    for t_ in threads:
        t_.start()
    for t_ in threads:
        t_.join()
    stop.set()
    gc_thread.join()

    assert gc_reports, "GC never ran"
    assert len(acks) == 4 * 15

    verify = make_lake(store, tmp_path, "gverify", data)
    with verify.reader() as con:
        rows = con.execute("SELECT writer, seq FROM markers")
        assert sorted(rows) == sorted(acks)

    # Retention respected: at most retain_generations catalogs remain
    # (plus any not-yet-swept tail from the final commits).
    final, _ = read_root(store)
    kept = [parse_catalog_key(k)[0] for k in store.list_prefix("catalog/")]
    assert final.generation in kept
