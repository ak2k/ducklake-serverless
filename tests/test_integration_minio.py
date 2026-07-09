"""Integration lane: full protocol against a real S3 API (MinIO).

Unlike the hermetic suites, DATA_PATH here is a real s3:// URL — DuckDB's
httpfs writes and reads Parquet over the network, exercising the S3
secret wiring, real conditional-write semantics, and multi-process
concurrency (each writer is a separate OS process, not a thread).

Requires a running MinIO (or any S3-compatible endpoint):
    DUCKLAKE_IT_ENDPOINT=http://127.0.0.1:9000 \\
    DUCKLAKE_IT_ACCESS_KEY=minioadmin DUCKLAKE_IT_SECRET_KEY=minioadmin \\
    uv run pytest -m integration --no-cov -o addopts=""

CI starts MinIO as a service container and sets these variables.
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import os
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING, Literal

import pytest

from ducklake_serverless.engine import S3Credentials
from ducklake_serverless.gc import collect, maintain_data
from ducklake_serverless.objectstore import (
    S3ObjectStore,
    make_s3_client,
    verify_conditional_writes,
)
from ducklake_serverless.root import resolve_head
from ducklake_serverless.session import Lake

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

pytestmark = pytest.mark.integration

ENDPOINT = os.environ.get("DUCKLAKE_IT_ENDPOINT")
ACCESS_KEY = os.environ.get("DUCKLAKE_IT_ACCESS_KEY")
SECRET_KEY = os.environ.get("DUCKLAKE_IT_SECRET_KEY")
BUCKET = os.environ.get("DUCKLAKE_IT_BUCKET", "ducklake-it")

requires_minio = pytest.mark.skipif(
    not (ENDPOINT and ACCESS_KEY and SECRET_KEY),
    reason="DUCKLAKE_IT_ENDPOINT/ACCESS_KEY/SECRET_KEY not set",
)

WRITER_PROCS = 4
COMMITS_PER_PROC = 6


def _client():  # boto3 client type is verbose; internal helper
    os.environ.setdefault("AWS_ACCESS_KEY_ID", str(ACCESS_KEY))
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", str(SECRET_KEY))
    return make_s3_client(endpoint_url=ENDPOINT, region_name="us-east-1")


def _s3_credentials() -> S3Credentials:
    endpoint = str(ENDPOINT).removeprefix("http://").removeprefix("https://")
    return S3Credentials(
        access_key_id=str(ACCESS_KEY),
        secret_access_key=str(SECRET_KEY),
        endpoint=endpoint,
        use_ssl=str(ENDPOINT).startswith("https"),
    )


@pytest.fixture
def prefix() -> Iterator[str]:
    run_prefix = f"it/{uuid.uuid4()}"
    client = _client()
    with contextlib.suppress(client.exceptions.BucketAlreadyOwnedByYou):
        client.create_bucket(Bucket=BUCKET)
    store = S3ObjectStore(client, BUCKET, prefix=run_prefix)
    # The endpoint must ENFORCE conditional writes, not just accept the
    # headers — garage 1.3.1 accepts-and-ignores them, which would make
    # every test here pass while proving nothing.
    verify_conditional_writes(store)
    yield run_prefix
    for key in store.list_prefix(""):
        store.delete(key)


def make_lake(prefix: str, workdir: Path) -> Lake:
    workdir.mkdir(parents=True, exist_ok=True)
    store = S3ObjectStore(_client(), BUCKET, prefix=prefix)
    return Lake(
        store,
        workdir=workdir,
        data_path=f"s3://{BUCKET}/{prefix}/data",
        s3_credentials=_s3_credentials(),
        max_attempts=30,
    )


def _writer_proc(args: tuple[str, str, int]) -> list[tuple[int, int]]:
    """Executed in a separate process: commit COMMITS_PER_PROC appends."""
    from pathlib import Path  # noqa: PLC0415  # spawn-safe import inside the child

    prefix, workdir, writer_id = args
    lake = make_lake(prefix, Path(workdir))
    acks: list[tuple[int, int]] = []
    for seq in range(COMMITS_PER_PROC):
        with lake.transaction() as tx:
            tx.sql("INSERT INTO markers VALUES (?, ?)", (writer_id, seq))
        acks.append((writer_id, seq))
    return acks


@requires_minio
def test_e2e_parquet_over_httpfs(prefix: str, tmp_path: Path) -> None:
    """Single writer, real s3:// DATA_PATH: Parquet lands in the bucket and

    reads back through a fresh reader connection.
    """
    lake = make_lake(prefix, tmp_path / "w")
    lake.bootstrap()
    with lake.transaction() as tx:
        tx.sql("CREATE TABLE events (id INTEGER)")
        # Small inserts are INLINED into the catalog (data travels with the
        # generation file); a large batch must spill real Parquet to s3://.
        tx.sql("INSERT INTO events SELECT range FROM range(100000)")

    raw_client = _client()
    listed = raw_client.list_objects_v2(Bucket=BUCKET, Prefix=f"{prefix}/data")
    data_keys = [key for o in listed.get("Contents", []) if (key := o.get("Key")) is not None]
    assert any(k.endswith(".parquet") for k in data_keys), data_keys

    reader_lake = make_lake(prefix, tmp_path / "r")
    with reader_lake.reader() as con:
        assert con.execute("SELECT count(*), sum(id) FROM events") == [(100000, 4999950000)]


@requires_minio
def test_streaming_reader_matches_download(prefix: str, tmp_path: Path) -> None:
    """reader(stream=True) attaches the catalog directly over httpfs (no

    download) and returns the same rows as the default download path;
    stream="auto" falls back to download for a small catalog. sum(id) forces a
    real Parquet scan over httpfs, so this proves the streamed catalog resolves
    its data files correctly, not just its metadata.
    """
    lake = make_lake(prefix, tmp_path / "w")
    lake.bootstrap()
    with lake.transaction() as tx:
        tx.sql("CREATE TABLE events (id INTEGER, k INTEGER)")
        tx.sql("INSERT INTO events SELECT range, range % 10 FROM range(100000)")

    query = "SELECT count(*), sum(id), count(*) FILTER (WHERE k = 3) FROM events"
    expected = [(100000, 4999950000, 10000)]
    modes: list[bool | Literal["auto"]] = [False, True, "auto"]
    for mode in modes:
        reader_lake = make_lake(prefix, tmp_path / f"r-{mode}")
        with reader_lake.reader(stream=mode) as con:
            assert con.execute(query) == expected, mode


@requires_minio
@pytest.mark.slow
def test_process_torture_multi_writer(prefix: str, tmp_path: Path) -> None:
    """The real thing: N separate OS processes appending concurrently

    against a real S3 API. Exactly-once, gapless generations, linear
    snapshots.
    """
    setup = make_lake(prefix, tmp_path / "setup")
    setup.bootstrap()
    with setup.transaction() as tx:
        tx.sql("CREATE TABLE markers (writer INTEGER, seq INTEGER)")

    jobs = [(prefix, str(tmp_path / f"proc{i}"), i) for i in range(WRITER_PROCS)]
    with concurrent.futures.ProcessPoolExecutor(max_workers=WRITER_PROCS) as pool:
        results = list(pool.map(_writer_proc, jobs))
    acks = [ack for proc_acks in results for ack in proc_acks]
    assert len(acks) == WRITER_PROCS * COMMITS_PER_PROC

    store = S3ObjectStore(_client(), BUCKET, prefix=prefix)
    final, _ = resolve_head(store)
    assert final.generation == 1 + len(acks)  # bootstrap + CREATE + appends

    verify = make_lake(prefix, tmp_path / "verify")
    with verify.reader() as con:
        rows = con.execute("SELECT writer, seq FROM markers")
        assert sorted(rows) == sorted(acks)
        ids = con.snapshot_ids()
        assert ids == sorted(set(ids))


@requires_minio
def test_gc_against_real_store(prefix: str, tmp_path: Path) -> None:
    """Retention sweep works over a real LIST/DELETE API."""
    lake = make_lake(prefix, tmp_path / "w")
    lake.bootstrap()
    with lake.transaction() as tx:
        tx.sql("CREATE TABLE t (v INTEGER)")
    for i in range(5):
        with lake.transaction() as tx:
            tx.sql("INSERT INTO t VALUES (?)", (i,))

    store = S3ObjectStore(_client(), BUCKET, prefix=prefix)
    report = collect(store, "it-gc", retain_generations=3, dry_run=False)
    assert report is not None
    remaining = store.list_prefix("payload/")
    assert len(remaining) == 3

    reader_lake = make_lake(prefix, tmp_path / "r")
    with reader_lake.reader() as con:
        assert len(con.execute("SELECT v FROM t")) == 5


@requires_minio
def test_data_maintenance_over_real_store(prefix: str, tmp_path: Path) -> None:
    """The #815 class of bug: maintenance must reclaim dead Parquet over a

    real S3 API without ever touching live data. (Upstream fixed the
    misorphaning in ducklake PR #863; this guards the whole path.)
    """
    lake = make_lake(prefix, tmp_path / "w")
    lake.bootstrap()
    with lake.transaction() as tx:
        tx.sql("CREATE TABLE t (id INTEGER)")
    with lake.transaction() as tx:
        tx.sql("INSERT INTO t SELECT range FROM range(100000)")
    with lake.transaction() as tx:
        tx.sql("DELETE FROM t")
    with lake.transaction() as tx:
        tx.sql("INSERT INTO t SELECT range FROM range(50000)")

    raw = _client()

    def bucket_parquets() -> int:
        listed = raw.list_objects_v2(Bucket=BUCKET, Prefix=f"{prefix}/data")
        return sum(
            1
            for o in listed.get("Contents", [])
            if (k := o.get("Key")) is not None and k.endswith(".parquet")
        )

    before = bucket_parquets()
    assert before >= 2  # dead file + live file at minimum

    store = S3ObjectStore(_client(), BUCKET, prefix=prefix)
    now = timedelta(0)
    r1 = maintain_data(
        lake,
        store,
        "it-gc",
        expire_older_than=now,
        physical_delete_delay=now,
        dry_run=False,
        _unsafe_allow_short_delay=True,  # no concurrent writers in this test
    )
    assert r1 is not None and r1.snapshots_expired
    r2 = maintain_data(
        lake,
        store,
        "it-gc",
        expire_older_than=now,
        physical_delete_delay=now,
        dry_run=False,
        _unsafe_allow_short_delay=True,
    )
    assert r2 is not None and r2.files_cleaned

    assert bucket_parquets() < before  # dead Parquet reclaimed from the bucket
    verify = make_lake(prefix, tmp_path / "v")
    with verify.reader() as con:
        # sum() forces real Parquet reads over httpfs — count(*) answers
        # from catalog metadata and passes even when files are gone.
        assert con.execute("SELECT count(*), sum(id) FROM t") == [(50000, 1249975000)]
