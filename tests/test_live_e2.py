"""Live smoke against a real S3-compatible endpoint (opt-in).

Run with:
    DUCKLAKE_LIVE_ENDPOINT=https://... \\
    DUCKLAKE_LIVE_BUCKET=... \\
    AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \\
    uv run pytest -m live --no-cov

Everything is scoped under a unique run prefix and deleted afterwards.
DATA_PATH stays local; this lane validates the CONTROL plane against real
conditional writes.

IMPORTANT — iDrive E2: E2 enforces conditional writes SEQUENTIALLY but not
atomically under concurrency (`probe_capabilities` reports
`atomic_create=False`), so it CANNOT safely serialize concurrent writers on
the marker protocol. These tests therefore run a SINGLE-WRITER sequential
acceptance (which E2 supports) and separately assert the capability probe
reports the endpoint's true atomicity — a concurrent multi-writer test would
(correctly) lose commits on E2.
"""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING

import pytest

from ducklake_serverless.objectstore import (
    S3ObjectStore,
    make_s3_client,
    probe_capabilities,
)
from ducklake_serverless.root import resolve_head
from ducklake_serverless.session import Lake

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

pytestmark = pytest.mark.live

ENDPOINT = os.environ.get("DUCKLAKE_LIVE_ENDPOINT")
BUCKET = os.environ.get("DUCKLAKE_LIVE_BUCKET")

requires_live = pytest.mark.skipif(
    not (ENDPOINT and BUCKET), reason="DUCKLAKE_LIVE_ENDPOINT/BUCKET not set"
)


@pytest.fixture
def live_store() -> Iterator[S3ObjectStore]:
    client = make_s3_client(endpoint_url=ENDPOINT)
    prefix = f"ducklake-serverless-test/{uuid.uuid4()}"
    store = S3ObjectStore(client, str(BUCKET), prefix=prefix)
    yield store
    for key in store.list_prefix(""):
        store.delete(key)


@requires_live
def test_live_capability_probe_reports_reality(live_store: S3ObjectStore) -> None:
    """The probe must characterize the endpoint's true concurrent atomicity.

    We don't assert a specific verdict (it depends on the endpoint under
    test) — only that the probe runs and reports a coherent Capabilities.
    Its value is that a marker-protocol lake refuses non-atomic-create
    backends at bootstrap.
    """
    caps = probe_capabilities(live_store)
    assert isinstance(caps.atomic_create, bool)
    assert isinstance(caps.atomic_cas, bool)


@requires_live
def test_live_single_writer_sequential_acceptance(
    live_store: S3ObjectStore, tmp_path: Path
) -> None:
    """Single-writer sequential commits over the real endpoint: every commit

    lands and reads back. This works on ANY store that enforces conditional
    writes sequentially (including E2) — no concurrency, so no atomicity
    requirement. Bootstrap skips the backend probe for the same reason.
    """
    data = tmp_path / "data"
    data.mkdir()

    def make_lake(name: str) -> Lake:
        work = tmp_path / name
        work.mkdir()
        return Lake(live_store, workdir=work, data_path=str(data), max_attempts=20)

    lake = make_lake("w")
    lake.bootstrap(verify_backend=False)  # single-writer: no concurrent create
    with lake.transaction() as tx:
        tx.sql("CREATE TABLE events (id INTEGER, msg VARCHAR)")
    for i in range(8):
        with lake.transaction() as tx:
            tx.sql("INSERT INTO events VALUES (?, ?)", (i, f"row-{i}"))

    final, gen = resolve_head(live_store)
    assert gen == 9  # bootstrap(0) + CREATE(1) + 8 inserts
    assert final.generation == 9

    reader = make_lake("r")
    with reader.reader() as con:
        # sum() forces a real scan — count(*) answers from catalog metadata.
        assert con.execute("SELECT count(*), sum(id) FROM events") == [(8, 28)]


@requires_live
def test_live_concurrent_writers_only_on_atomic_backends(
    live_store: S3ObjectStore, tmp_path: Path
) -> None:
    """Concurrent multi-writer acceptance — SKIPPED unless the endpoint

    enforces atomic create-only. On E2 this skips (its create-only is not
    atomic); on MinIO/AWS it runs and asserts exactly-once.
    """
    import threading  # noqa: PLC0415  # only this test spawns threads

    if not probe_capabilities(live_store).atomic_create:
        pytest.skip("endpoint lacks atomic create-only — concurrent writers unsafe")

    data = tmp_path / "data"
    data.mkdir()

    def make_lake(name: str) -> Lake:
        work = tmp_path / name
        work.mkdir()
        return Lake(live_store, workdir=work, data_path=str(data), max_attempts=30)

    make_lake("setup").bootstrap()
    with make_lake("create").transaction() as tx:
        tx.sql("CREATE TABLE events (writer INTEGER, seq INTEGER)")

    acks: list[tuple[int, int]] = []
    lock = threading.Lock()

    def writer(writer_id: int) -> None:
        lake = make_lake(f"w{writer_id}")
        for seq in range(5):
            with lake.transaction() as tx:
                tx.sql("INSERT INTO events VALUES (?, ?)", (writer_id, seq))
            with lock:
                acks.append((writer_id, seq))

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(acks) == 15
    _, gen = resolve_head(live_store)
    assert gen == 1 + len(acks)  # exactly-once: every commit its own generation
    with make_lake("verify").reader() as con:
        rows = con.execute("SELECT writer, seq FROM events")
        assert sorted(rows) == sorted(acks)
