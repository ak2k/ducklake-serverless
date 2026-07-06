"""Live smoke against a real S3-compatible endpoint (opt-in).

Run with:
    DUCKLAKE_LIVE_ENDPOINT=https://... \\
    DUCKLAKE_LIVE_BUCKET=... \\
    AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \\
    uv run pytest -m live --no-cov

Everything is scoped under a unique run prefix and deleted afterwards.
DATA_PATH stays local (Parquet-over-httpfs is exercised separately);
this lane validates the CONTROL plane — root CAS, catalog transport,
concurrent-writer conflict semantics — against real conditional writes,
including which status code the provider returns for a lost race
(AWS: 412 always, or 409 ConditionalRequestConflict under concurrent
mutation; both map to a retryable conflict here).
"""

from __future__ import annotations

import os
import threading
import uuid
from typing import TYPE_CHECKING

import pytest

from ducklake_serverless.objectstore import (
    S3ObjectStore,
    make_s3_client,
    verify_conditional_writes,
)
from ducklake_serverless.root import read_root
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
    verify_conditional_writes(store)
    yield store
    for key in store.list_prefix(""):
        store.delete(key)


@requires_live
def test_live_two_writers_conflict_and_land(live_store: S3ObjectStore, tmp_path: Path) -> None:
    """The acceptance test: two writers, real endpoint, both commits land."""
    data = tmp_path / "data"
    data.mkdir()

    def make_lake(name: str) -> Lake:
        work = tmp_path / name
        work.mkdir()
        return Lake(live_store, workdir=work, data_path=str(data), max_attempts=20)

    setup = make_lake("setup")
    setup.bootstrap()
    with setup.transaction() as tx:
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
    final, _ = read_root(live_store)
    assert final.generation == 1 + len(acks)

    verify = make_lake("verify")
    with verify.reader() as con:
        rows = con.execute("SELECT writer, seq FROM events")
        assert sorted(rows) == sorted(acks)
