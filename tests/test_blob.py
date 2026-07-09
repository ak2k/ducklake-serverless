"""BlobStore round-trip: the engine drives a non-DuckLake payload end to end.

This is the payload-agnosticism proof — an opaque blob gets versioned, ACID,
time-travelled storage through the same commit driver and marker protocol that
back DuckLake, with no duckdb in sight.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from ducklake_serverless import blob
from ducklake_serverless.models import Abort, RootDoc, WriterInfo, format_payload_key
from ducklake_serverless.objectstore import InMemoryObjectStore

if TYPE_CHECKING:
    from pathlib import Path


def test_blob_bootstrap_read_write_history(tmp_path: Path) -> None:
    store = InMemoryObjectStore()
    bs = blob.BlobStore(store, tmp_path)

    doc = bs.bootstrap(b"v0")
    assert doc.generation == 0
    assert bs.read() == b"v0"

    r1 = bs.write(b"v1")
    assert r1.generation == 1
    assert bs.read() == b"v1"

    payload = b"a much longer blob payload " * 100
    r2 = bs.write(payload)
    assert r2.generation == 2
    assert bs.read() == payload
    assert bs.head().generation == 2

    # Time travel: earlier generations are immutable and still resolvable.
    assert store.get(format_payload_key(0, doc.payload_uuid)).body == b"v0"


def test_blob_bootstrap_defaults_to_empty(tmp_path: Path) -> None:
    store = InMemoryObjectStore()
    bs = blob.BlobStore(store, tmp_path)
    bs.bootstrap()
    assert bs.read() == b""


def _root() -> RootDoc:
    return RootDoc(
        generation=0,
        payload_uuid=uuid4(),
        created_at=datetime.now(tz=UTC),
        writer=WriterInfo(lib_version="0.1.0", host="test", pid=1),
    )


def test_blob_commit_context_aborts_and_never_replays(tmp_path: Path) -> None:
    ctx = blob._BlobCommit()  # pyright: ignore[reportPrivateUsage]
    ctx.validate(tmp_path / "unused", _root())  # opaque bytes: no-op
    assert isinstance(ctx.decide_rebase(1, 5), Abort)
    with pytest.raises(AssertionError):
        ctx.replay(tmp_path / "unused")
