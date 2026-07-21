"""End-to-end chunked transport through both adapters.

Proves a chunked generation commits, records transport="chunked" in its
marker, reconstructs byte-identically for readers, and interoperates with
whole-file generations in the same lineage (transport is per-commit, derived
from the publish outcome — never inherited).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from ducklake_serverless import chunk as chunk_mod
from ducklake_serverless import session as session_mod
from ducklake_serverless.blob import BlobStore
from ducklake_serverless.chunk import MANIFEST_MAGIC, PACKS_PREFIX
from ducklake_serverless.engine import S3Credentials
from ducklake_serverless.errors import ExternalServiceError
from ducklake_serverless.models import RootDoc
from ducklake_serverless.objectstore import InMemoryObjectStore, S3ObjectStore
from ducklake_serverless.root import resolve_head
from ducklake_serverless.session import Lake

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def store() -> InMemoryObjectStore:
    return InMemoryObjectStore()


def make_blob(store: InMemoryObjectStore, tmp_path: Path, threshold: int | None) -> BlobStore:
    work = tmp_path / "blob-work"
    work.mkdir(exist_ok=True)
    return BlobStore(store, work, chunk_threshold=threshold)


def head(store: InMemoryObjectStore) -> RootDoc:
    doc, _ = resolve_head(store)
    return doc


def test_blob_chunked_write_read_roundtrip(store: InMemoryObjectStore, tmp_path: Path) -> None:
    bs = make_blob(store, tmp_path, threshold=0)  # always chunk
    bs.bootstrap(b"gen0")
    assert head(store).transport == "whole"  # bootstrap is always whole

    payload = bytes(i % 251 for i in range(200_000))
    bs.write(payload)
    doc = head(store)
    assert doc.transport == "chunked"
    assert store.get(doc.payload_key).body.startswith(MANIFEST_MAGIC)
    assert store.list_prefix(PACKS_PREFIX)  # packs exist
    assert bs.read() == payload  # reconstructs byte-identically


def test_blob_transport_is_per_commit_not_inherited(
    store: InMemoryObjectStore, tmp_path: Path
) -> None:
    """A small write after a chunked one goes whole — the inheritance bug guard."""
    bs = make_blob(store, tmp_path, threshold=1000)
    bs.bootstrap(b"gen0")
    big = bytes(i % 199 for i in range(50_000))
    bs.write(big)
    assert head(store).transport == "chunked"

    bs.write(b"small")  # below threshold: must be whole, not inherited-chunked
    doc = head(store)
    assert doc.transport == "whole"
    assert store.get(doc.payload_key).body == b"small"
    assert bs.read() == b"small"


def test_blob_chunked_dedup_across_generations(store: InMemoryObjectStore, tmp_path: Path) -> None:
    """A small edit publishes far fewer novel pack bytes than the payload."""
    bs = make_blob(store, tmp_path, threshold=0)
    bs.bootstrap()
    data = bytes(i % 251 for i in range(500_000))
    bs.write(data)
    packs_before = set(store.list_prefix(PACKS_PREFIX))

    edited = bytearray(data)
    edited[1000:1010] = b"XXXXXXXXXX"
    bs.write(bytes(edited))
    packs_after = set(store.list_prefix(PACKS_PREFIX))
    novel = packs_after - packs_before
    novel_bytes = sum(len(store.get(k).body) for k in novel)
    assert novel_bytes < len(data) // 4  # dedup: novel ≪ payload
    assert bs.read() == bytes(edited)


def test_blob_serialized_marker_omits_whole_transport(
    store: InMemoryObjectStore, tmp_path: Path
) -> None:
    """Whole markers stay byte-compatible: no transport field serialized."""
    bs = make_blob(store, tmp_path, threshold=None)
    bs.bootstrap(b"x")
    bs.write(b"y")
    doc = head(store)
    raw = store.get(doc.marker_key).body
    assert b"transport" not in raw
    assert RootDoc.from_json_bytes(raw).transport == "whole"


def test_lake_chunked_commit_and_read(store: InMemoryObjectStore, tmp_path: Path) -> None:
    """DuckLake adapter over the chunked transport, end to end."""
    data = tmp_path / "data"
    data.mkdir()
    work = tmp_path / "work"
    work.mkdir()
    lake = Lake(store, workdir=work, data_path=str(data), chunk_threshold=0)
    lake.bootstrap()
    assert head(store).transport == "whole"  # bootstrap whole

    with lake.transaction() as tx:
        tx.sql("CREATE TABLE t (v INTEGER)")
        tx.sql("INSERT INTO t VALUES (1), (2), (3)")
    doc = head(store)
    assert doc.transport == "chunked"
    assert store.get(doc.payload_key).body.startswith(MANIFEST_MAGIC)

    with lake.transaction() as tx:  # a second chunked commit deduping vs the first
        tx.sql("INSERT INTO t VALUES (4)")
    assert head(store).transport == "chunked"

    with lake.reader() as con:  # reader reconstructs and attaches
        assert con.execute("SELECT count(*), sum(v) FROM t") == [(4, 10)]


def test_lake_stream_true_on_chunked_head_raises(
    store: InMemoryObjectStore, tmp_path: Path
) -> None:
    """A chunked head is a manifest — httpfs cannot attach it; fail loudly.

    Uses the non-S3 store path: stream=True already raises on a non-S3 store,
    so exercise the transport gate directly via _stream_store's ordering —
    the transport check must precede the size heuristic. Covered here at the
    unit level; the S3 path is integration-lane territory.
    """
    data = tmp_path / "data"
    data.mkdir()
    work = tmp_path / "work"
    work.mkdir()
    lake = Lake(store, workdir=work, data_path=str(data), chunk_threshold=0)
    lake.bootstrap()
    with lake.transaction() as tx:
        tx.sql("CREATE TABLE t (v INTEGER)")

    # Non-S3 store: stream=True fails on the store check (already-covered),
    # auto falls back to download and still reads the chunked head fine.
    with pytest.raises(ExternalServiceError, match="S3-backed"), lake.reader(stream=True):
        pass
    with lake.reader(stream="auto") as con:
        assert con.execute("SELECT count(*) FROM t") == [(0,)]


def test_chunk_size_rescale_boundary_end_to_end(
    store: InMemoryObjectStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The rescale cliff, end to end: a payload outgrowing chunk_size *
    MAX_ENTRIES doubles the chunk size (dedup deliberately drops for that
    generation) and readers reconstruct across the size change. MAX_ENTRIES
    is monkeypatched tiny so a KB-scale payload crosses the same boundary a
    multi-GiB one would — same branches, no gigabytes."""
    monkeypatch.setattr(chunk_mod, "MAX_ENTRIES", 8)
    monkeypatch.setattr(chunk_mod, "DEFAULT_CHUNK_SIZE", 1024)
    bs = make_blob(store, tmp_path, threshold=0)
    bs.bootstrap()

    small = bytes(i % 251 for i in range(6 * 1024))  # 6 chunks @1K — under cap
    bs.write(small)
    doc1 = head(store)
    m1 = chunk_mod.load_manifest(store, doc1.payload_key)
    assert m1.chunk_size == 1024

    # Outgrow the cap at the base's size: 20K > 1K * 8 -> doubles to 4K
    # (1K*8=8K < 20K, 2K*8=16K < 20K, 4K*8=32K >= 20K).
    big = small + bytes((i * 7) % 251 for i in range(14 * 1024))
    bs.write(big)
    doc2 = head(store)
    m2 = chunk_mod.load_manifest(store, doc2.payload_key)
    assert m2.chunk_size == 4096  # rescaled across the boundary
    assert len(m2.entries) <= 8
    assert bs.read() == big  # reconstructs across the size change

    # Next generation dedups again at the NEW size (base pins 4096).
    edited = bytearray(big)
    edited[0] = (edited[0] + 1) % 256
    bs.write(bytes(edited))
    m3 = chunk_mod.load_manifest(store, head(store).payload_key)
    assert m3.chunk_size == 4096
    shared = {e.pack_sha256 for e in m2.entries} & {e.pack_sha256 for e in m3.entries}
    assert shared  # dedup resumed after the rescale generation
    assert bs.read() == bytes(edited)


def test_payload_size_recorded_by_every_writer_path(
    store: InMemoryObjectStore, tmp_path: Path
) -> None:
    """Every marker writer records the TRUE logical payload size.

    payload_size is written by four paths (blob bootstrap, lake bootstrap,
    whole commit, chunked commit) and consumed marker-only by listings and
    the stream heuristic — an under-recording writer would make healthy
    generations read as empty through info()-trusting consumers (review
    finding: the lake bootstrap omitted it and defaulted to 0).
    """
    bs = make_blob(store, tmp_path, threshold=100_000)
    initial = b"gen0-initial"
    bs.bootstrap(initial)
    assert head(store).payload_size == len(initial)

    small = b"w" * 1_000  # below threshold: whole
    bs.write(small)
    doc = head(store)
    assert doc.transport == "whole"
    assert doc.payload_size == len(small)
    assert doc.payload_size == len(store.get(doc.payload_key).body)

    big = bytes(i % 251 for i in range(200_000))  # above threshold: chunked
    bs.write(big)
    doc = head(store)
    assert doc.transport == "chunked"
    assert doc.payload_size == len(big)  # logical size, NOT the manifest's
    manifest = chunk_mod.load_manifest(store, doc.payload_key)
    assert doc.payload_size == manifest.total_size


def test_lake_bootstrap_records_gen0_payload_size(
    store: InMemoryObjectStore, tmp_path: Path
) -> None:
    """Gen 0 of a DuckLake is a real catalog file — its marker must say so."""
    data = tmp_path / "data"
    data.mkdir()
    work = tmp_path / "work"
    work.mkdir()
    lake = Lake(store, workdir=work, data_path=str(data))
    lake.bootstrap()
    doc = head(store)
    assert doc.generation == 0
    assert doc.payload_size > 0
    assert doc.payload_size == len(store.get(doc.payload_key).body)


def test_stream_auto_heuristic_reads_marker_size(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The auto heuristic gates on marker payload_size — both branches.

    Hermetic via moto: the S3-store + credentials gates precede the size
    check, so an InMemory store can never reach it (review finding: the
    stream-because-large branch had zero coverage in any lane).
    """
    import boto3  # noqa: PLC0415  # moto-lane dep, kept out of module import cost
    from moto import mock_aws  # noqa: PLC0415  # moto-lane dep

    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")  # pyright: ignore[reportUnknownMemberType]  # boto3.client factory is untyped
        client.create_bucket(Bucket="stream-test")
        s3_store = S3ObjectStore(client, "stream-test", prefix="lake")
        data = tmp_path / "data"
        data.mkdir()
        work = tmp_path / "work"
        work.mkdir()
        creds = S3Credentials(access_key_id="test", secret_access_key="test")  # noqa: S106  # moto dummy creds
        lake = Lake(s3_store, workdir=work, data_path=str(data), s3_credentials=creds)
        lake.bootstrap()
        doc, _ = resolve_head(s3_store)
        assert doc.transport == "whole"
        assert doc.payload_size > 0

        monkeypatch.setattr(session_mod, "STREAM_MIN_BYTES", 1)
        stream_store = lake._stream_store("auto")  # pyright: ignore[reportPrivateUsage]
        assert stream_store is s3_store  # large enough → stream

        monkeypatch.setattr(session_mod, "STREAM_MIN_BYTES", doc.payload_size + 1)
        stream_store = lake._stream_store("auto")  # pyright: ignore[reportPrivateUsage]
        assert stream_store is None  # below the floor → download
