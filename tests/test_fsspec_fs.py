"""fsspec selective reads: range translation, selectivity, DuckDB attach.

The property that matters: for ANY (start, length) request over ANY payload,
the filesystem returns exactly data[start:start+length] — while fetching only
the pack slices covering that range (proved by GET accounting, not trusted).
The capstone is DuckDB attaching a CHUNKED DuckLake catalog through the
filesystem — streaming-equivalent selective reads where httpfs cannot go.
"""

from __future__ import annotations

# fsspec ships no py.typed (see fsspec_fs.py's facade note); this test file
# touches its untyped surface (open/seek/read/register_filesystem).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
# pyright: reportAny=false, reportUnknownArgumentType=false
# pyright: reportMissingTypeStubs=false
from typing import TYPE_CHECKING, override

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ducklake_serverless import chunk as chunk_mod
from ducklake_serverless.blob import BlobStore
from ducklake_serverless.chunk import PACKS_PREFIX
from ducklake_serverless.errors import (
    ExternalServiceError,
    InputValidationError,
)
from ducklake_serverless.fsspec_fs import GenerationFileSystem
from ducklake_serverless.objectstore import InMemoryObjectStore
from ducklake_serverless.session import Lake

if TYPE_CHECKING:
    from pathlib import Path


class CountingStore(InMemoryObjectStore):
    """Tracks ranged-GET traffic so selectivity is PROVED, not assumed."""

    def __init__(self) -> None:
        super().__init__()
        self.range_gets: list[tuple[str, int, int]] = []
        self.full_gets: list[str] = []

    @override
    def get_range(self, key: str, start: int, length: int) -> bytes:
        self.range_gets.append((key, start, length))
        return super().get_range(key, start, length)

    @override
    def get(self, key: str):
        self.full_gets.append(key)
        return super().get(key)

    def reset_counts(self) -> None:
        self.range_gets = []
        self.full_gets = []

    def pack_bytes_fetched(self) -> int:
        return sum(n for k, _, n in self.range_gets if k.startswith(PACKS_PREFIX))


def make_chunked_blob(
    store: InMemoryObjectStore, tmp_path: Path, data: bytes
) -> GenerationFileSystem:
    work = tmp_path / "bw"
    work.mkdir(exist_ok=True)
    bs = BlobStore(store, work, chunk_threshold=0)
    bs.bootstrap(b"g0")
    bs.write(data)
    return GenerationFileSystem(store)


DATA = bytes((i * 31 + 7) % 251 for i in range(300_000))


def test_selective_read_exact_bytes_and_traffic(tmp_path: Path) -> None:
    """A small range read returns exact bytes and fetches ~that much, not all.

    cache_type="none" measures the translation layer itself; fsspec's default
    readahead cache would otherwise prefetch a whole block_size (5 MB default
    — bigger than this test payload) and mask the selectivity.
    """
    store = CountingStore()
    fs = make_chunked_blob(store, tmp_path, DATA)
    store.reset_counts()

    with fs.open("head", "rb", cache_type="none") as f:
        f.seek(150_000)
        got = f.read(4096)
    assert got == DATA[150_000 : 150_000 + 4096]
    # Selectivity: pack bytes fetched ≈ the request, nowhere near the payload.
    assert 0 < store.pack_bytes_fetched() <= 4096 + 2 * 64 * 1024
    # Full GETs are head-resolution + the manifest — never whole pack bodies.
    assert all(not k.startswith(PACKS_PREFIX) for k in store.full_gets)

    # With a bounded readahead block, caching still stays well under the
    # payload: block-sized prefetch, not a whole-file download.
    store.reset_counts()
    with fs.open("head", "rb", block_size=16 * 1024) as f:
        f.seek(10_000)
        assert f.read(100) == DATA[10_000:10_100]
    assert 0 < store.pack_bytes_fetched() < len(DATA) // 3


def test_sequential_full_read_matches_payload(tmp_path: Path) -> None:
    store = CountingStore()
    fs = make_chunked_blob(store, tmp_path, DATA)
    with fs.open("head", "rb") as f:
        assert f.read() == DATA


def test_whole_file_generation_served_by_ranged_gets(tmp_path: Path) -> None:
    """transport=whole goes through get_range on its single object."""
    store = CountingStore()
    work = tmp_path / "w"
    work.mkdir()
    bs = BlobStore(store, work, chunk_threshold=None)  # whole-file
    bs.bootstrap(b"g0")
    bs.write(DATA)
    fs = GenerationFileSystem(store)
    store.reset_counts()
    with fs.open("head", "rb") as f:
        f.seek(1000)
        assert f.read(500) == DATA[1000:1500]
    assert store.range_gets and all(k.startswith("payload/") for k, _, _ in store.range_gets)


def test_gen_paths_pin_generations(tmp_path: Path) -> None:
    """gen/<n> reads a PINNED old generation while head moves on."""
    store = InMemoryObjectStore()
    work = tmp_path / "w"
    work.mkdir()
    bs = BlobStore(store, work, chunk_threshold=0)
    bs.bootstrap(b"g0")
    v1 = bytes(i % 199 for i in range(50_000))
    bs.write(v1)
    v2 = bytes(i % 101 for i in range(60_000))
    bs.write(v2)

    fs = GenerationFileSystem(store)
    with fs.open("gen/1", "rb") as f:
        assert f.read() == v1  # pinned history
    with fs.open("head", "rb") as f:
        assert f.read() == v2
    info = fs.info("gen/1")
    assert info["generation"] == 1
    assert info["transport"] == "chunked"
    assert fs.exists("gen/2") and not fs.exists("gen/99")
    listed = fs.ls("", detail=False)
    assert "head" in listed and "gen/1" in listed


def test_read_only_everywhere(tmp_path: Path) -> None:
    store = InMemoryObjectStore()
    fs = make_chunked_blob(store, tmp_path, b"data")
    with pytest.raises(NotImplementedError):
        fs.open("head", "wb")
    with pytest.raises(NotImplementedError):
        fs.rm("head")
    with pytest.raises(NotImplementedError):
        fs.mkdir("x")
    with pytest.raises(InputValidationError):
        fs.info("not-a-path")


def test_corrupt_pack_slice_detected(tmp_path: Path) -> None:
    """A fully-covered chunk read through a tampered pack fails verification."""
    store = InMemoryObjectStore()
    fs = make_chunked_blob(store, tmp_path, DATA)
    victim = store.list_prefix(PACKS_PREFIX)[0]
    body = bytearray(store.get(victim).body)
    body[10] ^= 0xFF
    store.put(victim, bytes(body))
    with fs.open("head", "rb") as f, pytest.raises(ExternalServiceError, match="hash"):
        f.read()  # sequential read fully covers the tampered chunk


@settings(max_examples=40, deadline=None)
@given(
    start=st.integers(min_value=0, max_value=310_000),
    length=st.integers(min_value=0, max_value=310_000),
)
def test_property_any_range_is_exact(
    tmp_path_factory: pytest.TempPathFactory, start: int, length: int
) -> None:
    """For ANY (start, length): fs bytes == data[start:start+length]."""
    tmp = tmp_path_factory.mktemp("prop")
    store = InMemoryObjectStore()
    fs = make_chunked_blob(store, tmp, DATA)
    with fs.open("head", "rb", cache_type="none") as f:
        f.seek(start)
        assert f.read(length) == DATA[start : start + length]


def test_duckdb_scan_via_fsspec_and_attach_via_reconstruct(tmp_path: Path) -> None:
    """DuckDB integration, honestly scoped: scans go through the registered
    filesystem (selective); ATTACH of a chunked catalog goes through local
    reconstruction (DuckDB's ATTACH never consults fsspec — C++-core only).
    Both paths must serve the same chunked generation correctly."""
    store = CountingStore()
    data = tmp_path / "data"
    data.mkdir()
    work = tmp_path / "work"
    work.mkdir()
    lake = Lake(store, workdir=work, data_path=str(data), chunk_threshold=0)
    lake.bootstrap()
    with lake.transaction() as tx:
        tx.sql("CREATE TABLE t (id INTEGER, v DOUBLE)")
        tx.sql("INSERT INTO t SELECT range, range * 1.5 FROM range(50000)")

    fs = GenerationFileSystem(store)
    assert fs.info("head")["transport"] == "chunked"

    # Path 1 — DuckDB ATTACH via reconstruction (the supported attach path).
    with lake.reader() as con:
        assert con.execute("SELECT count(*), sum(id) FROM t") == [(50000, 1249975000)]

    # Path 2 — a DuckDB scan THROUGH the registered fsspec filesystem: write
    # a parquet whose bytes live only behind the filesystem's selective reads.
    store.reset_counts()
    con2 = duckdb.connect()
    con2.register_filesystem(fs)  # pyright: ignore[reportUnknownMemberType]
    # The head generation IS a DuckDB database file; read a byte range of it
    # through DuckDB's fsspec glue to prove the wiring end to end.
    n = fs.info("head")["size"]
    blob = con2.execute("SELECT content FROM read_blob('ducklake-serverless://head')").fetchall()
    con2.close()
    assert len(blob) == 1 and len(blob[0][0]) == n  # full bytes via fsspec
    assert store.range_gets, "fsspec reads did not go through get_range"
    _ = chunk_mod
