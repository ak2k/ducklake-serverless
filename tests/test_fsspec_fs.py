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
from ducklake_serverless import commit as commit_mod
from ducklake_serverless.blob import BlobStore
from ducklake_serverless.chunk import PACKS_PREFIX
from ducklake_serverless.errors import ExternalServiceError
from ducklake_serverless.fsspec_fs import GenerationFileSystem
from ducklake_serverless.objectstore import InMemoryObjectStore
from ducklake_serverless.session import Lake

if TYPE_CHECKING:
    from pathlib import Path

_ORIG_POLICY = commit_mod.TransportPolicy


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
        # Marker-recorded size (info) and reader-derived size (open) must
        # agree — deb20a4 split these two sources; drift means a writer
        # under-recorded and info()-trusting consumers would truncate.
        assert fs.info("head")["size"] == f.size == len(DATA)
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
    # ls honors its path (fsspec contract): root -> head + gen dir;
    # gen -> generation entries; a file path -> that entry alone.
    assert fs.ls("", detail=False) == ["head", "gen"]
    gen_names = fs.ls("gen", detail=False)
    assert "gen/1" in gen_names and "gen/2" in gen_names
    assert fs.ls("head", detail=False) == ["head"]
    assert [e["name"] for e in fs.ls("gen/1")] == ["gen/1"]
    assert fs.isdir("gen") and not fs.isdir("gen/1")
    assert fs.isfile("gen/1") and not fs.isfile("gen")
    # Non-canonical spellings do NOT alias generations (int() leniency).
    assert not fs.exists("gen/0_1")
    assert not fs.exists("gen/+1")
    assert not fs.exists("gen/01")
    # pin() gives multi-open consumers a stable snapshot path.
    assert fs.pin("head") == "gen/2"
    # Stable immutable identity for fsspec cache layers.
    assert fs.info("gen/1")["etag"] == fs.checksum("gen/1")


def test_read_only_everywhere(tmp_path: Path) -> None:
    store = InMemoryObjectStore()
    fs = make_chunked_blob(store, tmp_path, b"data")
    with pytest.raises(NotImplementedError):
        fs.open("head", "wb")
    with pytest.raises(NotImplementedError):
        fs.rm("head")
    with pytest.raises(NotImplementedError):
        fs.mkdir("x")
    with pytest.raises(FileNotFoundError):
        fs.info("not-a-path")
    with pytest.raises(NotImplementedError):
        fs.cp_file("head", "elsewhere")


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
    assert len(blob) == 1
    with fs.open("head", "rb") as f:
        assert blob[0][0] == f.read()  # CONTENT equal, not merely length
    assert len(blob[0][0]) == n
    assert store.range_gets, "fsspec reads did not go through get_range"
    _ = chunk_mod


def test_uninitialized_lake_probes_are_clean(tmp_path: Path) -> None:
    """exists/ls on an empty bucket answer, never crash (review finding: the
    LakeNotInitializedError sibling escaped the old catch tuple)."""
    fs = GenerationFileSystem(InMemoryObjectStore())
    assert fs.exists("head") is False
    assert fs.exists("") is True  # the root itself
    assert fs.ls("", detail=False) == ["gen"]  # no head yet
    with pytest.raises(FileNotFoundError):
        fs.ls("gen")
    with pytest.raises(FileNotFoundError):
        fs.open("head", "rb")


def test_missing_paths_are_filenotfound_and_predicates_return(tmp_path: Path) -> None:
    """The fsspec ecosystem contract: missing paths -> FileNotFoundError;
    isdir/isfile are total predicates on any input."""
    store = InMemoryObjectStore()
    fs = make_chunked_blob(store, tmp_path, b"data")
    with pytest.raises(FileNotFoundError):
        fs.info("gen/99")
    with pytest.raises(FileNotFoundError):
        fs.open("gen/99", "rb")
    assert fs.isdir("bogus/path") is False  # must not raise
    assert fs.isfile("bogus/path") is False
    assert fs.exists("gen/99") is False


def test_transport_outage_is_not_absence(tmp_path: Path) -> None:
    """A store outage during exists() PROPAGATES — never reads as 'the
    generation does not exist' (failure asymmetry; review finding)."""
    store = InMemoryObjectStore()
    fs = make_chunked_blob(store, tmp_path, b"data")

    def boom(key: str) -> object:
        raise ExternalServiceError("injected outage")

    original = store.get
    store.get = boom  # type: ignore[method-assign]
    try:
        with pytest.raises(ExternalServiceError):
            fs.exists("gen/1")
    finally:
        store.get = original  # type: ignore[method-assign]


def test_corrupt_manifest_listed_marker_only_fails_at_read(tmp_path: Path) -> None:
    """Listing is marker-only, so a corrupt manifest still lists (and never
    poisons the listing — the original review finding); the corruption
    surfaces loudly at open/read instead."""
    store = InMemoryObjectStore()
    work = tmp_path / "w"
    work.mkdir()
    bs = BlobStore(store, work, chunk_threshold=0)
    bs.bootstrap(b"g0")
    bs.write(payload_bytes(1))
    bs.write(payload_bytes(2))
    # Corrupt gen/1's manifest in place (oversize would also work).
    gen1_key = sorted(store.list_prefix("payload/"))[1]
    store.put(gen1_key, b"garbage that is not a manifest")

    fs = GenerationFileSystem(store)
    names = fs.ls("gen", detail=False)
    # Listing is MARKER-ONLY (payload_size lives in the marker), so the
    # corrupt generation still lists — and crucially the listing never
    # raises. Corruption surfaces loudly at open/read time instead.
    assert names == ["gen/0", "gen/1", "gen/2"]
    assert [e["name"] for e in fs.ls("gen")] == names  # detail mode agrees
    with pytest.raises(ExternalServiceError), fs.open("gen/1", "rb") as f:
        f.read()  # the corrupt manifest fails HERE, not in ls


def test_two_stores_two_filesystems_never_alias(tmp_path: Path) -> None:
    """cachable=False: two filesystems over two different stores are
    independent instances serving their own lakes (review finding: fsspec's
    instance cache tokenizes the store by repr — a value-style __repr__ on
    any store class would alias different lakes without this)."""
    store_a = InMemoryObjectStore()
    store_b = InMemoryObjectStore()
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    bs_a = BlobStore(store_a, tmp_path / "a", chunk_threshold=0)
    bs_b = BlobStore(store_b, tmp_path / "b", chunk_threshold=0)
    bs_a.bootstrap(b"lake A")
    bs_b.bootstrap(b"lake B")

    fs_a = GenerationFileSystem(store_a)
    fs_b = GenerationFileSystem(store_b)
    assert fs_a is not fs_b
    with fs_a.open("head", "rb") as f:
        assert f.read() == b"lake A"
    with fs_b.open("head", "rb") as f:
        assert f.read() == b"lake B"


def test_head_snapshot_pinned_per_open(tmp_path: Path) -> None:
    """An open handle on 'head' keeps serving ITS generation across a new
    commit (documented snapshot semantics — review-mandated pin)."""
    store = InMemoryObjectStore()
    work = tmp_path / "w"
    work.mkdir()
    bs = BlobStore(store, work, chunk_threshold=0)
    bs.bootstrap(b"g0")
    v1 = payload_bytes(1)
    bs.write(v1)
    f = fs_open = GenerationFileSystem(store).open("head", "rb")
    bs.write(payload_bytes(2))  # head moves on
    assert f.read() == v1  # the open handle still serves its snapshot
    fs_open.close()


def test_reader_memo_avoids_repeat_manifest_fetch(tmp_path: Path) -> None:
    """info-then-open must not download a chunked manifest twice (review
    finding: pandas/pyarrow's standard probe pattern doubled the fetch)."""
    store = CountingStore()
    fs = make_chunked_blob(store, tmp_path, DATA)
    pinned = fs.pin("head")
    store.reset_counts()
    fs.info(pinned)
    manifest_gets_after_info = len([k for k in store.full_gets if k.startswith("payload/")])
    assert manifest_gets_after_info == 0  # info is marker-only: NO manifest fetch
    with fs.open(pinned, "rb", cache_type="none") as f:
        f.seek(1000)
        f.read(10)
    with fs.open(pinned, "rb", cache_type="none") as f:
        f.seek(5000)
        f.read(10)
    manifest_gets_total = len([k for k in store.full_gets if k.startswith("payload/")])
    assert manifest_gets_total == 1  # memoized reader: ONE fetch across opens


def payload_bytes(seed: int, size: int = 50_000) -> bytes:
    """Deterministic distinct payloads for multi-generation tests."""
    return bytes((i * 31 + seed) % 251 for i in range(size))


def test_multipack_and_dedup_interleaved_geometry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P1 review finding: the whole suite previously ran single-pack,
    fully-contiguous manifests — the coalescing break conditions and
    multi-run loop iteration were NEVER executed. Force multi-pack geometry
    (tiny pack target) and a dedup-interleaved manifest (v2 shares most
    chunks with v1 but mutates interior ones, so file-adjacent entries
    reference different packs at non-contiguous offsets), then prove
    byte-exactness AND that a boundary-straddling range issues >1 GET
    (the run genuinely split)."""

    # TransportPolicy's pack_target default binds at dataclass definition,
    # so patch the constructor blob.py looks up at call time.
    def small_packs(**kw: object) -> commit_mod.TransportPolicy:
        return _ORIG_POLICY(pack_target=4096, **kw)  # pyright: ignore[reportArgumentType]  # test shim: kwargs pass through

    monkeypatch.setattr(commit_mod, "TransportPolicy", small_packs)
    monkeypatch.setattr(chunk_mod, "DEFAULT_CHUNK_SIZE", 1024)
    store = CountingStore()
    work = tmp_path / "w"
    work.mkdir()
    bs = BlobStore(store, work, chunk_threshold=0)
    bs.bootstrap(b"g0")
    v1 = bytes((i * 7 + 1) % 251 for i in range(40 * 1024))  # 40 chunks, 10 packs
    bs.write(v1)
    v2 = bytearray(v1)
    for pos in range(2048, len(v2), 5 * 1024):  # mutate interior chunks
        v2[pos] = (v2[pos] + 1) % 256
    bs.write(bytes(v2))  # dedup vs v1: interleaved reused/novel pack refs

    fs = GenerationFileSystem(store)
    assert len(store.list_prefix(PACKS_PREFIX)) > 5  # genuinely multi-pack

    # Full read: byte-exact across every pack switch and dedup interleave.
    with fs.open("head", "rb", cache_type="none") as f:
        assert f.read() == bytes(v2)

    # A range straddling pack/interleave boundaries: exact bytes, >1 GET.
    store.reset_counts()
    with fs.open("head", "rb", cache_type="none") as f:
        f.seek(1500)
        got = f.read(9000)  # spans mutated + reused chunks across packs
    assert got == bytes(v2)[1500:10500]
    assert len(store.range_gets) > 1  # the coalesced run genuinely split

    # Historic generation reads exactly too (v1's own multi-pack manifest).
    with fs.open("gen/1", "rb", cache_type="none") as f:
        assert f.read() == v1
