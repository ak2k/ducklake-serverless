"""Chunk/pack transport: build-reconstruct identity, dedup, and heal paths.

The property that matters: for ANY payload and ANY base, build_manifest +
publish_packs + reconstruct reproduces the payload byte-identically, and
dedup ships only novel bytes. The heal path (verify_packs) is the
stalled-writer defense and gets fault-injection coverage.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, override

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ducklake_serverless import chunk
from ducklake_serverless.chunk import (
    DEFAULT_CHUNK_SIZE,
    MANIFEST_MAGIC,
    MAX_ENTRIES,
    Manifest,
    build_manifest,
    choose_chunk_size,
    format_pack_key,
    publish_packs,
    put_pack,
    reconstruct,
    sniff_manifest,
    verify_packs,
)
from ducklake_serverless.errors import (
    AmbiguousCasError,
    ExternalServiceError,
    InputValidationError,
    ObjectNotFoundError,
)
from ducklake_serverless.objectstore import InMemoryObjectStore

if TYPE_CHECKING:
    from pathlib import Path


def roundtrip(data: bytes, tmp_path: Path, base: Manifest | None = None) -> Manifest:
    """build -> publish -> reconstruct -> assert byte-identity; return manifest."""
    store = InMemoryObjectStore()
    tmp_path.mkdir(parents=True, exist_ok=True)
    src = tmp_path / "src"
    src.write_bytes(data)
    manifest, packs = build_manifest(src, base, chunk_size=1024, pack_target=4096)
    publish_packs(store, packs)
    dest = tmp_path / "dest"
    reconstruct(store, manifest, dest)
    assert dest.read_bytes() == data
    return manifest


def test_roundtrip_various_sizes(tmp_path: Path) -> None:
    for i, size in enumerate([0, 1, 1023, 1024, 1025, 4096, 10_000, 100_000]):
        data = bytes((j * 7 + i) % 256 for j in range(size))
        manifest = roundtrip(data, tmp_path / str(i))
        assert manifest.total_size == size


def test_dedup_ships_only_novel_bytes(tmp_path: Path) -> None:
    """Editing one chunk of a payload publishes ~one chunk of new pack bytes."""
    (tmp_path / "v1").mkdir()
    data = bytes(i % 251 for i in range(64 * 1024))
    store = InMemoryObjectStore()
    src = tmp_path / "v1" / "f"
    src.write_bytes(data)
    m1, packs1 = build_manifest(src, None, chunk_size=1024, pack_target=8192)
    publish_packs(store, packs1)

    edited = bytearray(data)
    edited[5000:5010] = b"XXXXXXXXXX"  # dirty exactly one 1024-byte chunk
    src.write_bytes(bytes(edited))
    m2, packs2 = build_manifest(src, m1, chunk_size=1024, pack_target=8192)

    novel_bytes = sum(len(b) for _, b in packs2)
    assert novel_bytes == 1024  # one chunk, one new (tiny) pack
    assert len(m2.entries) == len(m1.entries)
    publish_packs(store, packs2)
    dest = tmp_path / "v2"
    reconstruct(store, m2, dest)
    assert dest.read_bytes() == bytes(edited)


def test_dedup_reuses_base_pack_references(tmp_path: Path) -> None:
    data = bytes(i % 200 for i in range(8 * 1024))
    src = tmp_path / "f"
    src.write_bytes(data)
    m1, _ = build_manifest(src, None, chunk_size=1024, pack_target=4096)
    m2, packs2 = build_manifest(src, m1, chunk_size=1024, pack_target=4096)
    assert packs2 == []  # identical payload: zero novel packs
    assert m2.entries == m1.entries


def test_choose_chunk_size_pins_to_base(tmp_path: Path) -> None:
    src = tmp_path / "f"
    src.write_bytes(b"x" * 4096)
    m1, _ = build_manifest(src, None, chunk_size=512)
    assert choose_chunk_size(100_000, m1) == 512  # base pins
    # Outgrows the cap at base size -> doubles until entries fit.
    huge = (512 * MAX_ENTRIES) + 1
    assert choose_chunk_size(huge, m1) == 1024
    assert choose_chunk_size(0, None) == DEFAULT_CHUNK_SIZE


def test_scaling_rule_caps_entries() -> None:
    for total in [0, 1, DEFAULT_CHUNK_SIZE * MAX_ENTRIES, DEFAULT_CHUNK_SIZE * MAX_ENTRIES * 8]:
        size = choose_chunk_size(total, None)
        assert size >= DEFAULT_CHUNK_SIZE
        assert (total + size - 1) // size <= MAX_ENTRIES


def test_manifest_bytes_roundtrip_and_magic(tmp_path: Path) -> None:
    src = tmp_path / "f"
    src.write_bytes(b"payload bytes here")
    manifest, _ = build_manifest(src, None, chunk_size=8)
    raw = manifest.to_bytes()
    assert raw.startswith(MANIFEST_MAGIC)
    assert Manifest.from_bytes(raw) == manifest


def test_sniff_rejects_non_manifests() -> None:
    assert sniff_manifest(b"") is None
    assert sniff_manifest(b"DUCK" + b"\x00" * 100) is None  # raw DuckDB-ish bytes
    assert sniff_manifest(MANIFEST_MAGIC + b"{not json") is None
    assert sniff_manifest(MANIFEST_MAGIC + b'{"schema": "wrong/9"}') is None


def test_from_bytes_wraps_validation_in_domain_error() -> None:
    with pytest.raises(InputValidationError):
        Manifest.from_bytes(MANIFEST_MAGIC + b'{"schema": "ducklake-serverless-manifest/1"}')


def test_reconstruct_pack_404_propagates_not_found(tmp_path: Path) -> None:
    """A swept pack surfaces as ObjectNotFoundError -> caller re-resolves head."""
    store = InMemoryObjectStore()
    src = tmp_path / "f"
    src.write_bytes(bytes(range(256)) * 64)
    manifest, packs = build_manifest(src, None, chunk_size=1024, pack_target=2048)
    publish_packs(store, packs)
    store.delete(format_pack_key(packs[0][0]))  # GC sweeps one pack mid-read
    dest = tmp_path / "dest"
    with pytest.raises(ObjectNotFoundError):
        reconstruct(store, manifest, dest)
    assert not dest.exists()  # no partial file left behind


def test_reconstruct_detects_corrupt_pack(tmp_path: Path) -> None:
    store = InMemoryObjectStore()
    src = tmp_path / "f"
    src.write_bytes(b"a" * 4096)
    manifest, packs = build_manifest(src, None, chunk_size=1024, pack_target=8192)
    publish_packs(store, packs)
    store.put(format_pack_key(packs[0][0]), b"tampered")  # corrupt in place
    with pytest.raises(ExternalServiceError, match="corrupt"):
        reconstruct(store, manifest, tmp_path / "dest")


def test_put_pack_412_is_success() -> None:
    """A rival landing the same content-addressed key first is a win."""
    store = InMemoryObjectStore()
    body = b"pack bytes"
    sha = hashlib.sha256(body).hexdigest()
    put_pack(store, sha, body)
    put_pack(store, sha, body)  # second put: 412 internally, still success
    assert store.get(format_pack_key(sha)).body == body


class AmbiguousPackStore(InMemoryObjectStore):
    """First pack PUT lands but reports ambiguous — put_pack must HEAD-resolve."""

    def __init__(self) -> None:
        super().__init__()
        self.ambiguous_remaining = 1

    @override
    def put_if_absent(self, key: str, body: bytes) -> str:
        if key.startswith(chunk.PACKS_PREFIX) and self.ambiguous_remaining:
            self.ambiguous_remaining -= 1
            super().put_if_absent(key, body)  # the write LANDS
            raise AmbiguousCasError(f"{key}: outcome unknown (injected)")
        return super().put_if_absent(key, body)


def test_put_pack_ambiguous_landed_resolves_by_head() -> None:
    store = AmbiguousPackStore()
    body = b"ambiguous pack"
    sha = hashlib.sha256(body).hexdigest()
    put_pack(store, sha, body)  # must not raise, must not double-write
    assert store.get(format_pack_key(sha)).body == body


class AmbiguousVanishedPackStore(InMemoryObjectStore):
    """First pack PUT vanishes with ambiguity — put_pack must retry and land it."""

    def __init__(self) -> None:
        super().__init__()
        self.vanish_remaining = 1

    @override
    def put_if_absent(self, key: str, body: bytes) -> str:
        if key.startswith(chunk.PACKS_PREFIX) and self.vanish_remaining:
            self.vanish_remaining -= 1
            raise AmbiguousCasError(f"{key}: outcome unknown (injected, not landed)")
        return super().put_if_absent(key, body)


def test_put_pack_ambiguous_vanished_retries() -> None:
    store = AmbiguousVanishedPackStore()
    body = b"vanishing pack"
    sha = hashlib.sha256(body).hexdigest()
    put_pack(store, sha, body)
    assert store.get(format_pack_key(sha)).body == body


def test_verify_packs_heals_swept_novel_pack(tmp_path: Path) -> None:
    """The stalled-writer defense: a swept novel pack is re-PUT before commit."""
    store = InMemoryObjectStore()
    src = tmp_path / "f"
    src.write_bytes(bytes(range(256)) * 16)
    manifest, packs = build_manifest(src, None, chunk_size=1024, pack_target=2048)
    publish_packs(store, packs)
    swept_sha = packs[0][0]
    store.delete(format_pack_key(swept_sha))  # GC swept it while we stalled

    verify_packs(store, manifest, chunk.novel_pack_index(packs))
    reconstruct(store, manifest, tmp_path / "dest")  # healed: reconstructs fine
    assert (tmp_path / "dest").read_bytes() == src.read_bytes()


def test_verify_packs_fails_loud_on_vanished_base_pack(tmp_path: Path) -> None:
    """A missing BASE pack cannot be healed (bytes not in hand) — fail, don't commit."""
    store = InMemoryObjectStore()
    src = tmp_path / "f"
    data = bytes(i % 199 for i in range(8 * 1024))
    src.write_bytes(data)
    m1, packs1 = build_manifest(src, None, chunk_size=1024, pack_target=4096)
    publish_packs(store, packs1)

    edited = bytearray(data)
    edited[0:4] = b"EDIT"
    src.write_bytes(bytes(edited))
    m2, packs2 = build_manifest(src, m1, chunk_size=1024, pack_target=4096)
    publish_packs(store, packs2)

    # Sweep a pack that m2 REUSES from the base (not among m2's novel packs).
    reused = m2.pack_keys() - {format_pack_key(sha) for sha, _ in packs2}
    store.delete(sorted(reused)[0])

    with pytest.raises(ExternalServiceError, match="base pack"):
        verify_packs(store, m2, chunk.novel_pack_index(packs2))


@settings(max_examples=30)
@given(
    data=st.binary(min_size=0, max_size=30_000),
    edits=st.lists(
        st.tuples(st.integers(min_value=0, max_value=29_999), st.binary(min_size=1, max_size=200)),
        max_size=5,
    ),
)
def test_property_edit_roundtrip(
    tmp_path_factory: pytest.TempPathFactory, data: bytes, edits: list[tuple[int, bytes]]
) -> None:
    """Any payload + any edits: reconstruct is byte-identical; dedup ∝ edits."""
    tmp = tmp_path_factory.mktemp("prop")
    store = InMemoryObjectStore()
    src = tmp / "src"
    src.write_bytes(data)
    m1, packs1 = build_manifest(src, None, chunk_size=512, pack_target=2048)
    publish_packs(store, packs1)

    edited = bytearray(data)
    for pos, patch in edits:
        if pos < len(edited):
            edited[pos : pos + len(patch)] = patch
    src.write_bytes(bytes(edited))
    m2, packs2 = build_manifest(src, m1, chunk_size=512, pack_target=2048)
    publish_packs(store, packs2)

    dest = tmp / "dest"
    reconstruct(store, m2, dest)
    assert dest.read_bytes() == bytes(edited)

    if bytes(edited) == data:
        assert packs2 == []  # no edits -> zero novel bytes
    novel = sum(len(b) for _, b in packs2)
    # Novel volume bounded by edit volume rounded up to chunk granularity
    # (+1 chunk per edit for straddling a boundary, + tail-chunk churn when
    # the payload length changed).
    budget = sum(512 + len(p) + 512 for _, p in edits) + abs(len(edited) - len(data)) + 512
    assert novel <= budget
