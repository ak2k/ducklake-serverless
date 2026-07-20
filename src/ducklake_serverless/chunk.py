"""Content-addressed chunk/pack transport for large payloads.

A chunked generation stores a MANIFEST at its payload key instead of the raw
bytes: fixed-offset chunks of the payload are grouped into ~8 MiB immutable
PACK objects at `packs/<sha256-of-pack-bytes>`, and the manifest records, in
file order, which slice of which pack reproduces each chunk. Reconstruction
fetches the referenced packs in parallel and reassembles a byte-identical
file.

Two protocol invariants here are load-bearing for GC safety (see gc.py's
mark-sweep proof) and MUST NOT be weakened:

- **Manifest entries are FULL**: every entry names its pack directly. Never
  delta-to-base — a retained manifest alone must suffice to mark every pack
  it depends on.
- **The dedup source is strictly the BASE generation's manifest** (the
  committed head being built upon). No global index, no other generation —
  the GC induction descends dedup chains through committed bases only.

Packs are raw concatenations (no framing): manifests carry offsets, GC never
parses packs, and we need neither restic's index-rebuild nor encryption.
Consequence, accepted: no repack — a partially-dead pack is retained until
fully unreferenced.
"""

from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Literal

import pydantic
from pydantic import BaseModel, ConfigDict, Field

from ducklake_serverless.errors import (
    AmbiguousCasError,
    CatalogHygieneError,
    ConditionalConflictError,
    ExternalServiceError,
    InputValidationError,
    ObjectNotFoundError,
    PreconditionFailedError,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from ducklake_serverless.objectstore import ObjectStore

PACKS_PREFIX = "packs/"

# First bytes of every engine-written manifest. GC's orphan-sniff relies on
# this: an engine manifest ALWAYS starts with the magic and parses (no false
# negatives); a raw payload that coincidentally starts with it still fails the
# parse and is treated as opaque (false positives merely over-retain).
MANIFEST_MAGIC = b"DLSM1\n"

# Fixed-offset chunk size. Sub-DuckDB-block granularity measured best on E2
# (16K -> 2.8% novel/commit, 64K -> 8.2%, 256K -> 29%); 64K balances dedup
# against manifest length and per-chunk hashing overhead.
DEFAULT_CHUNK_SIZE = 64 * 1024

# Target pack object size. Packs decouple chunk granularity from GET count:
# a cold reconstruct fetches ~total_size/8MiB objects regardless of chunking.
DEFAULT_PACK_TARGET = 8 * 1024 * 1024

# Manifest entry cap: chunk size scales up so entries never exceed this
# (1 TiB payload -> 8 MiB chunks -> ~128K entries). Keeps manifests ~tens of
# MB worst-case instead of GBs.
MAX_ENTRIES = 2**17

# A manifest larger than this is a bug (or an absurd payload) — refuse to
# publish rather than absorb it, mirroring MAX_CATALOG_BYTES.
MAX_MANIFEST_BYTES = 256 * 1024 * 1024

_HEX_SHA256 = r"^[0-9a-f]{64}$"


def format_pack_key(pack_sha256: str) -> str:
    """Canonical object key for a pack (content-addressed)."""
    return f"{PACKS_PREFIX}{pack_sha256}"


def parse_pack_key(key: str) -> str:
    """Inverse of `format_pack_key`. Raises on any non-canonical key."""
    rest = key.removeprefix(PACKS_PREFIX)
    if rest == key or not _is_sha256_hex(rest):
        raise InputValidationError(f"not a canonical pack key: {key!r}")
    return rest


_SHA256_HEX_LEN = 64


def _is_sha256_hex(s: str) -> bool:
    return len(s) == _SHA256_HEX_LEN and all(c in "0123456789abcdef" for c in s)


class ManifestEntry(BaseModel):
    """One chunk of the payload: which pack slice reproduces it.

    Entries are FULL — `pack_sha256` names the pack directly, never a
    reference to the base manifest (load-bearing for GC; see module docs).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    chunk_sha256: str = Field(pattern=_HEX_SHA256)
    pack_sha256: str = Field(pattern=_HEX_SHA256)
    pack_offset: int = Field(ge=0)
    length: int = Field(gt=0)


class Manifest(BaseModel):
    """The body of a chunked generation's payload object — immutable.

    Serialized as `MANIFEST_MAGIC` + JSON. `entries` are in file order;
    concatenating the referenced pack slices reproduces the payload exactly
    (verified against `file_sha256` on reconstruct).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_id: Literal["ducklake-serverless-manifest/1"] = Field(
        default="ducklake-serverless-manifest/1", alias="schema"
    )
    chunk_size: int = Field(gt=0)
    total_size: int = Field(ge=0)
    file_sha256: str = Field(pattern=_HEX_SHA256)
    compression: Literal["none"] = "none"
    entries: tuple[ManifestEntry, ...]

    def to_bytes(self) -> bytes:
        """Serialize for the payload object body (magic-prefixed)."""
        body = MANIFEST_MAGIC + self.model_dump_json(by_alias=True).encode()
        if len(body) > MAX_MANIFEST_BYTES:
            raise CatalogHygieneError(
                f"manifest is {len(body)} bytes (cap {MAX_MANIFEST_BYTES}) — "
                "chunk-size scaling failed or the payload is absurd"
            )
        return body

    @classmethod
    def from_bytes(cls, data: bytes) -> Manifest:
        """Parse a manifest body; InputValidationError on anything else.

        Wraps pydantic's ValidationError in a domain error — GC's orphan
        sniff depends on "not a manifest" being a clean, catchable signal.
        """
        if not data.startswith(MANIFEST_MAGIC):
            raise InputValidationError("not a manifest: missing magic prefix")
        try:
            return cls.model_validate_json(data[len(MANIFEST_MAGIC) :])
        except pydantic.ValidationError as exc:
            raise InputValidationError("not a manifest: body failed validation") from exc

    def pack_keys(self) -> set[str]:
        """Object keys of every pack this manifest references."""
        return {format_pack_key(e.pack_sha256) for e in self.entries}

    def chunk_index(self) -> dict[str, ManifestEntry]:
        """chunk_sha256 -> entry, the dedup lookup for the NEXT generation."""
        return {e.chunk_sha256: e for e in self.entries}


def choose_chunk_size(total_size: int, base: Manifest | None) -> int:
    """Chunk size for a payload: the base's (dedup requires equality) or scaled.

    Dedup only works when this generation chunks at the same offsets as its
    base, so an existing base pins the size. Without one (or when the payload
    outgrows the entry cap at the base's size — accepted: that rescale
    sacrifices one generation of dedup), scale by doubling so `entries`
    never exceeds MAX_ENTRIES.
    """
    if base is not None and total_size <= base.chunk_size * MAX_ENTRIES:
        return base.chunk_size
    size = base.chunk_size if base is not None else DEFAULT_CHUNK_SIZE
    while total_size > size * MAX_ENTRIES:
        size *= 2
    return size


def build_manifest(
    path: Path,
    base: Manifest | None,
    *,
    chunk_size: int | None = None,
    pack_target: int = DEFAULT_PACK_TARGET,
) -> tuple[Manifest, list[tuple[str, bytes]]]:
    """Chunk `path`, dedup against `base`, and return (manifest, novel packs).

    Novel chunks (absent from the base manifest) are appended into new packs
    of ~`pack_target` bytes; chunks found in the base reuse its pack slices.
    Returns the manifest plus `[(pack_sha256, pack_bytes), ...]` to publish.
    """
    data = path.read_bytes()
    size = chunk_size if chunk_size is not None else choose_chunk_size(len(data), base)
    base_index = base.chunk_index() if base is not None and base.chunk_size == size else {}

    entries: list[ManifestEntry] = []
    packs: list[tuple[str, bytes]] = []
    pending: list[tuple[str, bytes]] = []  # (chunk_sha256, chunk_bytes) awaiting a pack
    pending_bytes = 0
    pending_entry_at: dict[int, int] = {}  # index into `pending` -> index into `entries`

    def flush_pack() -> None:
        nonlocal pending, pending_bytes, pending_entry_at
        if not pending:
            return
        pack_bytes = b"".join(chunk for _, chunk in pending)
        pack_sha = hashlib.sha256(pack_bytes).hexdigest()
        offset = 0
        for i, (chunk_sha, chunk) in enumerate(pending):
            entries[pending_entry_at[i]] = ManifestEntry(
                chunk_sha256=chunk_sha,
                pack_sha256=pack_sha,
                pack_offset=offset,
                length=len(chunk),
            )
            offset += len(chunk)
        packs.append((pack_sha, pack_bytes))
        pending = []
        pending_bytes = 0
        pending_entry_at = {}

    for off in range(0, len(data), size):
        chunk = data[off : off + size]
        chunk_sha = hashlib.sha256(chunk).hexdigest()
        reused = base_index.get(chunk_sha)
        if reused is not None and reused.length == len(chunk):
            entries.append(reused)
            continue
        if pending_bytes + len(chunk) > pack_target and pending:
            flush_pack()
        # Placeholder entry; flush_pack() rewrites it with the real pack ref.
        pending_entry_at[len(pending)] = len(entries)
        entries.append(
            ManifestEntry(
                chunk_sha256=chunk_sha,
                pack_sha256="0" * 64,
                pack_offset=0,
                length=len(chunk),
            )
        )
        pending.append((chunk_sha, chunk))
        pending_bytes += len(chunk)
    flush_pack()

    manifest = Manifest(
        chunk_size=size,
        total_size=len(data),
        file_sha256=hashlib.sha256(data).hexdigest(),
        entries=tuple(entries),
    )
    return manifest, packs


def put_pack(store: ObjectStore, pack_sha256: str, body: bytes, *, max_attempts: int = 5) -> None:
    """Create-only PUT of one pack; a rival writing the same key is SUCCESS.

    Content-addressed keys make every outcome benign: 412/409 means a twin
    wrote identical bytes (done); ambiguity resolves by HEAD (present = done,
    absent = retry). This is a deliberately wider success-net than the
    uuid-keyed payload publish, whose keys never collide.
    """
    key = format_pack_key(pack_sha256)
    for _ in range(max_attempts):
        try:
            store.put_if_absent(key, body)
        except (PreconditionFailedError, ConditionalConflictError):
            return  # twin wrote identical bytes
        except AmbiguousCasError:
            try:
                store.head_meta(key)
            except ObjectNotFoundError:
                continue  # genuinely didn't land — retry
            return  # landed despite the ambiguous response
        else:
            return
    raise ExternalServiceError(f"pack {pack_sha256} kept failing across {max_attempts} attempts")


def publish_packs(
    store: ObjectStore, packs: list[tuple[str, bytes]], *, max_workers: int = 8
) -> None:
    """PUT novel packs concurrently; propagate the first failure."""
    if not packs:
        return
    with ThreadPoolExecutor(max_workers=min(max_workers, len(packs))) as pool:
        for future in [pool.submit(put_pack, store, sha, body) for sha, body in packs]:
            future.result()


def verify_packs(
    store: ObjectStore, manifest: Manifest, novel: dict[str, bytes], *, max_workers: int = 8
) -> None:
    """HEAD every referenced pack right before the manifest PUT; heal novel ones.

    The stalled-writer defense: a writer that stalled past the GC grace may
    have had its not-yet-referenced packs swept. Immediately before the
    manifest lands (after which the mark pass protects them), re-HEAD every
    pack — re-PUT any missing one we hold bytes for (create-only, idempotent),
    and fail loudly if a missing pack came from the base manifest (its bytes
    are not in hand; committing would publish a generation that cannot be
    reconstructed).
    """

    def check(key: str) -> None:
        try:
            store.head_meta(key)
        except ObjectNotFoundError:
            sha = parse_pack_key(key)
            body = novel.get(sha)
            if body is None:
                raise ExternalServiceError(
                    f"base pack {sha} vanished before commit — the base "
                    "generation was GC'd out from under this writer; rebase"
                ) from None
            put_pack(store, sha, body)

    keys = sorted(manifest.pack_keys())
    with ThreadPoolExecutor(max_workers=min(max_workers, len(keys))) as pool:
        for future in [pool.submit(check, key) for key in keys]:
            future.result()


def reconstruct(
    store: ObjectStore, manifest: Manifest, dest: Path, *, max_workers: int = 32
) -> None:
    """Fetch referenced packs in parallel and write the byte-identical payload.

    Each pack's bytes are verified against its content hash in the worker
    (localizes corruption to one object); the assembled file is verified
    against `file_sha256`. A pack 404 (GC swept an old generation mid-read)
    propagates as ObjectNotFoundError so callers re-resolve the head, exactly
    like today's whole-file fetch race.
    """
    keys = sorted(manifest.pack_keys())

    def fetch(key: str) -> tuple[str, bytes]:
        sha = parse_pack_key(key)
        body = store.get(key).body
        actual = hashlib.sha256(body).hexdigest()
        if actual != sha:
            raise ExternalServiceError(f"pack {sha} corrupt: content hashes to {actual}")
        return sha, body

    try:
        if keys:
            with ThreadPoolExecutor(max_workers=min(max_workers, len(keys))) as pool:
                pack_bodies = dict(pool.map(fetch, keys))
        else:
            pack_bodies = {}
        _assemble(manifest, pack_bodies, dest)
    except BaseException:
        dest.unlink(missing_ok=True)  # never leave a partial/corrupt file behind
        raise


def _assemble(manifest: Manifest, pack_bodies: dict[str, bytes], dest: Path) -> None:
    """Write entries' pack slices to `dest` in order; verify the file hash."""
    hasher = hashlib.sha256()
    with dest.open("wb") as out:
        for entry in manifest.entries:
            piece = pack_bodies[entry.pack_sha256][
                entry.pack_offset : entry.pack_offset + entry.length
            ]
            if len(piece) != entry.length:
                raise ExternalServiceError(
                    f"pack {entry.pack_sha256} too short for entry at offset {entry.pack_offset}"
                )
            out.write(piece)
            hasher.update(piece)
    if hasher.hexdigest() != manifest.file_sha256:
        raise ExternalServiceError("reconstructed payload hash mismatch — manifest/packs corrupt")


def load_manifest(store: ObjectStore, payload_key: str) -> Manifest:
    """Fetch and parse the manifest at a chunked generation's payload key."""
    return Manifest.from_bytes(store.get(payload_key).body)


def sniff_manifest(data: bytes) -> Manifest | None:
    """Parse `data` as a manifest if and only if it genuinely is one.

    GC's orphan fallback: engine-written manifests always parse (no false
    negatives — their packs MUST be marked); anything else returns None and
    is treated as an opaque payload (a coincidental magic prefix merely
    fails validation, which only ever under-deletes).
    """
    try:
        return Manifest.from_bytes(data)
    except InputValidationError:
        return None


def novel_pack_index(packs: Iterable[tuple[str, bytes]]) -> dict[str, bytes]:
    """pack_sha256 -> bytes for the packs this writer built (heal source)."""
    return dict(packs)
