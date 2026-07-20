"""Read-only fsspec filesystem over generations — selective reads on chunked.

This dissolves the streaming-vs-chunking tradeoff. httpfs streaming gives
DuckDB *selective* reads (only the blocks it touches) but requires each
generation to be ONE S3 object — which chunking abolished. What streaming
actually needs is random access by byte range, and a chunked generation's
manifest is exactly a range index: fixed-offset chunks mean any file range
maps to a contiguous run of entries, each naming `(pack, pack_offset,
length)`. This filesystem translates range reads into ranged GETs of only
the covering pack slices — streaming-equivalent selective reads over a
CHUNKED generation, plus something httpfs never had: per-chunk hash
verification of every fully-covered chunk it serves.

Paths name generations explicitly (immutable snapshots, in keeping with the
frozen-generation reading model):

    head            — the current head generation (resolved once per open)
    gen/<number>    — a specific generation

Whole-file generations are served by ranged GETs against their single
payload object; chunked generations by manifest translation. Everything is
read-only: any write/mutation entry point raises.

Requires the `fsspec` extra.

Consumers: anything fsspec-aware reads generations selectively —
`fs.open("head")` for pandas/polars/pyarrow, `fsspec.open` chains, or plain
file-like code. DuckDB's scan functions (`read_parquet`/`read_csv`) also go
through registered fsspec filesystems.

Known limitation — DuckDB ATTACH: DuckDB's `ATTACH` opens database files
through its C++ filesystem layer only (native paths + httpfs); registered
fsspec filesystems are consulted for scans, never for ATTACH (verified
against duckdb 1.5; upstream docs concur). Attaching a chunked DuckLake
generation therefore still goes through local reconstruction
(`Lake.reader()` / `GenerationCache.fetch_copy`) — which is windowed,
parallel, and cached. This filesystem gives every OTHER reader selective
access, and pins the head/gen-N snapshot semantics DuckDB readers get from
the reconstruction path.
"""

from __future__ import annotations

# fsspec ships no py.typed, so its base classes are Unknown to basedpyright.
# Per the repo contract (same treatment as duckdb in engine.py), this module
# IS the fsspec facade: the suppressions for the untyped dependency live here
# and nowhere else. Our own code below the facade line stays strictly typed.
# pyright: reportMissingImports=false, reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false, reportUntypedBaseClass=false
# pyright: reportGeneralTypeIssues=false, reportUnknownArgumentType=false
import bisect
import hashlib
from typing import TYPE_CHECKING, Any, override

from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile

from ducklake_serverless import chunk
from ducklake_serverless.errors import (
    ExternalServiceError,
    InputValidationError,
    ObjectNotFoundError,
)
from ducklake_serverless.root import read_marker, resolve_head

if TYPE_CHECKING:
    from ducklake_serverless.chunk import Manifest
    from ducklake_serverless.models import RootDoc
    from ducklake_serverless.objectstore import ObjectStore

# Verify chunk hashes on selective reads. Only chunks FULLY covered by a
# fetched slice can be verified (a partial chunk read can't be hashed);
# sequential scans therefore verify nearly everything they touch.
VERIFY_CHUNKS = True


class _RangeReader:
    """Random byte-range access over one generation's payload."""

    size: int

    def read_range(self, start: int, length: int) -> bytes:
        """Bytes [start, start+length) of the payload, truncated at EOF."""
        raise NotImplementedError


class _WholeReader(_RangeReader):
    """Ranged GETs straight against a whole-file generation's single object."""

    def __init__(self, store: ObjectStore, payload_key: str) -> None:
        self._store = store
        self._key = payload_key
        self.size = store.head_meta(payload_key).size

    @override
    def read_range(self, start: int, length: int) -> bytes:
        return self._store.get_range(self._key, start, length)


class _ChunkedReader(_RangeReader):
    """Manifest-translated selective reads over a chunked generation.

    Fixed-offset chunks make the translation a pure interval computation:
    entry i covers file offsets [starts[i], starts[i] + entries[i].length).
    A requested range maps (via bisect) to a contiguous entry run; each
    entry's bytes come from ONE ranged GET of its pack slice. Adjacent
    entries living contiguously in the same pack coalesce into a single GET
    (the common case by construction — build_manifest packs novel chunks in
    file order).
    """

    def __init__(self, store: ObjectStore, manifest: Manifest) -> None:
        self._store = store
        self._manifest = manifest
        self.size = manifest.total_size
        # File-offset index. Entries are in file order (manifest invariant);
        # lengths are all chunk_size except the tail, but trusting only
        # "in order" keeps this correct even for future variable tails.
        self._starts: list[int] = []
        off = 0
        for e in manifest.entries:
            self._starts.append(off)
            off += e.length
        if off != manifest.total_size:
            raise ExternalServiceError("manifest entries do not tile total_size — manifest corrupt")

    @override
    def read_range(self, start: int, length: int) -> bytes:
        end = min(start + max(0, length), self.size)
        if start >= end:
            return b""
        entries = self._manifest.entries
        first = bisect.bisect_right(self._starts, start) - 1
        out = bytearray()
        pos = start
        idx = first
        while pos < end:
            entry = entries[idx]
            entry_start = self._starts[idx]
            # Coalesce a maximal run of entries that are CONTIGUOUS in the
            # same pack — one ranged GET serves the whole run.
            run_end_idx = idx
            pack_off_end = entry.pack_offset + entry.length
            while (
                run_end_idx + 1 < len(entries)
                and self._starts[run_end_idx + 1] < end
                and entries[run_end_idx + 1].pack_sha256 == entry.pack_sha256
                and entries[run_end_idx + 1].pack_offset == pack_off_end
            ):
                run_end_idx += 1
                pack_off_end += entries[run_end_idx].length
            run_file_start = entry_start
            run_file_end = self._starts[run_end_idx] + entries[run_end_idx].length
            # Clip the run to the requested range, then translate to pack space.
            want_start = max(pos, run_file_start)
            want_end = min(end, run_file_end)
            pack_start = entry.pack_offset + (want_start - run_file_start)
            got = self._store.get_range(
                chunk.format_pack_key(entry.pack_sha256), pack_start, want_end - want_start
            )
            if len(got) != want_end - want_start:
                raise ExternalServiceError(
                    f"pack {entry.pack_sha256} returned a short range — "
                    "pack truncated or swept mid-read"
                )
            if VERIFY_CHUNKS:
                self._verify_covered_chunks(idx, run_end_idx, want_start, want_end, got)
            out += got
            pos = want_end
            idx = run_end_idx + 1
        return bytes(out)

    def _verify_covered_chunks(
        self, first_idx: int, last_idx: int, got_start: int, got_end: int, got: bytes
    ) -> None:
        """Hash-verify every chunk FULLY covered by the fetched slice."""
        entries = self._manifest.entries
        for i in range(first_idx, last_idx + 1):
            c_start = self._starts[i]
            c_end = c_start + entries[i].length
            if c_start >= got_start and c_end <= got_end:
                piece = got[c_start - got_start : c_end - got_start]
                if hashlib.sha256(piece).hexdigest() != entries[i].chunk_sha256:
                    raise ExternalServiceError(
                        f"chunk at file offset {c_start} failed hash verification "
                        f"— pack {entries[i].pack_sha256} corrupt"
                    )


def _reader_for(store: ObjectStore, doc: RootDoc) -> _RangeReader:
    match doc.transport:
        case "whole":
            return _WholeReader(store, doc.payload_key)
        case "chunked":
            return _ChunkedReader(store, chunk.load_manifest(store, doc.payload_key))


class GenerationFile(AbstractBufferedFile):
    """Read-only buffered file over one generation (fsspec plumbing)."""

    def __init__(
        self, fs: GenerationFileSystem, path: str, reader: _RangeReader, **kwargs: Any
    ) -> None:
        self._reader = reader
        super().__init__(fs, path, mode="rb", size=reader.size, **kwargs)

    @override
    def _fetch_range(self, start: int, end: int) -> bytes:
        return self._reader.read_range(start, end - start)


class GenerationFileSystem(AbstractFileSystem):  # pyright: ignore[reportUnsafeMultipleInheritance]
    """Read-only fsspec filesystem exposing generations as files.

    `ducklake-serverless://head` is the current head (resolved at open —
    each open is a consistent immutable snapshot); `ducklake-serverless://
    gen/<n>` pins a specific generation. Selective reads: chunked
    generations fetch only the pack slices covering each requested range.
    """

    protocol = "ducklake-serverless"
    root_marker = ""

    def __init__(self, store: ObjectStore, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._store = store

    def _resolve(self, path: str) -> RootDoc:
        name = self._strip_protocol(path).strip("/")
        if name == "head":
            doc, _ = resolve_head(self._store)
            return doc
        if name.startswith("gen/"):
            try:
                generation = int(name.removeprefix("gen/"))
            except ValueError:
                raise InputValidationError(f"not a generation path: {path!r}") from None
            return read_marker(self._store, generation)
        raise InputValidationError(f"unknown path {path!r} — use 'head' or 'gen/<number>'")

    @override
    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> GenerationFile:
        if mode != "rb":
            raise NotImplementedError("generations are immutable — read-only ('rb')")
        doc = self._resolve(path)
        return GenerationFile(
            self,
            path,
            _reader_for(self._store, doc),
            block_size=block_size,
            cache_options=cache_options,
            # Forward caching kwargs (cache_type etc.) — dropping them here
            # silently reinstates fsspec's default readahead, defeating
            # callers who asked for exact-range reads.
            **kwargs,
        )

    @override
    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Size/type for one generation path (fsspec metadata contract)."""
        doc = self._resolve(path)
        size = _reader_for(self._store, doc).size
        return {
            "name": self._strip_protocol(path).strip("/"),
            "size": size,
            "type": "file",
            "generation": doc.generation,
            "transport": doc.transport,
        }

    @override
    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[Any]:
        """List the head and every extant generation marker."""
        head_doc, _ = resolve_head(self._store)
        names = ["head"] + [f"gen/{g}" for g in range(head_doc.generation + 1)]
        entries: list[Any] = []
        for name in names:
            try:
                entries.append(self.info(name) if detail else name)
            except (ObjectNotFoundError, ExternalServiceError):
                continue  # swept or unreadable generation — omit
        return entries

    @override
    def exists(self, path: str, **kwargs: Any) -> bool:
        """Whether the path names a resolvable generation."""
        try:
            self._resolve(path)
        except (InputValidationError, ObjectNotFoundError, ExternalServiceError):
            return False
        return True

    # Read-only: every mutation entry point fails loudly.
    def _readonly(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("GenerationFileSystem is read-only")

    rm = _readonly
    rm_file = _readonly
    mv = _readonly
    touch = _readonly
    mkdir = _readonly
    makedirs = _readonly
    rmdir = _readonly
    pipe_file = _readonly
    put_file = _readonly
