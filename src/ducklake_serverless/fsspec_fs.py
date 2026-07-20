"""Read-only fsspec filesystem over generations — selective reads on chunked.

This dissolves the streaming-vs-chunking tradeoff. httpfs streaming gives
DuckDB *selective* reads (only the blocks it touches) but requires each
generation to be ONE S3 object — which chunking abolished. What streaming
actually needs is random access by byte range, and a chunked generation's
manifest is exactly a range index: fixed-offset chunks mean any file range
maps to a contiguous run of entries, each naming `(pack, pack_offset,
length)`. This filesystem translates range reads into ranged GETs of only
the covering pack slices — streaming-equivalent selective reads over a
CHUNKED generation, plus something httpfs never had: hash verification of
every chunk a fetched slice fully covers.

Paths name generations explicitly (immutable snapshots, in keeping with the
frozen-generation reading model):

    head            — the current head generation (resolved once per open)
    gen/<number>    — a specific generation (canonical decimal digits only)

`head` re-resolves per call: a single open is a consistent snapshot, but an
info-then-open pair (or multiple opens) can straddle a commit. Multi-open
consumers (pyarrow datasets, dask) should pin first: `path = fs.pin("head")`
returns the concrete `gen/<n>` path for the current head.

Whole-file generations are served by ranged GETs against their single
payload object; chunked generations by manifest translation. Everything is
read-only: mutation entry points raise. Missing paths surface as
`FileNotFoundError` (the fsspec ecosystem contract); transport failures
surface as this library's domain errors — an outage is never reported as
absence.

Requires the `fsspec` extra. Scope, honestly stated:

- Use by INSTANCE: `fs = GenerationFileSystem(store)` then `fs.open(...)`,
  `pandas.read_parquet(..., filesystem=fs)`, or DuckDB
  `con.register_filesystem(fs)` for its scan functions
  (`read_parquet`/`read_csv`/`read_blob`). URL-only construction
  (`fsspec.open("ducklake-serverless://...")`) is NOT supported — the
  constructor needs a live ObjectStore, and the class is deliberately not
  registered with fsspec's registry.
- Process-local: the filesystem (and its store/boto3 client) does not
  pickle; construct per process for dask/multiprocessing.
- fsspec's default readahead cache prefetches ~5 MB per miss. For exact
  point reads pass `cache_type="none"` (or a small `block_size`) to
  `fs.open`.

Known limitation — DuckDB ATTACH: DuckDB's `ATTACH` opens database files
through its C++ filesystem layer only (native paths + httpfs); registered
fsspec filesystems are consulted for scans, never for ATTACH (verified
against duckdb 1.5; upstream docs concur). Attaching a chunked DuckLake
generation therefore still goes through local reconstruction
(`Lake.reader()` / `GenerationCache.fetch_copy`) — which is windowed,
parallel, and cached.
"""

from __future__ import annotations

# fsspec ships no py.typed. Per AGENTS.md's untyped-dependency pattern this
# module is its one facade: downgrade exactly the Unknown-type trio to
# warnings here; everything else stays strict, with per-line ignores at the
# few fsspec call sites.
# pyright: reportUnknownMemberType=warning, reportUnknownArgumentType=warning
# pyright: reportUnknownVariableType=warning
import bisect
import contextlib
import hashlib
import re
from typing import TYPE_CHECKING, Any, override

from fsspec import AbstractFileSystem  # pyright: ignore[reportMissingImports]  # no py.typed
from fsspec.spec import AbstractBufferedFile  # pyright: ignore[reportMissingImports]  # no py.typed

from ducklake_serverless import chunk
from ducklake_serverless.errors import (
    ExternalServiceError,
    InputValidationError,
    NotFoundError,
)
from ducklake_serverless.root import read_marker, resolve_head

if TYPE_CHECKING:
    from ducklake_serverless.chunk import Manifest
    from ducklake_serverless.models import RootDoc
    from ducklake_serverless.objectstore import ObjectStore

# Canonical generation path: decimal digits only. int()'s leniency
# (underscores, '+', whitespace, unicode digits) would otherwise make
# 'gen/5_0' a silent alias of generation 50 — wrong-snapshot reads with
# valid checksums, the worst failure shape for an immutable-history store.
_GEN_RE = re.compile(r"^gen/(0|[1-9][0-9]*)$")


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
        if start < 0:
            raise InputValidationError("negative read offset")
        return self._store.get_range(self._key, start, length)


class _ChunkedReader(_RangeReader):
    """Manifest-translated selective reads over a chunked generation.

    Fixed-offset chunks make the translation a pure interval computation:
    entry i covers file offsets [starts[i], starts[i] + entries[i].length).
    A requested range maps (via bisect) to a contiguous entry run; each
    entry's bytes come from ONE ranged GET of its pack slice. Adjacent
    entries living contiguously in the same pack coalesce into a single GET
    (the common case by construction — build_manifest packs novel chunks in
    file order). Every chunk a fetched slice fully covers is hash-verified;
    partially-covered edge chunks cannot be (no full bytes to hash).
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
        if start < 0:
            # bisect with a negative start would wrap to the LAST entry and
            # return wrong bytes; unreachable via fsspec (seek rejects
            # negatives) but this seam must not rely on its callers.
            raise InputValidationError("negative read offset")
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
            # Clip the run to the requested range, then translate to pack
            # space (within a run, file-delta == pack-delta by construction).
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


class GenerationFile(AbstractBufferedFile):  # pyright: ignore[reportUntypedBaseClass]
    """Read-only buffered file over one generation (fsspec plumbing)."""

    def __init__(
        self, fs: GenerationFileSystem, path: str, reader: _RangeReader, **kwargs: Any
    ) -> None:
        self._reader = reader
        super().__init__(fs, path, mode="rb", size=reader.size, **kwargs)  # pyright: ignore[reportUnknownMemberType]

    def _fetch_range(self, start: int, end: int) -> bytes:
        return self._reader.read_range(start, end - start)


class GenerationFileSystem(AbstractFileSystem):  # pyright: ignore[reportUntypedBaseClass, reportUnsafeMultipleInheritance]  # fsspec's _Cached metaclass mixes untyped state pyright can't see
    """Read-only fsspec filesystem exposing generations as files.

    `head` is the current head (resolved per call — see the module docstring
    for pinning); `gen/<n>` pins a specific generation. Selective reads:
    chunked generations fetch only the pack slices covering each requested
    range. Missing paths raise FileNotFoundError; transport failures raise
    domain errors (never reported as absence).
    """

    protocol = "ducklake-serverless"
    root_marker = ""
    # fsspec's instance cache tokenizes ctor args via str(); a live store's
    # default repr is its memory address — repr-fragile (any store class
    # adding a value-style __repr__ would silently alias DIFFERENT lakes to
    # one cached filesystem: wrong-lake reads with no error) and the cache
    # strong-refs every store forever. A live handle is not a cache key.
    cachable = False

    def __init__(self, store: ObjectStore, **kwargs: Any) -> None:
        super().__init__(**kwargs)  # pyright: ignore[reportUnknownMemberType]
        self._store = store
        # Readers memoized per payload key: generations are immutable, so
        # this is trivially correct, and it stops the info-then-open pattern
        # (pandas/pyarrow) from downloading a chunked generation's manifest
        # twice — or ls() from downloading one per generation per call.
        self._readers: dict[str, _RangeReader] = {}

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        # Base strips 'proto://' and trailing '/'; also strip leading '/'
        # (the memory-filesystem pattern) so inherited helpers (_parent,
        # glob machinery) agree with our protocol-free, slash-free names.
        stripped: str = super()._strip_protocol(path)  # pyright: ignore[reportUnknownMemberType]
        return stripped.lstrip("/")

    def pin(self, path: str = "head") -> str:
        """Resolve `path` to its concrete, immutable `gen/<n>` form.

        Multi-open consumers should pin once and use the returned path for
        every subsequent open, so all reads come from one snapshot.
        """
        return f"gen/{self._resolve(path).generation}"

    def _resolve(self, path: str) -> RootDoc:
        """Marker for the generation `path` names.

        Raises FileNotFoundError for anything that does not resolve to an
        extant generation (fsspec ecosystem contract — consumers probe with
        `except FileNotFoundError`); transport failures propagate as domain
        errors so an outage is never mistaken for absence.
        """
        name = self._strip_protocol(path)
        if name == "head":
            try:
                doc, _ = resolve_head(self._store)
            except NotFoundError as exc:  # uninitialized lake: no roots/0 yet
                raise FileNotFoundError(path) from exc
            return doc
        m = _GEN_RE.match(name)
        if m is None:
            raise FileNotFoundError(path)
        try:
            return read_marker(self._store, int(m.group(1)))
        except NotFoundError as exc:  # no such generation
            raise FileNotFoundError(path) from exc

    def _reader(self, doc: RootDoc) -> _RangeReader:
        reader = self._readers.get(doc.payload_key)
        if reader is None:
            try:
                reader = _reader_for(self._store, doc)
            except NotFoundError as exc:  # payload swept (generation aged out)
                raise FileNotFoundError(doc.payload_key) from exc
            except InputValidationError as exc:  # unparseable/oversized manifest
                raise ExternalServiceError(
                    f"{doc.payload_key}: committed chunked generation has an "
                    "unreadable manifest — corrupt payload object"
                ) from exc
            self._readers[doc.payload_key] = reader
        return reader

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
            self._reader(doc),
            block_size=block_size,
            cache_options=cache_options,
            # Forward caching kwargs (cache_type etc.) — dropping them here
            # silently reinstates fsspec's default readahead, defeating
            # callers who asked for exact-range reads.
            **kwargs,
        )

    def _dir_entry(self, name: str) -> dict[str, Any]:
        return {"name": name, "size": 0, "type": "directory"}

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Size/type for one path (fsspec metadata contract).

        `""` (root) and `gen` are directories; `head` and `gen/<n>` are
        files. `etag` is a stable immutable identity (the payload key), so
        fsspec cache layers (filecache/blockcache) key correctly forever.
        """
        name = self._strip_protocol(path)
        if name in ("", "gen"):
            return self._dir_entry(name)
        doc = self._resolve(path)
        return {
            "name": name,
            # Marker-recorded size: listings never fetch payloads/manifests.
            "size": doc.payload_size,
            "type": "file",
            "generation": doc.generation,
            "transport": doc.transport,
            "etag": doc.payload_key,
        }

    def checksum(self, path: str) -> str:  # pyright: ignore[reportIncompatibleMethodOverride]  # base returns int; stable str is strictly more useful and fsspec stringifies it
        """Stable content identity — generations are immutable."""
        return self._resolve(path).payload_key

    def ukey(self, path: str) -> str:
        """Stable content identity for cache validity (same as checksum)."""
        return self._resolve(path).payload_key

    def ls(self, path: str = "", detail: bool = True, **kwargs: Any) -> list[Any]:
        """List AT `path` (fsspec contract).

        Root -> head + the gen/ directory; gen -> generation entries; a file
        path -> that entry alone. Unreadable individual generations (swept payloads, corrupt
        manifests) are omitted in BOTH detail modes; transport failures
        propagate — an outage must not masquerade as an empty lake.
        """
        name = self._strip_protocol(path)
        if name == "":
            entries: list[dict[str, Any]] = []
            with contextlib.suppress(FileNotFoundError):  # uninitialized lake
                entries.append(self.info("head"))
            entries.append(self._dir_entry("gen"))
            return entries if detail else [e["name"] for e in entries]
        if name == "gen":
            try:
                head_doc, _ = resolve_head(self._store)
            except NotFoundError as exc:
                raise FileNotFoundError(path) from exc
            gens: list[dict[str, Any]] = []
            for g in range(head_doc.generation + 1):
                try:
                    gens.append(self.info(f"gen/{g}"))
                except (FileNotFoundError, InputValidationError):
                    continue  # swept payload or corrupt manifest — omit
            return gens if detail else [e["name"] for e in gens]
        entry = self.info(name)  # a file path: [info(path)] per convention
        return [entry] if detail else [entry["name"]]

    def exists(self, path: str, **kwargs: Any) -> bool:
        """Whether the path resolves.

        Transport failures PROPAGATE — an S3 outage must not read as "the
        lake does not exist".
        """
        name = self._strip_protocol(path)
        if name in ("", "gen"):
            return True
        try:
            self._resolve(path)
        except FileNotFoundError:
            return False
        return True

    def isdir(self, path: str) -> bool:
        """Only the root and `gen` are directories."""
        return self._strip_protocol(path) in ("", "gen")

    def isfile(self, path: str) -> bool:
        """Whether the path names a resolvable generation file."""
        name = self._strip_protocol(path)
        if name in ("", "gen"):
            return False
        return self.exists(path)

    # Read-only: every mutation entry point fails loudly.

    def _readonly(self, *args: Any, **kwargs: Any) -> Any:  # pyright: ignore[reportAny]  # signature-compatible stub for N inherited methods
        raise NotImplementedError("GenerationFileSystem is read-only")

    rm = _readonly
    _rm = _readonly
    rm_file = _readonly
    mv = _readonly
    cp_file = _readonly
    touch = _readonly
    mkdir = _readonly
    makedirs = _readonly
    rmdir = _readonly
    pipe_file = _readonly
    put_file = _readonly
