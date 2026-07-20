"""The transaction envelope: the library's public API.

A `Lake` wraps an `ObjectStore` and a local working directory. Writers use
`lake.transaction()` — SQL runs against a local copy of the current
catalog generation via the stock ducklake extension, is recorded as a
logical changeset, then committed by creating the next immutable
generation marker (`roots/<gen>`). On a lost race, `decide_rebase` chooses
between replaying the changeset onto the winner's generation and aborting.
An ambiguous marker create resolves by GETting the marker — exact and
permanent (see root.py). Readers use `lake.reader()`.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

from ducklake_serverless import chunk, commit
from ducklake_serverless.commit import writer_info
from ducklake_serverless.engine import (
    DUCKDB_VERSION,
    LakeConnection,
    S3Credentials,
    probe_ducklake_format_version,
)
from ducklake_serverless.errors import (
    ExternalServiceError,
    ObjectNotFoundError,
    VersionMismatchError,
)
from ducklake_serverless.generation import GenerationCache, check_hygiene, publish_generation
from ducklake_serverless.models import (
    Changeset,
    CommitResult,
    ConflictPolicy,
    RootDoc,
    Statement,
    format_payload_key,
)
from ducklake_serverless.objectstore import S3ObjectStore
from ducklake_serverless.rebase import decide_rebase
from ducklake_serverless.recorder import record
from ducklake_serverless.root import (
    MarkerOutcome,
    read_marker,
    resolve_head,
    write_hint,
)

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from ducklake_serverless.models import RebaseDecision
    from ducklake_serverless.objectstore import ObjectStore

DEFAULT_MAX_ATTEMPTS = 5

# DuckLake's entries in the generic RootDoc.pins mapping. These move to the
# DuckLakeCatalog payload adapter in the core/adapters reorg; for now the
# still-DuckLake-coupled Lake writes and reads them here.
_PIN_DUCKDB_VERSION = "duckdb_storage_version"
_PIN_DUCKLAKE_FORMAT = "ducklake_format_version"

# reader(stream="auto") streams over httpfs only above this catalog size. Below
# it, one bulk download beats httpfs's ~16 range GETs (measured crossover is in
# the tens of MB on a ~30ms-RTT link — see docs/benchmarks/httpfs-read-path.md).
STREAM_MIN_BYTES = 32 * 1024 * 1024

# Catalogs at least this large publish via the chunked transport by default.
# Note the interaction with streaming: a chunked generation is not one
# attachable object, so httpfs streaming only ever engages for WHOLE heads —
# with default settings chunking takes over below STREAM_MIN_BYTES and
# streaming never fires. Streaming remains for lakes that opt out of chunking
# (chunk_threshold=None) while their catalogs grow large.
DEFAULT_CHUNK_THRESHOLD = 16 * 1024 * 1024


class Transaction:
    """One open transaction: SQL runs against a private catalog copy.

    Every statement is classified and recorded at this boundary — the
    recording is what makes replay-on-conflict possible.
    """

    def __init__(self, connection: LakeConnection) -> None:
        self._connection = connection
        self._recorded: list[Statement] = []

    def sql(self, statement: str, params: tuple[object, ...] = ()) -> list[tuple[object, ...]]:
        """Classify, record, and execute one statement."""
        stmt = record(statement, params)
        rows = self._connection.execute(statement, params)
        self._recorded.append(stmt)
        return rows

    @property
    def changeset(self) -> Changeset:
        """The statements recorded so far, in execution order."""
        return Changeset(statements=tuple(self._recorded))


@dataclass(frozen=True)
class _DuckLakeCommit:
    """The DuckLake `commit.CommitContext`: the three hooks the driver needs.

    Built per transaction, closing over the recorded changeset and the `Lake`
    that owns the connection machinery. `validate` runs the DuckDB hygiene +
    format-migration guard; `decide_rebase` applies the SQL replay-safety
    policy; `replay` re-executes the changeset onto the current head.
    """

    lake: Lake
    changeset: Changeset

    def validate(self, work: Path, base: RootDoc) -> None:
        """Guard against a silent format migration, then check DuckDB hygiene."""
        self.lake.check_format_unmigrated(work, base)
        check_hygiene(work)

    def decide_rebase(self, attempt: int, max_attempts: int) -> RebaseDecision:
        """Replay blind appends, abort the rest (or per the lake's policy)."""
        return decide_rebase(self.changeset, self.lake.conflict_policy, attempt, max_attempts)

    def replay(self, stale_work: Path) -> tuple[RootDoc, Path]:
        """Discard the losing copy and re-run the changeset onto the head."""
        GenerationCache.discard(stale_work)
        return self.lake.replay_onto_head(self.changeset)

    def transport_policy(self, base: RootDoc) -> commit.TransportPolicy:
        """Chunk large catalogs, deduping against the base's manifest."""
        return self.lake.transport_policy(base)


class Lake:
    """A serverless DuckLake rooted at one object-store prefix."""

    def __init__(
        self,
        store: ObjectStore,
        workdir: Path,
        data_path: str,
        *,
        conflict_policy: ConflictPolicy = ConflictPolicy.APPEND_ONLY_REPLAY,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        s3_credentials: S3Credentials | None = None,
        chunk_threshold: int | None = DEFAULT_CHUNK_THRESHOLD,
    ) -> None:
        """Bind the lake to a store, a scratch dir, and a Parquet destination.

        `data_path` is where DuckDB writes Parquet — an s3:// URL in
        production, a local directory in hermetic tests. No trailing slash
        (upstream ducklake#815 misclassifies files under one as orphans).

        `chunk_threshold`: catalogs at least this many bytes are published
        via the chunked transport (content-addressed packs — see chunk.py);
        smaller ones stay whole-file. None disables chunking entirely; 0
        chunks always. Whole-file generations remain httpfs-streamable.
        """
        self._store = store
        self._workdir = workdir
        self._data_path = data_path.rstrip("/")
        self._cache = GenerationCache(store, workdir)
        self.conflict_policy = conflict_policy
        self._max_attempts = max_attempts
        self._s3_credentials = s3_credentials
        self._chunk_threshold = chunk_threshold

    def transport_policy(self, base: RootDoc) -> commit.TransportPolicy:
        """Chunking policy for a commit onto `base` (dedup source = base only)."""
        base_manifest = None
        if base.transport == "chunked":
            base_manifest = chunk.load_manifest(self._store, base.payload_key)
        return commit.TransportPolicy(
            chunk_threshold=self._chunk_threshold, base_manifest=base_manifest
        )

    def bootstrap(self, *, verify_backend: bool = True) -> RootDoc:
        """Create generation 0 (an empty DuckLake catalog) and its marker.

        Create-only end to end: loses cleanly to any concurrent bootstrap,
        and an ambiguous marker create resolves by GET (same as any commit).

        By default this probes the backend for ATOMIC create-only enforcement
        under concurrency and refuses to create a lake on a store that lacks
        it — the marker protocol serializes commits on `If-None-Match: *`, so
        a store that resolves concurrent creates last-writer-wins (iDrive E2,
        garage, rclone serve s3) would silently lose commits. Pass
        `verify_backend=False` only for a single-writer lake where no
        concurrent create can occur. (See `probe_capabilities`; a v1 root-CAS
        strategy could serve atomic-CAS-only backends, but no tested backend
        needs it — E2 enforces neither primitive atomically.)
        """
        if verify_backend:
            commit.require_atomic_create(self._store)
        payload_uuid = uuid4()
        catalog_path = self._workdir / f"bootstrap-{payload_uuid}.duckdb"
        connection = LakeConnection(
            catalog_path, self._data_path, s3_credentials=self._s3_credentials
        )
        connection.close()

        try:
            publish_generation(self._store, catalog_path, 0, payload_uuid)
            doc = RootDoc(
                generation=0,
                payload_uuid=payload_uuid,
                created_at=datetime.now(tz=UTC),
                writer=writer_info(),
                pins={
                    _PIN_DUCKDB_VERSION: DUCKDB_VERSION,
                    _PIN_DUCKLAKE_FORMAT: probe_ducklake_format_version(catalog_path),
                },
            )
            outcome = commit.create_marker_resolving(
                self._store, doc, payload_uuid, 0, self._max_attempts
            )
            match outcome:
                case MarkerOutcome.WON:
                    pass  # we created generation 0 — publish the hint below
                case MarkerOutcome.LOST:
                    # A concurrent bootstrap won generation 0; adopt its
                    # (equally empty) lake. Our orphan catalog is swept later.
                    return read_marker(self._store, 0)
                case MarkerOutcome.ABSENT:  # helper retries ABSENT, never returns it
                    raise AssertionError("create_marker_resolving returned ABSENT")
        finally:
            GenerationCache.discard(catalog_path)
        write_hint(self._store, 0)
        return doc

    @contextmanager
    def _writable_copy(self, work: Path) -> Generator[LakeConnection]:
        """Attach a work copy for writing; abandon+discard it on any failure.

        On success the connection is closed (checkpointing the file) and the
        work copy is left on disk for the caller to publish and discard.
        """
        connection = LakeConnection(work, self._data_path, s3_credentials=self._s3_credentials)
        try:
            yield connection
        except BaseException:
            connection.abandon()
            GenerationCache.discard(work)
            raise
        connection.close()

    @contextmanager
    def transaction(self) -> Generator[Transaction]:
        """Run SQL against the lake and commit it as one new generation.

        On a lost CAS race the recorded changeset is either replayed onto
        the winner's generation (blind appends, or replay_all policy) or
        the whole call raises ConflictAbortError. VersionMismatchError if
        local versions differ from the lake's pins.
        """
        base, work = self._fetch_current_base()
        with self._writable_copy(work) as connection:
            transaction = Transaction(connection)
            yield transaction

        try:
            self._commit(work, base, transaction.changeset)
        finally:
            GenerationCache.discard(work)

    def check_format_unmigrated(self, work: Path, base: RootDoc) -> None:
        """Refuse to publish a catalog whose format was migrated on ATTACH.

        A newer ducklake extension silently rewrites the catalog format when
        it attaches; the duckdb-version pin cannot catch that — the extension
        versions independently — so probe the file itself before it ships.
        """
        base_format = base.pins.get(_PIN_DUCKLAKE_FORMAT)
        work_format = probe_ducklake_format_version(work)
        if work_format != base_format:
            raise VersionMismatchError(
                f"local ducklake extension migrated the catalog format "
                f"({base_format} -> {work_format}); "
                "publishing would break other readers. Upgrade the lake "
                "explicitly instead."
            )

    def _commit(self, work: Path, base: RootDoc, changeset: Changeset) -> CommitResult:
        """Commit the work copy as the next generation via the generic driver.

        Wraps the recorded changeset and this lake in a `_DuckLakeCommit`
        context (validate / rebase-decision / replay) and hands the create-only
        marker protocol to `commit.run_commit`.
        """
        ctx = _DuckLakeCommit(lake=self, changeset=changeset)
        return commit.run_commit(self._store, base, work, ctx, self._max_attempts)

    def replay_onto_head(self, changeset: Changeset) -> tuple[RootDoc, Path]:
        """Resolve head, fetch its catalog, and re-execute the changeset onto it.

        Returns the head doc and the mutated work copy, ready to publish as
        head+1. GC-race-safe via `_fetch_current_base`.
        """
        head, work = self._fetch_current_base()
        with self._writable_copy(work) as connection:
            for statement in changeset.statements:
                connection.execute(statement.sql, statement.params)
        return head, work

    def _fetch_current_base(self) -> tuple[RootDoc, Path]:
        """Resolve the current head and fetch its catalog, GC-race-safe.

        Between resolving the head and fetching its catalog, GC may sweep
        that generation. No user SQL has run yet (or, on the rebase path, the
        replay hasn't started), so re-resolving and retrying is always
        correct — resolve_head always yields a currently-extant marker.
        """
        for _ in range(self._max_attempts):
            base, _ = resolve_head(self._store)
            self._check_versions(base)
            try:
                return base, self._cache.fetch_copy(base)
            except ObjectNotFoundError:
                continue
        raise ExternalServiceError(
            f"catalog for the current head kept vanishing across "
            f"{self._max_attempts} attempts — GC retention is too aggressive "
            "for this commit rate"
        )

    @contextmanager
    def scratch(self) -> Generator[LakeConnection]:
        """Writable connection on a throwaway CATALOG copy — never published.

        Only catalog mutations are discarded on exit. Statements that act on
        DATA_PATH (inserts spilling Parquet, maintenance CALLs without
        ``dry_run => true``) still hit the shared bucket immediately — run
        exclusively dry-run/read statements here.
        """
        _, work = self._fetch_current_base()
        try:
            connection = LakeConnection(work, self._data_path, s3_credentials=self._s3_credentials)
        except BaseException:
            GenerationCache.discard(work)
            raise
        try:
            yield connection
        finally:
            connection.abandon()
            GenerationCache.discard(work)

    @contextmanager
    def reader(self, *, stream: bool | Literal["auto"] = False) -> Generator[LakeConnection]:
        """Attach the current generation READ_ONLY (frozen-DuckLake pattern).

        `stream` selects how the catalog is fetched:

        - `False` (default): download the catalog, attach the local copy. One
          bulk GET — best for the common case (small catalog, or any backend).
        - `True`: attach the catalog directly from S3 over httpfs, no download.
          Requires an S3-backed store and credentials. A selective read pulls
          only the blocks it needs (fewer bytes for a large catalog) but takes
          ~16 range GETs vs one download, so it only wins for a LARGE catalog
          over a HIGH-LATENCY backend.
        - `"auto"`: stream only when the store is S3-backed and the catalog is
          at least `STREAM_MIN_BYTES`; otherwise download.
        """
        stream_store = self._stream_store(stream)
        if stream_store is not None:
            connection = self._open_streaming_reader(stream_store)
            try:
                yield connection
            finally:
                connection.abandon()
            return
        _, path = self._fetch_current_base()
        try:
            connection = LakeConnection(
                path, data_path=None, read_only=True, s3_credentials=self._s3_credentials
            )
        except BaseException:
            GenerationCache.discard(path)
            raise
        try:
            yield connection
        finally:
            connection.abandon()
            GenerationCache.discard(path)

    def _stream_store(self, stream: bool | Literal["auto"]) -> S3ObjectStore | None:
        """The S3 store to stream the catalog from, or None to download instead.

        Returning the narrowed store (not a bool) lets the httpfs path stay
        typed without an assert.
        """
        if stream is False:
            return None
        store = self._store
        if not isinstance(store, S3ObjectStore):
            if stream is True:
                raise ExternalServiceError("streaming reads require an S3-backed store")
            return None  # auto on a non-S3 store → download
        if self._s3_credentials is None:
            if stream is True:
                raise ExternalServiceError("streaming reads require s3_credentials for httpfs")
            return None  # auto with no creds → download
        base, _ = resolve_head(store)
        if base.transport != "whole":
            # A chunked generation is a manifest, not one attachable object —
            # httpfs cannot stream it. The transport gate precedes the size
            # heuristic (a manifest's small size would silently mask this).
            if stream is True:
                raise ExternalServiceError(
                    "streaming reads require a whole-file head; the current "
                    "generation is chunked — use the download reader"
                )
            return None  # auto on a chunked head → download (reconstruct)
        if stream is True:
            return store
        # Marker-recorded size: no HEAD round trip for the auto heuristic.
        return store if base.payload_size >= STREAM_MIN_BYTES else None

    def _open_streaming_reader(self, store: S3ObjectStore) -> LakeConnection:
        """Attach the current head's catalog directly from S3 (httpfs), no download.

        GC may sweep the resolved generation before the attach lands; re-resolve
        and retry, mirroring `_fetch_current_base`'s download-path race handling.
        """
        last_exc: ExternalServiceError | None = None
        for _ in range(self._max_attempts):
            base, _ = resolve_head(store)
            self._check_versions(base)
            if base.transport != "whole":
                # The head flipped to a chunked generation since the stream
                # decision — a manifest is not attachable; refuse loudly.
                raise ExternalServiceError(
                    "streaming reads require a whole-file head; the current "
                    "generation is chunked — use the download reader"
                )
            uri = store.s3_uri(format_payload_key(base.generation, base.payload_uuid))
            try:
                return LakeConnection(
                    uri, data_path=None, read_only=True, s3_credentials=self._s3_credentials
                )
            except ExternalServiceError as exc:  # catalog may have been swept mid-attach
                last_exc = exc
        raise ExternalServiceError(
            f"streaming attach for the current head kept failing across "
            f"{self._max_attempts} attempts"
        ) from last_exc

    def _check_versions(self, root: RootDoc) -> None:
        """Refuse to write when local versions differ from the lake's pins.

        A newer ducklake extension would silently migrate the catalog format
        for the whole fleet on ATTACH; upgrades must be explicit.
        """
        lake_duckdb = root.pins.get(_PIN_DUCKDB_VERSION)
        if lake_duckdb != DUCKDB_VERSION:
            raise VersionMismatchError(
                f"lake pins duckdb {lake_duckdb}, "
                f"local is {DUCKDB_VERSION}; upgrade the lake explicitly"
            )
