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

import os
import random
import socket
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from ducklake_serverless import __version__
from ducklake_serverless.engine import (
    DUCKDB_VERSION,
    LakeConnection,
    S3Credentials,
    probe_ducklake_format_version,
)
from ducklake_serverless.errors import (
    AmbiguousCasError,
    BackendUnsafeError,
    ConditionalConflictError,
    ConflictAbortError,
    ExternalServiceError,
    ObjectNotFoundError,
    PreconditionFailedError,
    VersionMismatchError,
)
from ducklake_serverless.generation import GenerationCache, publish_generation
from ducklake_serverless.models import (
    Abort,
    Changeset,
    CommitResult,
    ConflictPolicy,
    Replay,
    RootDoc,
    Statement,
    WriterInfo,
    format_catalog_key,
)
from ducklake_serverless.objectstore import probe_atomic_create, probe_capabilities
from ducklake_serverless.rebase import decide_rebase
from ducklake_serverless.recorder import record
from ducklake_serverless.root import (
    MarkerOutcome,
    create_marker,
    read_marker,
    resolve_head,
    resolve_marker,
    write_hint,
)

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from ducklake_serverless.objectstore import ObjectStore

DEFAULT_MAX_ATTEMPTS = 5
_BACKOFF_BASE_S = 0.05
_BACKOFF_CAP_S = 2.0


def _writer_info() -> WriterInfo:
    return WriterInfo(lib_version=__version__, host=socket.gethostname(), pid=os.getpid())


def _backoff(attempt: int) -> None:
    delay: float = min(_BACKOFF_CAP_S, _BACKOFF_BASE_S * (2.0**attempt))
    jitter: float = random.uniform(0, delay)  # noqa: S311  # jitter, not crypto
    time.sleep(jitter)


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
class _Attempt:
    """Commit phase: publish + create the marker at `base.generation + 1`."""

    base: RootDoc
    work: Path
    attempt: int


@dataclass(frozen=True)
class _Committed:
    """Terminal commit phase: the marker landed (created, or resolved WON)."""

    result: CommitResult


@dataclass(frozen=True)
class _Aborted:
    """Terminal commit phase: lost the race and the changeset can't replay."""

    reason: str


# The commit loop is a small state machine over these phases: it only ever
# sequences whatever `_advance` returns, and the terminal phases exit on match,
# so an illegal transition (e.g. continuing after Committed) is unrepresentable.
_CommitPhase = _Attempt | _Committed | _Aborted


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
    ) -> None:
        """Bind the lake to a store, a scratch dir, and a Parquet destination.

        `data_path` is where DuckDB writes Parquet — an s3:// URL in
        production, a local directory in hermetic tests. No trailing slash
        (upstream ducklake#815 misclassifies files under one as orphans).
        """
        self._store = store
        self._workdir = workdir
        self._data_path = data_path.rstrip("/")
        self._cache = GenerationCache(store, workdir)
        self._conflict_policy = conflict_policy
        self._max_attempts = max_attempts
        self._s3_credentials = s3_credentials

    def _require_atomic_create(self) -> None:
        """Refuse a backend whose create-only isn't atomic under concurrency."""
        if not probe_atomic_create(self._store):
            # Re-probe the full set only to enrich the refusal diagnostic; the
            # happy path above pays for a single contention round, not two.
            caps = probe_capabilities(self._store)
            raise BackendUnsafeError(
                "backend does not enforce If-None-Match: * atomically under "
                "concurrency (concurrent create-only PUTs all 'win', silently "
                "losing commits) — it cannot serialize a marker-protocol lake. "
                f"Probed capabilities: {caps}. Use an atomic backend (MinIO, "
                "AWS S3, SeaweedFS) for concurrent writers, or "
                "bootstrap(verify_backend=False) for a single-writer lake."
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
            self._require_atomic_create()
        catalog_uuid = uuid4()
        catalog_path = self._workdir / f"bootstrap-{catalog_uuid}.duckdb"
        connection = LakeConnection(
            catalog_path, self._data_path, s3_credentials=self._s3_credentials
        )
        connection.close()

        try:
            publish_generation(self._store, catalog_path, 0, catalog_uuid)
            doc = RootDoc(
                generation=0,
                catalog_uuid=catalog_uuid,
                duckdb_storage_version=DUCKDB_VERSION,
                ducklake_format_version=probe_ducklake_format_version(catalog_path),
                created_at=datetime.now(tz=UTC),
                writer=_writer_info(),
            )
            match self._create_marker_resolving(doc, catalog_uuid, 0):
                case MarkerOutcome.WON:
                    pass  # we created generation 0 — publish the hint below
                case MarkerOutcome.LOST:
                    # A concurrent bootstrap won generation 0; adopt its
                    # (equally empty) lake. Our orphan catalog is swept later.
                    return read_marker(self._store, 0)
                case MarkerOutcome.ABSENT:  # helper retries ABSENT, never returns it
                    raise AssertionError("_create_marker_resolving returned ABSENT")
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

    def _check_format_unmigrated(self, work: Path, base: RootDoc) -> None:
        """Refuse to publish a catalog whose format was migrated on ATTACH.

        A newer ducklake extension silently rewrites the catalog format when
        it attaches; the duckdb-version pin cannot catch that — the extension
        versions independently — so probe the file itself before it ships.
        """
        work_format = probe_ducklake_format_version(work)
        if work_format != base.ducklake_format_version:
            raise VersionMismatchError(
                f"local ducklake extension migrated the catalog format "
                f"({base.ducklake_format_version} -> {work_format}); "
                "publishing would break other readers. Upgrade the lake "
                "explicitly instead."
            )

    def _publish_generation_resolved(
        self, work: Path, generation: int, new_uuid: UUID, attempt: int
    ) -> bool:
        """Upload a catalog generation, resolving ambiguous outcomes.

        The upload is create-only to a unique immutable key, so ambiguity
        resolves with one GET: present means it landed. Returns False when
        the upload definitively did not land (caller retries with backoff).
        """
        try:
            publish_generation(self._store, work, generation, new_uuid)
        except AmbiguousCasError:
            try:
                self._store.get(format_catalog_key(generation, new_uuid))
            except ObjectNotFoundError:
                if attempt + 1 >= self._max_attempts:
                    raise ConflictAbortError(
                        f"catalog upload kept failing across {self._max_attempts} attempts"
                    ) from None
                return False
        return True

    def _commit(self, work: Path, base: RootDoc, changeset: Changeset) -> CommitResult:
        """Create the next generation marker, rebasing onto the head until won.

        A marker create is create-only, so both a 412 (someone won this
        generation) and an ambiguous outcome resolve by GETting the marker:
        our uuid means WON (permanent — the marker is immutable and immortal),
        anyone else's means LOST, absent means our create never landed. Runs as
        a small state machine (see `_CommitPhase`): each `_advance` performs one
        attempt and returns the next phase; terminals exit here.
        """
        phase: _CommitPhase = _Attempt(base=base, work=work, attempt=0)
        while True:
            match phase:
                case _Committed():
                    return phase.result
                case _Aborted():
                    raise ConflictAbortError(phase.reason)
                case _Attempt():
                    phase = self._advance(phase, changeset)

    def _advance(self, phase: _Attempt, changeset: Changeset) -> _CommitPhase:
        """Run one commit attempt and return the next phase.

        Publish the catalog and create the marker at `base.generation + 1`. A
        GC-race on publish retries the same base; a win commits; a loss consults
        `decide_rebase` — abort, or rebase onto the current HEAD (not just the
        collided marker — a stale writer must not burn one attempt per
        generation of lag) and try the next generation.
        """
        target = phase.base.generation + 1
        new_uuid = uuid4()
        self._check_format_unmigrated(phase.work, phase.base)
        if not self._publish_generation_resolved(phase.work, target, new_uuid, phase.attempt):
            next_attempt = phase.attempt + 1
            _backoff(next_attempt)
            return _Attempt(base=phase.base, work=phase.work, attempt=next_attempt)
        new_doc = phase.base.model_copy(
            update={
                "generation": target,
                "catalog_uuid": new_uuid,
                "created_at": datetime.now(tz=UTC),
                "writer": _writer_info(),
            }
        )

        result = self._try_create_marker(new_doc, new_uuid, phase.attempt)
        if result is not None:
            return _Committed(result=result)

        attempt = phase.attempt + 1
        decision = decide_rebase(changeset, self._conflict_policy, attempt, self._max_attempts)
        match decision:
            case Abort():
                return _Aborted(reason=decision.reason)
            case Replay():
                _backoff(attempt)
                GenerationCache.discard(phase.work)
                base, work = self._replay_onto_head(changeset)
                return _Attempt(base=base, work=work, attempt=attempt)

    def _create_marker_resolving(
        self, doc: RootDoc, doc_uuid: UUID, base_attempt: int
    ) -> MarkerOutcome:
        """Create `doc`'s marker, resolving ambiguity by GET; retry on ABSENT.

        A marker create is create-only, so a 412/409 and an ambiguous outcome
        both resolve by GETting the marker: our uuid means WON (permanent — the
        marker is immutable and immortal), anyone else's means LOST, absent
        means our create never landed. ABSENT retries the SAME doc (safe: the
        key is never deleted, so re-creating is idempotent against a still-in-
        flight twin), bounded by max_attempts. Returns WON or LOST, never
        ABSENT — an ABSENT that never resolves raises ExternalServiceError.
        """
        target = doc.generation
        local_attempt = 0
        while True:
            try:
                create_marker(self._store, doc)
            except (PreconditionFailedError, ConditionalConflictError, AmbiguousCasError):
                outcome = resolve_marker(self._store, target, doc_uuid)
            else:
                return MarkerOutcome.WON
            match outcome:
                case MarkerOutcome.WON | MarkerOutcome.LOST:
                    return outcome  # WON (our write echoed back / landed) or LOST
                case MarkerOutcome.ABSENT:
                    # Our create genuinely didn't land — retry the SAME doc.
                    local_attempt += 1
                    if base_attempt + local_attempt >= self._max_attempts:
                        raise ExternalServiceError(
                            f"marker create for generation {target} kept failing "
                            f"across {self._max_attempts} attempts"
                        )
                    _backoff(base_attempt + local_attempt)

    def _try_create_marker(
        self, new_doc: RootDoc, new_uuid: UUID, attempt: int
    ) -> CommitResult | None:
        """Attempt the marker create; return a CommitResult on WIN, else None.

        None means the generation was lost (a rival won it) — the caller
        rebases onto the resolved head and tries the next generation.
        """
        match self._create_marker_resolving(new_doc, new_uuid, attempt):
            case MarkerOutcome.WON:
                write_hint(self._store, new_doc.generation)
                return CommitResult(
                    generation=new_doc.generation, catalog_uuid=new_uuid, attempts=attempt + 1
                )
            case MarkerOutcome.LOST:
                return None
            case MarkerOutcome.ABSENT:  # invariant: _create_marker_resolving never returns ABSENT
                raise AssertionError("_create_marker_resolving returned ABSENT")

    def _replay_onto_head(self, changeset: Changeset) -> tuple[RootDoc, Path]:
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
                return base, self._cache.fetch_copy(base.generation, base.catalog_uuid)
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
    def reader(self) -> Generator[LakeConnection]:
        """Attach the current generation READ_ONLY (frozen-DuckLake pattern)."""
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

    def _check_versions(self, root: RootDoc) -> None:
        """Refuse to write when local versions differ from the lake's pins.

        A newer ducklake extension would silently migrate the catalog format
        for the whole fleet on ATTACH; upgrades must be explicit.
        """
        if root.duckdb_storage_version != DUCKDB_VERSION:
            raise VersionMismatchError(
                f"lake pins duckdb {root.duckdb_storage_version}, "
                f"local is {DUCKDB_VERSION}; upgrade the lake explicitly"
            )
