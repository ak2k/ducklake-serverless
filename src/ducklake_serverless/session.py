"""The transaction envelope: the library's public API.

A `Lake` wraps an `ObjectStore` and a local working directory. Writers use
`lake.transaction()` — SQL runs against a local copy of the current
catalog generation via the stock ducklake extension, is recorded as a
logical changeset, then committed by publishing the new generation and
CASing the root. On a lost race, `decide_rebase` chooses between replaying
the changeset onto the winner's generation and aborting to the caller.
Readers use `lake.reader()` — the frozen-DuckLake pattern.
"""

from __future__ import annotations

import os
import random
import socket
import time
from contextlib import contextmanager
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
    RootDoc,
    Statement,
    WriterInfo,
    format_catalog_key,
)
from ducklake_serverless.rebase import decide_rebase
from ducklake_serverless.recorder import record
from ducklake_serverless.root import (
    CasOutcome,
    bootstrap_root,
    publish_root,
    read_root,
    resolve_cas,
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

    def bootstrap(self) -> RootDoc:
        """Create generation 0 (an empty DuckLake catalog) and the root.

        Create-only end to end: loses cleanly to any concurrent bootstrap.
        """
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
            bootstrap_root(self._store, doc)
        finally:
            GenerationCache.discard(catalog_path)
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
        base, etag, work = self._fetch_current_base()
        with self._writable_copy(work) as connection:
            transaction = Transaction(connection)
            yield transaction

        try:
            self._commit(work, base, etag, transaction.changeset)
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

    def _commit(self, work: Path, base: RootDoc, etag: str, changeset: Changeset) -> CommitResult:
        """Publish + CAS, replaying onto newer generations until won or aborted."""
        attempt = 0
        while True:
            new_uuid = uuid4()
            self._check_format_unmigrated(work, base)
            if not self._publish_generation_resolved(work, base.generation + 1, new_uuid, attempt):
                attempt += 1
                _backoff(attempt)
                continue
            new_doc = base.model_copy(
                update={
                    "generation": base.generation + 1,
                    "catalog_uuid": new_uuid,
                    "created_at": datetime.now(tz=UTC),
                    "writer": _writer_info(),
                }
            )
            try:
                publish_root(self._store, new_doc, etag)
                return CommitResult(
                    generation=new_doc.generation,
                    catalog_uuid=new_uuid,
                    attempts=attempt + 1,
                )
            except (PreconditionFailedError, ConditionalConflictError):
                # A 412/409 can be our OWN successful write echoed back: an
                # SDK-level transport retry of a conditional PUT that landed
                # 412s against itself. Resolve by commit token before
                # concluding someone else won — otherwise this would replay
                # an already-committed changeset (double-apply).
                outcome, current, current_etag = resolve_cas(self._store, new_uuid)
                if outcome is CasOutcome.WON:
                    return CommitResult(
                        generation=current.generation,
                        catalog_uuid=new_uuid,
                        attempts=attempt + 1,
                    )
                base, etag = current, current_etag
            except AmbiguousCasError:
                outcome, current, current_etag = resolve_cas(self._store, new_uuid)
                if outcome is CasOutcome.WON:
                    return CommitResult(
                        generation=current.generation,
                        catalog_uuid=new_uuid,
                        attempts=attempt + 1,
                    )
                # LOST after an ambiguous outcome is unresolvable: our write
                # may have landed and been built upon before this re-read —
                # any successor destroys the uuid evidence. Replaying could
                # double-apply; the only safe exit is abort.
                raise ConflictAbortError(
                    "commit outcome ambiguous and the root has moved on — the "
                    "write may or may not be committed. Verify lake state "
                    "before retrying this transaction."
                ) from None

            while True:
                attempt += 1
                decision = decide_rebase(
                    changeset, self._conflict_policy, attempt, self._max_attempts
                )
                if isinstance(decision, Abort):
                    raise ConflictAbortError(decision.reason)
                _backoff(attempt)
                try:
                    replayed = self._replay(base, changeset)
                except ObjectNotFoundError:
                    # GC swept the base between our root read and the fetch —
                    # the root has necessarily advanced; rebase onto current.
                    base, etag = read_root(self._store)
                else:
                    GenerationCache.discard(work)
                    work = replayed
                    break

    def _replay(self, winner: RootDoc, changeset: Changeset) -> Path:
        """Re-execute the changeset against a fresh copy of the winner's catalog."""
        self._check_versions(winner)
        work = self._cache.fetch_copy(winner.generation, winner.catalog_uuid)
        with self._writable_copy(work) as connection:
            for statement in changeset.statements:
                connection.execute(statement.sql, statement.params)
        return work

    def _fetch_current_base(self) -> tuple[RootDoc, str, Path]:
        """Resolve the current root and fetch its catalog, GC-race-safe.

        Between reading the root and fetching its catalog, GC may sweep
        that generation (the root advanced past retention meanwhile). No
        user SQL has run yet, so re-reading and retrying is always correct.
        """
        for _ in range(self._max_attempts):
            base, etag = read_root(self._store)
            self._check_versions(base)
            try:
                return base, etag, self._cache.fetch_copy(base.generation, base.catalog_uuid)
            except ObjectNotFoundError:
                continue
        raise ExternalServiceError(
            f"catalog for the current root kept vanishing across "
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
        _, _, work = self._fetch_current_base()
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
        _, _, path = self._fetch_current_base()
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
