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
from uuid import uuid4

from ducklake_serverless import __version__
from ducklake_serverless.engine import (
    DUCKDB_VERSION,
    LakeConnection,
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
    ) -> None:
        """Bind the lake to a store, a scratch dir, and a Parquet destination.

        `data_path` is where DuckDB writes Parquet — an s3:// URL in
        production, a local directory in hermetic tests. No trailing slash
        (upstream ducklake#815 mis-classifies files under one as orphans).
        """
        self._store = store
        self._workdir = workdir
        self._data_path = data_path.rstrip("/")
        self._cache = GenerationCache(store, workdir)
        self._conflict_policy = conflict_policy
        self._max_attempts = max_attempts

    def bootstrap(self) -> RootDoc:
        """Create generation 0 (an empty DuckLake catalog) and the root.

        Create-only end to end: loses cleanly to any concurrent bootstrap.
        """
        catalog_uuid = uuid4()
        catalog_path = self._workdir / f"bootstrap-{catalog_uuid}.duckdb"
        connection = LakeConnection(catalog_path, self._data_path)
        connection.close()

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
        return doc

    @contextmanager
    def transaction(self) -> Generator[Transaction]:
        """Run SQL against the lake and commit it as one new generation.

        On a lost CAS race the recorded changeset is either replayed onto
        the winner's generation (blind appends, or replay_all policy) or
        the whole call raises ConflictAbortError. VersionMismatchError if
        local versions differ from the lake's pins.
        """
        base, etag, work = self._fetch_current_base()
        connection = LakeConnection(work, self._data_path)
        transaction = Transaction(connection)
        try:
            yield transaction
        except BaseException:
            connection.abandon()
            raise
        connection.close()

        self._commit(work, base, etag, transaction.changeset)

    def _commit(self, work: Path, base: RootDoc, etag: str, changeset: Changeset) -> CommitResult:
        """Publish + CAS, replaying onto newer generations until won or aborted."""
        attempt = 0
        while True:
            new_uuid = uuid4()
            publish_generation(self._store, work, base.generation + 1, new_uuid)
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
                base, etag = read_root(self._store)
            except AmbiguousCasError:
                outcome, current, current_etag = resolve_cas(self._store, new_uuid)
                if outcome is CasOutcome.WON:
                    return CommitResult(
                        generation=current.generation,
                        catalog_uuid=new_uuid,
                        attempts=attempt + 1,
                    )
                base, etag = current, current_etag

            while True:
                attempt += 1
                decision = decide_rebase(
                    changeset, self._conflict_policy, attempt, self._max_attempts
                )
                if isinstance(decision, Abort):
                    raise ConflictAbortError(decision.reason)
                _backoff(attempt)
                try:
                    work = self._replay(base, changeset)
                    break
                except ObjectNotFoundError:
                    # GC swept the base between our root read and the fetch —
                    # the root has necessarily advanced; rebase onto current.
                    base, etag = read_root(self._store)

    def _replay(self, winner: RootDoc, changeset: Changeset) -> Path:
        """Re-execute the changeset against a fresh copy of the winner's catalog."""
        self._check_versions(winner)
        work = self._cache.fetch_copy(winner.generation, winner.catalog_uuid)
        connection = LakeConnection(work, self._data_path)
        try:
            for statement in changeset.statements:
                connection.execute(statement.sql, statement.params)
        except BaseException:
            connection.abandon()
            raise
        connection.close()
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
    def reader(self) -> Generator[LakeConnection]:
        """Attach the current generation READ_ONLY (frozen-DuckLake pattern)."""
        _, _, path = self._fetch_current_base()
        connection = LakeConnection(path, data_path=None, read_only=True)
        try:
            yield connection
        finally:
            connection.abandon()

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
