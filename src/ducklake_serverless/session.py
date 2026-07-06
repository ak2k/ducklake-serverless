"""The transaction envelope: the library's public API.

A `Lake` wraps an `ObjectStore` and a local working directory. Writers use
`lake.transaction()` — fetch the current generation, run SQL against a
local copy via the stock ducklake extension, then publish catalog + CAS
the root. Readers use `lake.reader()`, which is just the frozen-DuckLake
pattern: resolve the root, attach that generation READ_ONLY.

P1 scope: single-writer happy path. A lost CAS raises ConflictAbortError;
the rebase/replay loop lands in P2.
"""

from __future__ import annotations

import os
import socket
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
    ConditionalConflictError,
    ConflictAbortError,
    PreconditionFailedError,
    VersionMismatchError,
)
from ducklake_serverless.generation import GenerationCache, publish_generation
from ducklake_serverless.models import RootDoc, WriterInfo
from ducklake_serverless.root import bootstrap_root, publish_root, read_root

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from ducklake_serverless.objectstore import ObjectStore


def _writer_info() -> WriterInfo:
    return WriterInfo(lib_version=__version__, host=socket.gethostname(), pid=os.getpid())


class Transaction:
    """One open transaction: SQL runs against a private catalog copy."""

    def __init__(self, connection: LakeConnection) -> None:
        self._connection = connection

    def sql(self, statement: str, params: tuple[object, ...] = ()) -> list[tuple[object, ...]]:
        """Execute one statement inside the transaction."""
        return self._connection.execute(statement, params)


class Lake:
    """A serverless DuckLake rooted at one object-store prefix."""

    def __init__(self, store: ObjectStore, workdir: Path, data_path: str) -> None:
        """Bind the lake to a store, a scratch dir, and a Parquet destination.

        `data_path` is where DuckDB writes Parquet — an s3:// URL in
        production, a local directory in hermetic tests. No trailing slash
        (upstream ducklake#815 mis-classifies files under one as orphans).
        """
        self._store = store
        self._workdir = workdir
        self._data_path = data_path.rstrip("/")
        self._cache = GenerationCache(store, workdir)

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

        Raises ConflictAbortError if another writer commits first (P1: no
        rebase yet), VersionMismatchError if local DuckDB/DuckLake versions
        differ from the lake's, and AmbiguousCasError only after resolution
        failed (P2 wires resolve_cas into the retry loop).
        """
        base, etag = read_root(self._store)
        self._check_versions(base)

        work = self._cache.fetch_copy(base.generation, base.catalog_uuid)
        connection = LakeConnection(work, self._data_path)
        try:
            yield Transaction(connection)
        except BaseException:
            connection.abandon()
            raise
        connection.close()

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
        except (PreconditionFailedError, ConditionalConflictError) as exc:
            # P2 replaces this with the rebase/replay loop.
            raise ConflictAbortError(
                "another writer committed first; re-read and retry the transaction"
            ) from exc

    @contextmanager
    def reader(self) -> Generator[LakeConnection]:
        """Attach the current generation READ_ONLY (frozen-DuckLake pattern)."""
        doc, _ = read_root(self._store)
        path = self._cache.fetch_copy(doc.generation, doc.catalog_uuid)
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
