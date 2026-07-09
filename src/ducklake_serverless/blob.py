"""BlobStore: versioned, ACID storage of one opaque blob on object storage.

The general-purpose face of the engine. Any bytes — a file, a DuckDB or SQLite
database, an arbitrary artifact — get atomic create-only-CAS versioning, time
travel via retained generations, and serverless single-writer commits, reusing
the exact marker protocol and commit driver that back the DuckLake adapter.
There is no SQL and nothing to merge: a lost commit race aborts, and the caller
re-reads and re-writes. This module imports no duckdb — it exercises the engine
purely through `commit`, `models`, and `root`, which is the whole point.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from ducklake_serverless import commit
from ducklake_serverless.models import Abort, RootDoc, format_catalog_key
from ducklake_serverless.root import MarkerOutcome, read_marker, resolve_head, write_hint

if TYPE_CHECKING:
    from pathlib import Path

    from ducklake_serverless.models import CommitResult, RebaseDecision
    from ducklake_serverless.objectstore import ObjectStore

DEFAULT_MAX_ATTEMPTS = 5


class _BlobCommit:
    """The trivial `commit.CommitContext` for an opaque blob."""

    def validate(self, work: Path, base: RootDoc) -> None:
        """Nothing to check — the bytes are opaque to the engine."""

    def decide_rebase(self, attempt: int, max_attempts: int) -> RebaseDecision:
        """A blob cannot be merged onto a new base, so always abort."""
        return Abort(reason="blob payload lost the commit race; re-read and re-write")

    def replay(self, stale_work: Path) -> tuple[RootDoc, Path]:
        """Unreachable: `decide_rebase` never returns Replay for a blob."""
        raise AssertionError("blob commits never replay")


class BlobStore:
    """A single versioned blob rooted at one object-store prefix."""

    def __init__(
        self, store: ObjectStore, workdir: Path, *, max_attempts: int = DEFAULT_MAX_ATTEMPTS
    ) -> None:
        self._store = store
        self._workdir = workdir
        self._max_attempts = max_attempts

    def bootstrap(self, initial: bytes = b"", *, verify_backend: bool = True) -> RootDoc:
        """Create generation 0 from `initial` bytes and its marker.

        Create-only end to end: loses cleanly to a concurrent bootstrap, and an
        ambiguous marker create resolves by GET. `verify_backend=False` skips
        the atomic-create-only probe for a single-writer store.
        """
        if verify_backend:
            commit.require_atomic_create(self._store)
        blob_uuid = uuid4()
        self._store.put_if_absent(format_catalog_key(0, blob_uuid), initial)
        doc = RootDoc(
            generation=0,
            catalog_uuid=blob_uuid,
            created_at=datetime.now(tz=UTC),
            writer=commit.writer_info(),
        )
        match commit.create_marker_resolving(self._store, doc, blob_uuid, 0, self._max_attempts):
            case MarkerOutcome.WON:
                write_hint(self._store, 0)
                return doc
            case MarkerOutcome.LOST:
                return read_marker(self._store, 0)
            case MarkerOutcome.ABSENT:  # helper retries ABSENT, never returns it
                raise AssertionError("create_marker_resolving returned ABSENT")

    def head(self) -> RootDoc:
        """The current head marker."""
        base, _ = resolve_head(self._store)
        return base

    def read(self) -> bytes:
        """The current generation's bytes."""
        base, _ = resolve_head(self._store)
        return self._store.get(base.catalog_key).body

    def write(self, data: bytes) -> CommitResult:
        """Commit `data` wholesale as the next generation.

        Raises `ConflictAbortError` if another writer wins the race — a blob
        can't be merged, so re-read and re-write.
        """
        base, _ = resolve_head(self._store)
        work = self._workdir / f"blob-{uuid4()}"
        work.write_bytes(data)
        try:
            return commit.run_commit(self._store, base, work, _BlobCommit(), self._max_attempts)
        finally:
            work.unlink(missing_ok=True)
