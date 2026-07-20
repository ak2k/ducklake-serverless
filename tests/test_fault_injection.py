"""Fault-injection coverage for the marker create + resolution paths.

The plain in-memory fake can only produce clean 412s, so these paths — the
ones that guard double-apply — need a wrapping store that lies about
outcomes the way a real network does: the marker create lands but the
caller sees a transport error, or a rival slips in first.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, override

import pytest

from ducklake_serverless.errors import (
    AmbiguousCasError,
    VersionMismatchError,
)
from ducklake_serverless.models import ROOTS_PREFIX
from ducklake_serverless.objectstore import GetResult, InMemoryObjectStore, ObjectMeta
from ducklake_serverless.root import resolve_head
from ducklake_serverless.session import Lake

if TYPE_CHECKING:
    from pathlib import Path


class AmbiguousMarkerStore:
    """Wraps the fake: the Nth marker create LANDS but reports AmbiguousCasError.

    Models the real S3 failure this protocol exists to survive — a
    successful create-only PUT whose response is lost in transit.
    """

    def __init__(self, inner: InMemoryObjectStore, fail_on_call: int = 1) -> None:
        self._inner = inner
        self._marker_puts = 0
        self._fail_on = fail_on_call

    def get(self, key: str) -> GetResult:
        """Delegate."""
        return self._inner.get(key)

    def put_if_absent(self, key: str, body: bytes) -> str:
        """Land marker creates, then lie about the outcome on the chosen one."""
        if key.startswith(ROOTS_PREFIX):
            self._marker_puts += 1
            if self._marker_puts == self._fail_on:
                self._inner.put_if_absent(key, body)  # marker LANDS
                raise AmbiguousCasError(f"{key}: outcome unknown (injected)")
        return self._inner.put_if_absent(key, body)

    def put_if_match(self, key: str, body: bytes, etag: str) -> str:
        """Delegate."""
        return self._inner.put_if_match(key, body, etag)

    def put(self, key: str, body: bytes) -> str:
        """Delegate."""
        return self._inner.put(key, body)

    def list_prefix(self, prefix: str) -> list[str]:
        """Delegate."""
        return self._inner.list_prefix(prefix)

    def list_meta(self, prefix: str) -> list[ObjectMeta]:
        """Delegate."""
        return self._inner.list_meta(prefix)

    def head_meta(self, key: str) -> ObjectMeta:
        """Delegate."""
        return self._inner.head_meta(key)

    def delete(self, key: str) -> None:
        """Delegate."""
        self._inner.delete(key)


class AmbiguousAbsentThenLands(AmbiguousMarkerStore):
    """The chosen marker create raises AmbiguousCasError WITHOUT landing; the

    retry of the SAME key lands. Models the case the base store does not: a
    create that genuinely did not land (resolve_marker → ABSENT), so the
    protocol must re-issue the identical doc rather than adopt or abort.
    """

    @override
    def put_if_absent(self, key: str, body: bytes) -> str:
        """Raise ambiguous WITHOUT landing on the chosen call; land on retry."""
        if key.startswith(ROOTS_PREFIX):
            self._marker_puts += 1
            if self._marker_puts == self._fail_on:
                raise AmbiguousCasError(f"{key}: outcome unknown, did not land (injected)")
        return self._inner.put_if_absent(key, body)


@pytest.fixture
def inner() -> InMemoryObjectStore:
    return InMemoryObjectStore()


def make_lake(store: object, tmp_path: Path, name: str) -> Lake:
    data = tmp_path / "data"
    data.mkdir(exist_ok=True)
    work = tmp_path / name
    work.mkdir()
    return Lake(store, workdir=work, data_path=str(data))  # pyright: ignore[reportArgumentType]


def test_ambiguous_create_that_landed_resolves_won_exactly_once(
    inner: InMemoryObjectStore, tmp_path: Path
) -> None:
    """The marker create lands, the response is lost: resolve_marker answers

    WON through _commit end-to-end — exactly once, no duplicate generation.
    """
    setup = make_lake(inner, tmp_path, "setup")
    setup.bootstrap()
    with setup.transaction() as tx:
        tx.sql("CREATE TABLE t (v INTEGER)")

    wrapped = AmbiguousMarkerStore(inner)
    writer = make_lake(wrapped, tmp_path, "writer")
    with writer.transaction() as tx:
        tx.sql("INSERT INTO t VALUES (42)")

    _, gen = resolve_head(inner)
    assert gen == 2  # bootstrap(0) + CREATE(1) + exactly ONE insert commit(2)

    verify = make_lake(inner, tmp_path, "verify")
    with verify.reader() as con:
        assert con.execute("SELECT count(*), sum(v) FROM t") == [(1, 42)]  # exactly once


def test_ambiguous_then_overtaken_resolves_won_not_aborts(
    inner: InMemoryObjectStore, tmp_path: Path
) -> None:
    """THE v2 fix (inverts v1's abort): our marker create lands, a rival

    commits ON TOP before we resolve. The marker at our generation is
    immutable and immortal, so resolution still answers WON — the commit
    succeeds exactly once, no client reconciliation.
    """
    setup = make_lake(inner, tmp_path, "setup")
    setup.bootstrap()
    with setup.transaction() as tx:
        tx.sql("CREATE TABLE t (v INTEGER)")

    class AmbiguousThenOvertaken(AmbiguousMarkerStore):
        @override
        def put_if_absent(self, key: str, body: bytes) -> str:
            if key.startswith(ROOTS_PREFIX):
                self._marker_puts += 1
                if self._marker_puts == self._fail_on:
                    self._inner.put_if_absent(key, body)  # our marker LANDS
                    # Rival commits the NEXT generation on top before we resolve.
                    rival = make_lake(self._inner, tmp_path, "rival")
                    with rival.transaction() as tx:
                        tx.sql("INSERT INTO t VALUES (99)")
                    raise AmbiguousCasError(f"{key}: outcome unknown (injected)")
            return self._inner.put_if_absent(key, body)

    wrapped = AmbiguousThenOvertaken(inner)
    writer = make_lake(wrapped, tmp_path, "writer")
    with writer.transaction() as tx:  # NO abort — commit succeeds
        tx.sql("INSERT INTO t VALUES (42)")

    verify = make_lake(inner, tmp_path, "verify")
    with verify.reader() as con:
        # ours exactly once (marker landed), rival's once — sum forces real scan.
        assert con.execute("SELECT count(*), sum(v) FROM t") == [(2, 141)]


def test_ambiguous_lost_rebases(inner: InMemoryObjectStore, tmp_path: Path) -> None:
    """Ambiguous outcome where our create genuinely LOST to a rival at the

    same generation: v1 hard-aborted; v2 rebases and commits.
    """
    setup = make_lake(inner, tmp_path, "setup")
    setup.bootstrap()
    with setup.transaction() as tx:
        tx.sql("CREATE TABLE t (v INTEGER)")

    class RivalWinsThenAmbiguous(AmbiguousMarkerStore):
        @override
        def put_if_absent(self, key: str, body: bytes) -> str:
            if key.startswith(ROOTS_PREFIX):
                self._marker_puts += 1
                if self._marker_puts == self._fail_on:
                    # A rival takes THIS generation first, then our create
                    # fails ambiguously — resolution finds the rival's uuid.
                    rival = make_lake(self._inner, tmp_path, "rival")
                    with rival.transaction() as tx:
                        tx.sql("INSERT INTO t VALUES (99)")
                    raise AmbiguousCasError(f"{key}: outcome unknown (injected)")
            return self._inner.put_if_absent(key, body)

    wrapped = RivalWinsThenAmbiguous(inner)
    writer = make_lake(wrapped, tmp_path, "writer")
    with writer.transaction() as tx:
        tx.sql("INSERT INTO t VALUES (42)")

    verify = make_lake(inner, tmp_path, "verify")
    with verify.reader() as con:
        assert con.execute("SELECT count(*), sum(v) FROM t") == [(2, 141)]  # both, once


def test_replay_onto_version_mismatched_head_refused(
    inner: InMemoryObjectStore, tmp_path: Path
) -> None:
    """A rival wins AND its marker pins a different duckdb version: the rebase

    onto head must refuse rather than silently migrate the winner's catalog.
    """
    setup = make_lake(inner, tmp_path, "setup")
    setup.bootstrap()
    with setup.transaction() as tx:
        tx.sql("CREATE TABLE t (v INTEGER)")

    class RivalWinsWithForeignVersion(AmbiguousMarkerStore):
        @override
        def put_if_absent(self, key: str, body: bytes) -> str:
            if key.startswith(ROOTS_PREFIX):
                self._marker_puts += 1
                if self._marker_puts == self._fail_on:
                    # Rival commits first, then a foreign-version marker is
                    # planted at the NEXT generation as the poisoned head.
                    rival = make_lake(self._inner, tmp_path, "rival")
                    with rival.transaction() as tx:
                        tx.sql("INSERT INTO t VALUES (99)")
                    head, _ = resolve_head(self._inner)
                    foreign = head.model_copy(
                        update={
                            "generation": head.generation + 1,
                            "pins": {**head.pins, "duckdb_storage_version": "v0.0.1-foreign"},
                        }
                    )
                    self._inner.put_if_absent(foreign.marker_key, foreign.to_json_bytes())
            return self._inner.put_if_absent(key, body)

    wrapped = RivalWinsWithForeignVersion(inner)
    writer = make_lake(wrapped, tmp_path, "writer")
    with pytest.raises(VersionMismatchError), writer.transaction() as tx:
        tx.sql("INSERT INTO t VALUES (42)")


def test_multi_statement_changeset_replays_in_order_with_params(
    inner: InMemoryObjectStore, tmp_path: Path
) -> None:
    """Replay must re-execute ALL statements in order with their params."""
    setup = make_lake(inner, tmp_path, "setup")
    setup.bootstrap()
    with setup.transaction() as tx:
        tx.sql("CREATE TABLE log (seq INTEGER, msg VARCHAR)")

    class LoseFirstCreate(AmbiguousMarkerStore):
        @override
        def put_if_absent(self, key: str, body: bytes) -> str:
            if key.startswith(ROOTS_PREFIX):
                self._marker_puts += 1
                if self._marker_puts == self._fail_on:
                    # A rival slips in first; our create genuinely 412s and the
                    # multi-statement changeset must replay onto head.
                    rival = make_lake(self._inner, tmp_path, "rival")
                    with rival.transaction() as tx:
                        tx.sql("INSERT INTO log VALUES (0, 'rival')")
            return self._inner.put_if_absent(key, body)

    wrapped = LoseFirstCreate(inner)
    writer = make_lake(wrapped, tmp_path, "writer")
    with writer.transaction() as tx:
        tx.sql("INSERT INTO log VALUES (?, ?)", (1, "first"))
        tx.sql("INSERT INTO log VALUES (?, ?)", (2, "second"))
        tx.sql("INSERT INTO log VALUES (?, ?)", (3, "third"))

    verify = make_lake(inner, tmp_path, "verify")
    with verify.reader() as con:
        rows = con.execute("SELECT seq, msg FROM log ORDER BY seq")
        assert rows == [(0, "rival"), (1, "first"), (2, "second"), (3, "third")]


def test_bootstrap_ambiguous_absent_retries_and_initializes_once(
    inner: InMemoryObjectStore, tmp_path: Path
) -> None:
    """bootstrap()'s gen-0 create fails ambiguously WITHOUT landing: the ABSENT

    resolution re-issues the SAME doc and the lake initializes exactly once,
    instead of the old handler raising ObjectNotFoundError on read_marker(0).
    """
    wrapped = AmbiguousAbsentThenLands(inner)  # first ROOTS put (gen 0) doesn't land
    lake = make_lake(wrapped, tmp_path, "boot")
    lake.bootstrap(verify_backend=False)  # single-writer: focus on the retry path

    _, gen = resolve_head(inner)
    assert gen == 0  # created on retry, exactly once
    with lake.transaction() as tx:  # the lake is usable
        tx.sql("CREATE TABLE t (v INTEGER)")
    with make_lake(inner, tmp_path, "reader").reader() as con:
        assert con.execute("SELECT count(*) FROM t") == [(0,)]


def test_commit_ambiguous_absent_retries_and_commits_once(
    inner: InMemoryObjectStore, tmp_path: Path
) -> None:
    """A commit's marker create fails ambiguously WITHOUT landing: the shared

    ABSENT-retry path re-issues the same doc so the row lands exactly once.
    """
    setup = make_lake(inner, tmp_path, "setup")
    setup.bootstrap()
    with setup.transaction() as tx:
        tx.sql("CREATE TABLE t (v INTEGER)")

    wrapped = AmbiguousAbsentThenLands(inner)  # writer's first marker create
    writer = make_lake(wrapped, tmp_path, "writer")
    with writer.transaction() as tx:
        tx.sql("INSERT INTO t VALUES (7)")

    _, gen = resolve_head(inner)
    assert gen == 2  # bootstrap(0) + CREATE(1) + insert(2), landed on retry
    with make_lake(inner, tmp_path, "verify").reader() as con:
        assert con.execute("SELECT count(*), sum(v) FROM t") == [(1, 7)]
