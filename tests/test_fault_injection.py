"""Fault-injection coverage for the ambiguous-CAS and replay paths.

The plain in-memory fake can only produce clean 412s, so these paths —
the ones that guard double-apply — need a wrapping store that lies about
outcomes the way a real network does: the write lands but the caller
sees a transport error.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, override

import pytest

from ducklake_serverless.errors import (
    AmbiguousCasError,
    ConflictAbortError,
    VersionMismatchError,
)
from ducklake_serverless.objectstore import GetResult, InMemoryObjectStore
from ducklake_serverless.root import ROOT_KEY, read_root
from ducklake_serverless.session import Lake

if TYPE_CHECKING:
    from pathlib import Path


class AmbiguousOnceStore:
    """Wraps the fake: the Nth root CAS LANDS but reports AmbiguousCasError.

    Models the real S3 failure this protocol exists to survive — a
    successful conditional PUT whose response is lost in transit.
    """

    def __init__(self, inner: InMemoryObjectStore, fail_on_call: int = 1) -> None:
        self._inner = inner
        self._root_put_calls = 0
        self._fail_on = fail_on_call

    def get(self, key: str) -> GetResult:
        """Delegate."""
        return self._inner.get(key)

    def put_if_absent(self, key: str, body: bytes) -> str:
        """Delegate."""
        return self._inner.put_if_absent(key, body)

    def put_if_match(self, key: str, body: bytes, etag: str) -> str:
        """Land the write, then lie about the outcome on the chosen call."""
        if key == ROOT_KEY:
            self._root_put_calls += 1
            if self._root_put_calls == self._fail_on:
                self._inner.put_if_match(key, body, etag)  # write LANDS
                raise AmbiguousCasError(f"{key}: outcome unknown (injected)")
        return self._inner.put_if_match(key, body, etag)

    def list_prefix(self, prefix: str) -> list[str]:
        """Delegate."""
        return self._inner.list_prefix(prefix)

    def delete(self, key: str) -> None:
        """Delegate."""
        self._inner.delete(key)


@pytest.fixture
def inner() -> InMemoryObjectStore:
    return InMemoryObjectStore()


def make_lake(store: object, tmp_path: Path, name: str) -> Lake:
    data = tmp_path / "data"
    data.mkdir(exist_ok=True)
    work = tmp_path / name
    work.mkdir()
    return Lake(store, workdir=work, data_path=str(data))  # pyright: ignore[reportArgumentType]


def test_ambiguous_cas_that_landed_resolves_won_exactly_once(
    inner: InMemoryObjectStore, tmp_path: Path
) -> None:
    """The write lands, the response is lost: resolve_cas must answer WON

    through _commit end-to-end, committing exactly once — no retry, no
    duplicate generation, no double-apply.
    """
    setup = make_lake(inner, tmp_path, "setup")
    setup.bootstrap()
    with setup.transaction() as tx:
        tx.sql("CREATE TABLE t (v INTEGER)")

    wrapped = AmbiguousOnceStore(inner)
    writer = make_lake(wrapped, tmp_path, "writer")
    with writer.transaction() as tx:
        tx.sql("INSERT INTO t VALUES (42)")

    doc, _ = read_root(inner)
    assert doc.generation == 2  # bootstrap + CREATE + exactly ONE insert commit

    verify = make_lake(inner, tmp_path, "verify")
    with verify.reader() as con:
        assert con.execute("SELECT v FROM t") == [(42,)]  # exactly once


def test_ambiguous_then_overtaken_aborts_not_replays(
    inner: InMemoryObjectStore, tmp_path: Path
) -> None:
    """Our ambiguous write lands, a rival commits on top before we re-read:

    the uuid evidence is destroyed — the only safe outcome is abort, and
    our rows must appear exactly once (from the landed write).
    """
    setup = make_lake(inner, tmp_path, "setup")
    setup.bootstrap()
    with setup.transaction() as tx:
        tx.sql("CREATE TABLE t (v INTEGER)")

    class AmbiguousThenOvertaken(AmbiguousOnceStore):
        @override
        def put_if_match(self, key: str, body: bytes, etag: str) -> str:
            if key == ROOT_KEY:
                self._root_put_calls += 1
                if self._root_put_calls == self._fail_on:
                    self._inner.put_if_match(key, body, etag)  # our write LANDS
                    # Rival commits on top before our resolve re-read.
                    rival = make_lake(self._inner, tmp_path, "rival")
                    with rival.transaction() as tx:
                        tx.sql("INSERT INTO t VALUES (99)")
                    raise AmbiguousCasError(f"{key}: outcome unknown (injected)")
            return self._inner.put_if_match(key, body, etag)

    wrapped = AmbiguousThenOvertaken(inner)
    writer = make_lake(wrapped, tmp_path, "writer")
    with pytest.raises(ConflictAbortError, match="ambiguous"), writer.transaction() as tx:
        tx.sql("INSERT INTO t VALUES (42)")

    verify = make_lake(inner, tmp_path, "verify")
    with verify.reader() as con:
        rows = sorted(int(r[0]) for r in con.execute("SELECT v FROM t"))  # pyright: ignore[reportArgumentType]  # duckdb rows untyped
        assert rows == [42, 99]  # ours exactly once (it landed), rival's once


def test_replay_onto_version_mismatched_winner_refused(
    inner: InMemoryObjectStore, tmp_path: Path
) -> None:
    """A rival wins the race AND pins a different duckdb version: the rebase

    replay must refuse rather than silently migrate the winner's catalog.
    (Mutation-verified gap: deleting _replay's version check passed the
    old suite.)
    """
    setup = make_lake(inner, tmp_path, "setup")
    setup.bootstrap()
    with setup.transaction() as tx:
        tx.sql("CREATE TABLE t (v INTEGER)")

    class RivalWinsWithForeignVersion(AmbiguousOnceStore):
        @override
        def put_if_match(self, key: str, body: bytes, etag: str) -> str:
            if key == ROOT_KEY:
                self._root_put_calls += 1
                if self._root_put_calls == self._fail_on:
                    # Rival commits first — then its root is doctored to pin
                    # a foreign duckdb version, so OUR replay must refuse.
                    rival = make_lake(self._inner, tmp_path, "rival")
                    with rival.transaction() as tx:
                        tx.sql("INSERT INTO t VALUES (99)")
                    doc, cur_etag = read_root(self._inner)
                    foreign = doc.model_copy(update={"duckdb_storage_version": "v0.0.1-foreign"})
                    self._inner.put_if_match(ROOT_KEY, foreign.to_json_bytes(), cur_etag)
            return self._inner.put_if_match(key, body, etag)

    wrapped = RivalWinsWithForeignVersion(inner)
    writer = make_lake(wrapped, tmp_path, "writer")
    with pytest.raises(VersionMismatchError), writer.transaction() as tx:
        tx.sql("INSERT INTO t VALUES (42)")


def test_multi_statement_changeset_replays_in_order_with_params(
    inner: InMemoryObjectStore, tmp_path: Path
) -> None:
    """Replay must re-execute ALL statements in order with their params —

    prior suites only ever replayed single-statement changesets.
    """
    setup = make_lake(inner, tmp_path, "setup")
    setup.bootstrap()
    with setup.transaction() as tx:
        tx.sql("CREATE TABLE log (seq INTEGER, msg VARCHAR)")

    class LoseFirstCas(AmbiguousOnceStore):
        @override
        def put_if_match(self, key: str, body: bytes, etag: str) -> str:
            if key == ROOT_KEY:
                self._root_put_calls += 1
                if self._root_put_calls == self._fail_on:
                    # A rival slips in first; our CAS genuinely loses (412)
                    # and the multi-statement changeset must replay.
                    rival = make_lake(self._inner, tmp_path, "rival")
                    with rival.transaction() as tx:
                        tx.sql("INSERT INTO log VALUES (0, 'rival')")
            return self._inner.put_if_match(key, body, etag)

    wrapped = LoseFirstCas(inner)
    writer = make_lake(wrapped, tmp_path, "writer")
    with writer.transaction() as tx:
        tx.sql("INSERT INTO log VALUES (?, ?)", (1, "first"))
        tx.sql("INSERT INTO log VALUES (?, ?)", (2, "second"))
        tx.sql("INSERT INTO log VALUES (?, ?)", (3, "third"))

    verify = make_lake(inner, tmp_path, "verify")
    with verify.reader() as con:
        rows = con.execute("SELECT seq, msg FROM log ORDER BY seq")
        assert rows == [(0, "rival"), (1, "first"), (2, "second"), (3, "third")]
