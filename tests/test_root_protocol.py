"""Root CAS protocol over the in-memory fake: the serialization point's contract."""

from __future__ import annotations

import pytest

from ducklake_serverless.errors import (
    ExternalServiceError,
    ObjectNotFoundError,
    PreconditionFailedError,
)
from ducklake_serverless.objectstore import InMemoryObjectStore
from ducklake_serverless.root import (
    ROOT_KEY,
    CasOutcome,
    bootstrap_root,
    publish_root,
    read_root,
    resolve_cas,
)
from tests.test_models import make_root


def test_bootstrap_then_read() -> None:
    store = InMemoryObjectStore()
    doc = make_root()
    etag = bootstrap_root(store, doc)
    read_doc, read_etag = read_root(store)
    assert read_doc == doc
    assert read_etag == etag


def test_bootstrap_is_create_only() -> None:
    store = InMemoryObjectStore()
    bootstrap_root(store, make_root())
    with pytest.raises(PreconditionFailedError):
        bootstrap_root(store, make_root())


def test_read_missing_root() -> None:
    with pytest.raises(ObjectNotFoundError):
        read_root(InMemoryObjectStore())


def test_read_malformed_root_wrapped() -> None:
    store = InMemoryObjectStore()
    store.put_if_absent(ROOT_KEY, b'{"schema": "wrong/9"}')
    with pytest.raises(ExternalServiceError):
        read_root(store)


def test_publish_advances_generation() -> None:
    store = InMemoryObjectStore()
    g0 = make_root(generation=0)
    etag0 = bootstrap_root(store, g0)
    g1 = make_root(generation=1)
    publish_root(store, g1, etag0)
    doc, _ = read_root(store)
    assert doc == g1


def test_publish_with_stale_etag_fails() -> None:
    store = InMemoryObjectStore()
    etag0 = bootstrap_root(store, make_root(generation=0))
    publish_root(store, make_root(generation=1), etag0)
    with pytest.raises(PreconditionFailedError):
        publish_root(store, make_root(generation=2), etag0)


def test_exactly_one_winner_per_etag() -> None:
    """N writers CAS against the same observed root: exactly one succeeds."""
    store = InMemoryObjectStore()
    etag0 = bootstrap_root(store, make_root(generation=0))
    winners = 0
    contenders = [make_root(generation=1) for _ in range(10)]
    for doc in contenders:
        try:
            publish_root(store, doc, etag0)
            winners += 1
        except PreconditionFailedError:
            pass
    assert winners == 1
    final, _ = read_root(store)
    assert final in contenders


def test_resolve_cas_won() -> None:
    """Ambiguous outcome where our write actually landed: token matches."""
    store = InMemoryObjectStore()
    etag0 = bootstrap_root(store, make_root(generation=0))
    ours = make_root(generation=1)
    publish_root(store, ours, etag0)  # landed, but pretend we never saw the 200
    outcome, doc, _ = resolve_cas(store, ours.catalog_uuid)
    assert outcome is CasOutcome.WON
    assert doc == ours


def test_resolve_cas_lost() -> None:
    store = InMemoryObjectStore()
    etag0 = bootstrap_root(store, make_root(generation=0))
    theirs = make_root(generation=1)
    publish_root(store, theirs, etag0)
    ours = make_root(generation=1)  # never landed
    outcome, doc, etag = resolve_cas(store, ours.catalog_uuid)
    assert outcome is CasOutcome.LOST
    assert doc == theirs
    # The returned etag is current, so the rebase can CAS against it directly.
    publish_root(store, make_root(generation=2), etag)
