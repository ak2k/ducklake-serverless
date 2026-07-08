"""Generation-marker protocol over the in-memory fake: the serialization contract.

Invariants under test: per-generation exclusivity, marker density, and that
head resolution + ambiguity resolution are exact and permanent regardless of
hint state.
"""

from __future__ import annotations

import pytest

from ducklake_serverless.errors import (
    ExternalServiceError,
    LakeNotInitializedError,
    ObjectNotFoundError,
    PreconditionFailedError,
)
from ducklake_serverless.models import HintDoc, RootDoc, format_marker_key
from ducklake_serverless.objectstore import InMemoryObjectStore
from ducklake_serverless.root import (
    ROOT_HINT_KEY,
    MarkerOutcome,
    create_marker,
    read_marker,
    resolve_head,
    resolve_marker,
    write_hint,
)
from tests.test_models import make_root


def commit_chain(store: InMemoryObjectStore, through: int) -> list[RootDoc]:
    """Create dense markers 0..through, advancing the hint like real commits."""
    docs: list[RootDoc] = []
    for gen in range(through + 1):
        doc = make_root(generation=gen)
        create_marker(store, doc)
        write_hint(store, gen)
        docs.append(doc)
    return docs


# ── create + read ──


def test_create_then_read_marker() -> None:
    store = InMemoryObjectStore()
    doc = make_root(generation=0)
    create_marker(store, doc)
    assert read_marker(store, 0) == doc


def test_create_is_per_generation_exclusive() -> None:
    store = InMemoryObjectStore()
    create_marker(store, make_root(generation=0))
    with pytest.raises(PreconditionFailedError):
        create_marker(store, make_root(generation=0))  # same generation, different uuid


def test_read_missing_marker() -> None:
    with pytest.raises(ObjectNotFoundError):
        read_marker(InMemoryObjectStore(), 5)


def test_malformed_marker_wrapped() -> None:
    store = InMemoryObjectStore()
    store.put_if_absent(format_marker_key(0), b'{"schema": "wrong/9"}')
    with pytest.raises(ExternalServiceError):
        read_marker(store, 0)


def test_marker_body_generation_must_match_key() -> None:
    store = InMemoryObjectStore()
    # A body claiming a different generation than its key is corruption.
    store.put_if_absent(format_marker_key(3), make_root(generation=7).to_json_bytes())
    with pytest.raises(ExternalServiceError, match="claims generation 7"):
        read_marker(store, 3)


# ── ambiguity resolution: exact and permanent ──


def test_resolve_marker_won() -> None:
    store = InMemoryObjectStore()
    ours = make_root(generation=0)
    create_marker(store, ours)  # landed; pretend we never saw the 200
    assert resolve_marker(store, 0, ours.catalog_uuid) is MarkerOutcome.WON


def test_resolve_marker_lost() -> None:
    store = InMemoryObjectStore()
    theirs = make_root(generation=0)
    create_marker(store, theirs)
    ours = make_root(generation=0)
    assert resolve_marker(store, 0, ours.catalog_uuid) is MarkerOutcome.LOST


def test_resolve_marker_absent() -> None:
    store = InMemoryObjectStore()
    assert resolve_marker(store, 0, make_root().catalog_uuid) is MarkerOutcome.ABSENT


def test_won_is_permanent_across_successors() -> None:
    """The v2 fix: our marker's uuid answers WON no matter how far head advances."""
    store = InMemoryObjectStore()
    ours = make_root(generation=0)
    create_marker(store, ours)
    for gen in range(1, 20):  # extend head far past our gen 0
        create_marker(store, make_root(generation=gen))
    # Head is now 19; our gen-0 evidence is untouched and still definitive.
    assert resolve_marker(store, 0, ours.catalog_uuid) is MarkerOutcome.WON


def test_exactly_one_winner_per_generation() -> None:
    store = InMemoryObjectStore()
    contenders = [make_root(generation=0) for _ in range(10)]
    winners = 0
    for doc in contenders:
        try:
            create_marker(store, doc)
            winners += 1
        except PreconditionFailedError:
            pass
    assert winners == 1


# ── head resolution across hint states ──


def test_resolve_head_via_fresh_hint() -> None:
    store = InMemoryObjectStore()
    docs = commit_chain(store, through=5)
    doc, gen = resolve_head(store)
    assert gen == 5
    assert doc == docs[5]


def test_resolve_head_forward_probes_past_stale_hint() -> None:
    """Hint lags (a slow writer's late PUT): probe forward, never serve stale."""
    store = InMemoryObjectStore()
    docs = commit_chain(store, through=8)
    store.put(ROOT_HINT_KEY, HintDoc(generation=2).to_json_bytes())  # regress the hint
    doc, gen = resolve_head(store)
    assert gen == 8
    assert doc == docs[8]


def test_resolve_head_missing_hint_discovers() -> None:
    store = InMemoryObjectStore()
    docs = commit_chain(store, through=6)
    store.delete(ROOT_HINT_KEY)
    doc, gen = resolve_head(store)
    assert gen == 6
    assert doc == docs[6]


def test_resolve_head_corrupt_hint_discovers() -> None:
    store = InMemoryObjectStore()
    commit_chain(store, through=4)
    store.put(ROOT_HINT_KEY, b"not json")
    _, gen = resolve_head(store)
    assert gen == 4


def test_resolve_head_poison_high_hint_rediscovers() -> None:
    """A hint pointing at a generation with no marker must never be trusted."""
    store = InMemoryObjectStore()
    commit_chain(store, through=3)
    store.put(ROOT_HINT_KEY, HintDoc(generation=99).to_json_bytes())
    _, gen = resolve_head(store)
    assert gen == 3  # rediscovered, not believed


def test_resolve_head_uninitialized_lake() -> None:
    store = InMemoryObjectStore()
    with pytest.raises(LakeNotInitializedError):
        resolve_head(store)


def test_gallop_discovery_over_long_chain() -> None:
    """No hint, deep history: galloping still finds the true head."""
    store = InMemoryObjectStore()
    for gen in range(200):
        create_marker(store, make_root(generation=gen))  # no hint written
    _, gen = resolve_head(store)
    assert gen == 199


def test_resolve_head_stops_at_max_generation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Head resolution must terminate at MAX_GENERATION, never probe past it.

    generation MAX+1 is unformattable (format_marker_key raises
    InputValidationError), which only ObjectNotFoundError is caught around — so
    without the boundary guard, a head sitting at MAX would crash both the
    forward-probe and the gallop-discovery paths. With MAX patched small, this
    exercises both guards on a lake whose head IS the max generation.
    """
    monkeypatch.setattr("ducklake_serverless.root.MAX_GENERATION", 3)
    monkeypatch.setattr("ducklake_serverless.models.MAX_GENERATION", 3)
    store = InMemoryObjectStore()
    docs = commit_chain(store, through=3)  # head == MAX_GENERATION

    doc, gen = resolve_head(store)  # forward-probe path (valid hint at head)
    assert gen == 3
    assert doc == docs[3]

    store.delete(ROOT_HINT_KEY)  # gallop-discovery path (no hint)
    doc, gen = resolve_head(store)
    assert gen == 3
    assert doc == docs[3]


def test_resolve_head_probe_cap_returns_valid_recent_head(monkeypatch: pytest.MonkeyPatch) -> None:
    """When the forward probe hits PROBE_CAP it returns the most recent head it

    reached — always a real, committed generation, never an error or a loop. A
    recent head is valid; nobody needs the exact instantaneous frontier.
    """
    monkeypatch.setattr("ducklake_serverless.root.PROBE_CAP", 3)
    store = InMemoryObjectStore()
    commit_chain(store, through=20)
    store.put(ROOT_HINT_KEY, HintDoc(generation=0).to_json_bytes())  # force a long forward probe

    doc, gen = resolve_head(store)
    # Only 3 galloping probes fit under the cap, so it stops strictly BELOW the
    # true head (20) — proving the cap bounded the walk — yet still returns a
    # real, committed generation it actually reached.
    assert 0 < gen < 20
    assert read_marker(store, gen) == doc
