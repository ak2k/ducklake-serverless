"""Lease contract: at most one holder; expiry enables takeover."""

from __future__ import annotations

import json
import time

from ducklake_serverless.lease import LEASE_KEY, Lease
from ducklake_serverless.objectstore import InMemoryObjectStore


def test_acquire_fresh() -> None:
    store = InMemoryObjectStore()
    assert Lease(store, "a").acquire()
    body: dict[str, object] = json.loads(store.get(LEASE_KEY).body)  # pyright: ignore[reportAny]
    assert body["holder"] == "a"


def test_second_holder_blocked_while_live() -> None:
    store = InMemoryObjectStore()
    assert Lease(store, "a", ttl_seconds=60).acquire()
    assert not Lease(store, "b", ttl_seconds=60).acquire()


def test_reacquire_own_lease() -> None:
    store = InMemoryObjectStore()
    lease = Lease(store, "a", ttl_seconds=60)
    assert lease.acquire()
    assert lease.acquire()  # idempotent for the same holder


def test_expired_lease_taken_over() -> None:
    store = InMemoryObjectStore()
    assert Lease(store, "dead", ttl_seconds=0).acquire()
    time.sleep(0.01)
    assert Lease(store, "b", ttl_seconds=60).acquire()
    assert json.loads(store.get(LEASE_KEY).body)["holder"] == "b"


def test_corrupt_lease_taken_over() -> None:
    store = InMemoryObjectStore()
    store.put_if_absent(LEASE_KEY, b"not json at all")
    assert Lease(store, "b").acquire()


def test_renew_extends_and_release_frees() -> None:
    store = InMemoryObjectStore()
    lease = Lease(store, "a", ttl_seconds=60)
    assert lease.acquire()
    assert lease.renew()
    lease.release()
    assert Lease(store, "b").acquire()


def test_renew_after_takeover_fails() -> None:
    """A holder that lost its lease to expiry+takeover cannot renew."""
    store = InMemoryObjectStore()
    stale = Lease(store, "stale", ttl_seconds=0)
    assert stale.acquire()
    time.sleep(0.01)
    assert Lease(store, "fresh", ttl_seconds=60).acquire()
    assert not stale.renew()


def test_release_does_not_free_someone_elses_lease() -> None:
    store = InMemoryObjectStore()
    stale = Lease(store, "stale", ttl_seconds=0)
    assert stale.acquire()
    time.sleep(0.01)
    assert Lease(store, "fresh", ttl_seconds=60).acquire()
    stale.release()  # must be a no-op: fresh still holds
    assert not Lease(store, "third", ttl_seconds=60).acquire()


def test_exactly_one_winner_among_contenders() -> None:
    store = InMemoryObjectStore()
    leases = [Lease(store, f"w{i}", ttl_seconds=60) for i in range(10)]
    winners = [lease for lease in leases if lease.acquire()]
    assert len(winners) == 1


def test_release_is_atomic_tombstone_not_delete() -> None:
    """release() must never remove a successor's live lease (the old

    check-then-delete had that race); it tombstones its own lease instead.
    """
    store = InMemoryObjectStore()
    lease = Lease(store, "a", ttl_seconds=60)
    assert lease.acquire()
    lease.release()
    # The key still exists (tombstoned, expired) — and is acquirable.
    body: dict[str, object] = json.loads(store.get(LEASE_KEY).body)  # pyright: ignore[reportAny]
    assert body["expires_at"] == 0.0
    assert Lease(store, "b", ttl_seconds=60).acquire()
