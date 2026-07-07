"""Lease contract: at most one holder; expiry enables takeover."""

from __future__ import annotations

import json
import time
from unittest import mock

from ducklake_serverless.lease import LEASE_KEY, Lease, Observed, classify_lease
from ducklake_serverless.models import LeaseAcquirable, LeaseHeldByOther, LeaseVacant
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
    assert body["ttl"] == 0.0  # tombstone: zero ttl means instantly expired
    assert Lease(store, "b", ttl_seconds=60).acquire()


def test_expiry_uses_store_clock_not_holder_clock() -> None:
    """A lease from a holder whose clock is far in the FUTURE must still

    expire by the store's clock: skewed holders cannot stretch their leases.
    """
    store = InMemoryObjectStore()
    skewed = Lease(store, "skewed", ttl_seconds=0.01)
    # Holder writes expires_at with a clock 1 hour fast; ttl is what counts.
    with mock.patch("ducklake_serverless.lease.time.time", return_value=time.time() + 3600):
        assert skewed.acquire()
    time.sleep(0.05)  # let the store-clock ttl lapse
    # Store-clock arithmetic sees the lease as expired despite the huge
    # holder-clock expires_at; takeover must succeed.
    assert Lease(store, "honest", ttl_seconds=60).acquire()


# --- pure classify_lease: the acquisition decision, no I/O ---


def test_classify_vacant_when_absent() -> None:
    assert isinstance(classify_lease(None, "a"), LeaseVacant)


def test_classify_other_holder_live_is_held() -> None:
    state = classify_lease(Observed(holder="b", seconds_left=30.0, etag="e1"), "a")
    assert isinstance(state, LeaseHeldByOther)
    assert state.seconds_left == 30.0


def test_classify_other_holder_expired_is_acquirable() -> None:
    state = classify_lease(Observed(holder="b", seconds_left=-1.0, etag="e1"), "a")
    assert isinstance(state, LeaseAcquirable)
    assert state.etag == "e1"


def test_classify_own_lease_acquirable_regardless_of_expiry() -> None:
    live = classify_lease(Observed(holder="a", seconds_left=30.0, etag="e1"), "a")
    expired = classify_lease(Observed(holder="a", seconds_left=-5.0, etag="e2"), "a")
    assert isinstance(live, LeaseAcquirable) and live.etag == "e1"
    assert isinstance(expired, LeaseAcquirable) and expired.etag == "e2"


def test_classify_corrupt_reads_as_acquirable() -> None:
    # A corrupt body parses to holder "<corrupt>", ttl/expiry 0 -> seconds_left <= 0.
    state = classify_lease(Observed(holder="<corrupt>", seconds_left=0.0, etag="e1"), "a")
    assert isinstance(state, LeaseAcquirable)
