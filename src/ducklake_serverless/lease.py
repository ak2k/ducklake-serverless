"""TTL lease over the object store: mutual exclusion for maintenance.

The commit path never uses leases — CAS on the root is its only
coordination. Leases exist for background maintenance (GC/compaction)
where "at most one runner fleet-wide" is wanted and a crashed holder must
not block forever.

Clock discipline: expiry is anchored to the STORE's clock, not any
holder's. The lease body carries only `{holder, ttl}`; a would-be taker
computes expiry as the lease object's `last_modified` (S3 server time)
plus its ttl, compared against the same server's time as observed via a
freshly-written probe. Holders' wall clocks never enter the comparison,
so maintenance hosts with skewed clocks cannot shorten or stretch each
other's leases. (Ported from the haystack S3Lease pattern, which got
this right first.) Residual skew is only the store's own clock drift
between two of its writes — negligible. Lease consumers must still stay
safe under brief holder overlap (GC is: swept keys are immutable garbage,
so overlapping sweeps are idempotent — see gc.py's module docstring).
"""

from __future__ import annotations

import contextlib
import json
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ducklake_serverless.errors import (
    ConditionalConflictError,
    ObjectNotFoundError,
    PreconditionFailedError,
)
from ducklake_serverless.models import (
    LeaseAcquirable,
    LeaseHeldByOther,
    LeaseState,
    LeaseVacant,
)

if TYPE_CHECKING:
    from ducklake_serverless.objectstore import ObjectStore

LEASE_KEY = "maintenance-lease"


@dataclass(frozen=True)
class Observed:
    """One read of the lease object: who holds it, seconds left, and its etag.

    `seconds_left` is computed against the STORE's clock (see module docstring);
    <= 0 means expired. A corrupt body reads as expired-and-unowned.
    """

    holder: str
    seconds_left: float
    etag: str


def classify_lease(observed: Observed | None, holder_id: str) -> LeaseState:
    """Pure decision: what can `holder_id` do with the observed lease?

    Vacant (no object) and Acquirable (expired, ours, or corrupt) are both
    takeable; only a live lease held by someone else refuses acquisition.
    No I/O — the clock work happens in `_observe`, so this is exhaustively
    unit-testable.
    """
    if observed is None:
        return LeaseVacant()
    if observed.holder != holder_id and observed.seconds_left > 0:
        return LeaseHeldByOther(seconds_left=observed.seconds_left)
    return LeaseAcquirable(etag=observed.etag)


class Lease:
    """Acquire/renew/release a named TTL lease via conditional writes."""

    def __init__(
        self,
        store: ObjectStore,
        holder_id: str,
        *,
        key: str = LEASE_KEY,
        ttl_seconds: float = 300.0,
    ) -> None:
        self._store = store
        self._holder = holder_id
        self._key = key
        self._ttl = ttl_seconds
        self._etag: str | None = None

    def _body(self, ttl: float | None = None) -> bytes:
        effective = ttl if ttl is not None else self._ttl
        return json.dumps(
            {
                "holder": self._holder,
                "ttl": effective,
                # Holder-clock fallback, used ONLY when the store reports no
                # timestamps; store time is authoritative when present.
                "expires_at": time.time() + effective,
            }
        ).encode()

    def _observe(self) -> Observed | None:
        """Read the lease once: holder, seconds left, and etag; None if absent.

        Both timestamps come from the store: the lease object's
        last_modified, and 'now' as the last_modified of a probe object we
        just wrote. When the backend reports no timestamps, fall back to
        local time for 'now' only (single-clock comparison degrades to the
        old holder-clock behavior, never to something worse). One GET —
        the holder rides along, so callers never re-read to learn it.
        """
        try:
            current = self._store.get(self._key)
        except ObjectNotFoundError:
            return None
        holder, ttl, expires_at = _parse(current.body)
        if current.last_modified is None:
            # Timestampless backend: degrade to the holder-clock expiry.
            return Observed(holder, expires_at - time.time(), current.etag)
        probe_key = f"{self._key}.now-probe-{uuid.uuid4()}"
        self._store.put_if_absent(probe_key, b"t")
        try:
            now_dt = self._store.get(probe_key).last_modified
        finally:
            self._store.delete(probe_key)
        if now_dt is None:  # store gave a timestamp once but not twice — degrade likewise
            return Observed(holder, expires_at - time.time(), current.etag)
        elapsed = (now_dt - current.last_modified).total_seconds()
        return Observed(holder, ttl - elapsed, current.etag)

    def acquire(self) -> bool:
        """Try to take the lease. True iff we now hold it.

        Fresh key: create-only PUT. Existing key: overwrite via If-Match
        only when expired or already ours — an atomic takeover, never
        delete-then-create (that would race).
        """
        try:
            self._etag = self._store.put_if_absent(self._key, self._body())
        except PreconditionFailedError:
            pass
        else:
            return True

        match classify_lease(self._observe(), self._holder):
            case LeaseVacant():
                return self.acquire()  # holder released between our calls
            case LeaseHeldByOther():
                return False
            case LeaseAcquirable() as state:
                try:
                    self._etag = self._store.put_if_match(self._key, self._body(), state.etag)
                except (PreconditionFailedError, ConditionalConflictError, ObjectNotFoundError):
                    return False  # lost the takeover race
                return True

    def renew(self) -> bool:
        """Extend the lease (rewrites it, resetting last_modified). True iff held."""
        if self._etag is None:
            return False
        try:
            self._etag = self._store.put_if_match(self._key, self._body(), self._etag)
        except (PreconditionFailedError, ConditionalConflictError, ObjectNotFoundError):
            self._etag = None
            return False
        return True

    def release(self) -> None:
        """Give the lease up by tombstoning it (atomic, never delete).

        A check-then-delete could remove a successor's live lease taken
        over between the check and the delete. Overwriting our own lease
        with a zero-ttl body via If-Match is atomic: it succeeds only
        while we still hold it, and the tombstone is immediately
        acquirable by anyone.
        """
        if self._etag is None:
            return
        with contextlib.suppress(
            PreconditionFailedError, ConditionalConflictError, ObjectNotFoundError
        ):  # a successor already took over — nothing of ours to release
            self._store.put_if_match(self._key, self._body(ttl=0.0), self._etag)
        self._etag = None


def _parse(body: bytes) -> tuple[str, float, float]:
    """Parse a lease body; malformed bodies read as expired-and-unowned."""
    try:
        doc: dict[str, object] = json.loads(body)  # pyright: ignore[reportAny]  # validated below
        return (
            str(doc["holder"]),
            float(doc["ttl"]),  # pyright: ignore[reportArgumentType]  # ValueError caught
            float(doc["expires_at"]),  # pyright: ignore[reportArgumentType]
        )
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return "<corrupt>", 0.0, 0.0
