"""TTL lease over the object store: mutual exclusion for maintenance.

The commit path never uses leases — CAS on the root is its only
coordination. Leases exist for background maintenance (GC/compaction)
where "at most one runner fleet-wide" is wanted and a crashed holder must
not block forever. Expiry is computed from the store's LastModified-style
server time... except ObjectStore has no timestamps, so the lease body
carries an `expires_at` epoch written by the holder. Clock skew between
maintenance hosts therefore erodes the guarantee at the margin — TTLs
should be minutes, not milliseconds, and GC must remain safe even if two
runners briefly overlap (it is: every mutation goes through CAS).
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING

from ducklake_serverless.errors import (
    ConditionalConflictError,
    ObjectNotFoundError,
    PreconditionFailedError,
)

if TYPE_CHECKING:
    from ducklake_serverless.objectstore import ObjectStore

LEASE_KEY = "maintenance-lease"


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

    def _body(self) -> bytes:
        return json.dumps({"holder": self._holder, "expires_at": time.time() + self._ttl}).encode()

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

        try:
            current = self._store.get(self._key)
        except ObjectNotFoundError:
            return self.acquire()  # holder released between our calls
        holder, expires_at = _parse(current.body)
        if holder != self._holder and expires_at > time.time():
            return False
        try:
            self._etag = self._store.put_if_match(self._key, self._body(), current.etag)
        except (PreconditionFailedError, ConditionalConflictError, ObjectNotFoundError):
            return False  # lost the takeover race
        return True

    def renew(self) -> bool:
        """Extend the lease. True iff still held after the call."""
        if self._etag is None:
            return False
        try:
            self._etag = self._store.put_if_match(self._key, self._body(), self._etag)
        except (PreconditionFailedError, ConditionalConflictError, ObjectNotFoundError):
            self._etag = None
            return False
        return True

    def release(self) -> None:
        """Give the lease up. Only deletes if we still hold it."""
        if self._etag is None:
            return
        try:
            current = self._store.get(self._key)
        except ObjectNotFoundError:
            self._etag = None
            return
        if current.etag == self._etag:
            self._store.delete(self._key)
        self._etag = None


def _parse(body: bytes) -> tuple[str, float]:
    """Parse a lease body; malformed bodies read as expired-and-unowned."""
    try:
        doc: dict[str, object] = json.loads(body)  # pyright: ignore[reportAny]  # validated below
        return str(doc["holder"]), float(doc["expires_at"])  # pyright: ignore[reportArgumentType]  # ValueError caught
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return "<corrupt>", 0.0
