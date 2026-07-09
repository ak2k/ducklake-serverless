"""Generation-marker commit protocol: create, resolve, discover head.

A commit is the create-only PUT of an immutable marker `roots/<gen>`
(If-None-Match: *). Exactly one body ever exists per generation, and
markers are NEVER deleted — so "did my commit land?" is answered by one
GET of the exact key I tried to create, and the answer is permanent:

  present with my uuid  -> WON (forever)
  present with another  -> LOST (forever)
  absent                -> never landed; safe to retry the same doc

The advisory `root-hint` (a bare generation number) is only a probe start
position — always verified by GETting the marker it names, never trusted
as a document. The markers themselves are dense (0..head, no gaps, because
each marker's creator read its parent), which is what makes discovery by
galloping correct.
"""

from __future__ import annotations

import contextlib
from enum import StrEnum
from typing import TYPE_CHECKING

import pydantic

from ducklake_serverless.errors import (
    ExternalServiceError,
    LakeNotInitializedError,
    ObjectNotFoundError,
)
from ducklake_serverless.models import MAX_GENERATION, HintDoc, RootDoc, format_marker_key

if TYPE_CHECKING:
    from uuid import UUID

    from ducklake_serverless.objectstore import ObjectStore

ROOT_HINT_KEY = "root-hint"

# Forward-probe safety cap. Hitting it just returns the last-found marker —
# a recent, fully-committed head, which is all any caller needs (nobody
# requires the true instantaneous head). Only reached under a commit rate
# that outpaces the reader's GET rate, where "latest" is undefined anyway.
PROBE_CAP = 4096
# Galloping step ceiling: bounds a single probe jump when a hint lags far
# behind, without letting the step overshoot into a huge sparse scan.
GALLOP_CAP = 1024


class MarkerOutcome(StrEnum):
    """Resolution of a marker create whose outcome we must verify."""

    WON = "won"  # the extant marker carries our uuid
    LOST = "lost"  # the extant marker carries someone else's uuid
    ABSENT = "absent"  # no marker exists; our create never landed


def read_marker(store: ObjectStore, generation: int) -> RootDoc:
    """Fetch and validate one generation marker. Raises ObjectNotFoundError if absent."""
    result = store.get(format_marker_key(generation))
    try:
        doc = RootDoc.from_json_bytes(result.body)
    except pydantic.ValidationError as exc:
        raise ExternalServiceError(f"marker roots/{generation:08d} is malformed") from exc
    if doc.generation != generation:
        raise ExternalServiceError(
            f"marker roots/{generation:08d} body claims generation {doc.generation}"
        )
    return doc


def create_marker(store: ObjectStore, doc: RootDoc) -> None:
    """Create-only PUT of a marker (the commit point).

    PreconditionFailedError/ConditionalConflictError (someone won this
    generation) and AmbiguousCasError (unknown outcome) propagate — resolve
    either with `resolve_marker`.
    """
    store.put_if_absent(doc.marker_key, doc.to_json_bytes())


def resolve_marker(store: ObjectStore, generation: int, our_uuid: UUID) -> MarkerOutcome:
    """Decide WON / LOST / ABSENT for a marker create with an uncertain outcome.

    Never retries a conditional PUT to answer this — one GET of the
    immutable marker is definitive and permanent.
    """
    try:
        doc = read_marker(store, generation)
    except ObjectNotFoundError:
        return MarkerOutcome.ABSENT
    return MarkerOutcome.WON if doc.payload_uuid == our_uuid else MarkerOutcome.LOST


def write_hint(store: ObjectStore, generation: int) -> None:
    """Best-effort advance of the advisory hint. Failures are swallowed.

    The hint carries only a generation number; correctness never depends on
    its freshness, existence, or monotonicity.
    """
    with contextlib.suppress(ExternalServiceError):
        # advisory only — a commit is already durable at marker creation
        store.put(ROOT_HINT_KEY, HintDoc(generation=generation).to_json_bytes())


def _hint_generation(store: ObjectStore) -> int | None:
    """Read the hint's generation, or None if unreadable (never trusted directly).

    The hint is advisory: `write_hint` swallows write failures, so the read
    side must be equally forgiving. Missing, corrupt, OR a transient transport
    failure all degrade to None — `resolve_head` then falls back to
    gallop-discovery over the immortal markers. A hint read must never be able
    to fail head resolution.
    """
    try:
        result = store.get(ROOT_HINT_KEY)
    except (ObjectNotFoundError, ExternalServiceError):
        return None
    try:
        return HintDoc.from_json_bytes(result.body).generation
    except pydantic.ValidationError:
        return None  # corrupt hint reads as missing


def _gallop_discover(store: ObjectStore) -> RootDoc:
    """Find a valid head with no usable hint: exponential search over dense markers.

    Markers are dense and immortal, so presence is monotonic in generation:
    double from 0 to the first gap, then binary-search the frontier. O(log
    head) GETs. Raises LakeNotInitializedError if generation 0 is absent.
    """
    try:
        low_doc = read_marker(store, 0)
    except ObjectNotFoundError as exc:
        raise LakeNotInitializedError("no roots/00000000 marker") from exc

    # Exponential bracket: lo is known-present, hi is the first known-absent.
    lo = 0
    step = 1
    hi: int | None = None
    while hi is None:
        probe = lo + step
        if probe > MAX_GENERATION:
            # Nothing can exist past the max generation (create enforces it),
            # so the frontier is at/below it. Clamp the bracket so the binary
            # search below never probes an out-of-range (unformattable) key.
            hi = MAX_GENERATION + 1
            break
        try:
            read_marker(store, probe)
        except ObjectNotFoundError:
            hi = probe
        else:
            lo = probe
            step *= 2
    # Binary search for the last present generation in (lo, hi).
    last = lo
    left, right = lo + 1, hi - 1
    while left <= right:
        mid = (left + right) // 2
        try:
            read_marker(store, mid)
        except ObjectNotFoundError:
            right = mid - 1
        else:
            last = mid
            left = mid + 1
    return low_doc if last == 0 else read_marker(store, last)


def resolve_head(store: ObjectStore) -> tuple[RootDoc, int]:
    """Resolve a recent committed head: (doc, generation).

    Start from the hint if it verifies, else discover by galloping. Then
    forward-probe (galloping under lag) to the frontier, capped. The result
    is always a real, fully-committed generation — possibly a moment stale,
    which is the same staleness a single mutable-root read had in v1.
    """
    hint = _hint_generation(store)
    if hint is None:
        doc = _gallop_discover(store)
    else:
        try:
            doc = read_marker(store, hint)
        except ObjectNotFoundError:
            # Poison-high or GC-swept hint: the number names no marker.
            # Never probe backward from an unverified hint — rediscover.
            doc = _gallop_discover(store)

    generation = doc.generation
    step = 1
    probes = 0
    while probes < PROBE_CAP:
        probes += 1
        target = generation + step
        if target > MAX_GENERATION:
            # Beyond the max generation nothing can exist — treat like a gap.
            if step == 1:
                break  # at the max generation; no successor is possible
            step = 1
            continue
        try:
            nxt = read_marker(store, target)
        except ObjectNotFoundError:
            if step == 1:
                break  # true frontier
            step = 1  # overshoot after a gallop — refine one at a time
            continue
        doc = nxt
        generation += step
        step = min(step * 2, GALLOP_CAP)
    return doc, generation
