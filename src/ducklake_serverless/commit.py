"""The generic, payload-agnostic commit driver.

The trust-root commit loop, lifted out of the DuckLake session: publish a
prepared work file as the next immutable generation, create its create-only
marker (`roots/<gen>`), and — on a lost CAS race — consult the payload's
rebase policy to replay onto the new head or abort. It knows nothing about
DuckDB, SQL, or catalogs; a `CommitContext` supplies the three payload-specific
hooks (`validate`, `decide_rebase`, `replay`). New-generation pins are carried
forward from the base marker, so a commit never changes the lake's pinned
versions — an upgrade is a separate, explicit operation.

A marker create is create-only, so both a 412 (a rival won this generation) and
an ambiguous outcome resolve by GETting the marker: our uuid means WON
(permanent — the marker is immutable and immortal), anyone else's means LOST,
absent means our create never landed. The loop is a small state machine over
`_Attempt`/`_Committed`/`_Aborted`, so an illegal transition is unrepresentable.
"""

from __future__ import annotations

import os
import random
import socket
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol
from uuid import UUID, uuid4

from ducklake_serverless import __version__, chunk
from ducklake_serverless.errors import (
    AmbiguousCasError,
    BackendUnsafeError,
    ConditionalConflictError,
    ConflictAbortError,
    ExternalServiceError,
    ObjectNotFoundError,
    PreconditionFailedError,
)
from ducklake_serverless.models import (
    Abort,
    CommitResult,
    Replay,
    RootDoc,
    WriterInfo,
    format_payload_key,
)
from ducklake_serverless.objectstore import probe_atomic_create, probe_capabilities
from ducklake_serverless.root import (
    MarkerOutcome,
    create_marker,
    resolve_marker,
    write_hint,
)

if TYPE_CHECKING:
    from pathlib import Path

    from ducklake_serverless.models import RebaseDecision
    from ducklake_serverless.objectstore import ObjectStore

_BACKOFF_BASE_S = 0.05
_BACKOFF_CAP_S = 2.0


@dataclass(frozen=True)
class TransportPolicy:
    """When and how a commit uses the chunked transport.

    `chunk_threshold=None` means never chunk (whole-file always); `0` means
    always chunk. Between: payloads at least the threshold are chunked.
    `base_manifest` is the BASE generation's manifest when the base itself is
    chunked — the ONLY legal dedup source (load-bearing for GC; see chunk.py).
    """

    chunk_threshold: int | None = None
    base_manifest: chunk.Manifest | None = None
    pack_target: int = chunk.DEFAULT_PACK_TARGET


class CommitContext(Protocol):
    """The payload-specific hooks the generic commit loop needs.

    A `Lake` builds one of these per transaction, closing over whatever the
    payload's rebase needs (for DuckLake, the recorded SQL changeset). A plain
    blob supplies a trivial context: `validate` is a no-op, `decide_rebase`
    always aborts, and `replay` is never reached.
    """

    def validate(self, work: Path, base: RootDoc) -> None:
        """Reject the prepared work file before it is published.

        Raises a domain error (hygiene failure, or a format migration relative
        to `base`) that aborts the commit. Runs before the create-only upload,
        so a rejection strands nothing.
        """
        ...

    def decide_rebase(self, attempt: int, max_attempts: int) -> RebaseDecision:
        """Choose whether to replay the change onto the new head or abort."""
        ...

    def replay(self, stale_work: Path) -> tuple[RootDoc, Path]:
        """Discard the losing work copy and re-derive (base, work) on the head.

        Called only when `decide_rebase` returned `Replay`. Returns the current
        head marker and a fresh work copy with the change re-applied, ready to
        publish as head+1.
        """
        ...

    def transport_policy(self, base: RootDoc) -> TransportPolicy:
        """Chunking policy for this commit, given the base being built upon.

        The context owns fetching the base's manifest when the base is chunked
        (it already holds the store and the base doc's transport).
        """
        ...


def writer_info() -> WriterInfo:
    """Provenance for the writer publishing a generation."""
    return WriterInfo(lib_version=__version__, host=socket.gethostname(), pid=os.getpid())


def require_atomic_create(store: ObjectStore) -> None:
    """Refuse a backend whose create-only isn't atomic under concurrency.

    The marker protocol serializes commits on `If-None-Match: *`, so a store
    that resolves concurrent creates last-writer-wins (iDrive E2, garage,
    `rclone serve s3`) would silently lose commits. Callers may skip this for a
    single-writer lake where no concurrent create can occur.
    """
    if not probe_atomic_create(store):
        # Re-probe the full set only to enrich the refusal diagnostic; the happy
        # path above pays for a single contention round, not two.
        caps = probe_capabilities(store)
        raise BackendUnsafeError(
            "backend does not enforce If-None-Match: * atomically under "
            "concurrency (concurrent create-only PUTs all 'win', silently "
            "losing commits) — it cannot serialize a marker-protocol lake. "
            f"Probed capabilities: {caps}. Use an atomic backend (MinIO, "
            "AWS S3, SeaweedFS) for concurrent writers, or "
            "verify_backend=False for a single-writer lake."
        )


def _backoff(attempt: int) -> None:
    delay: float = min(_BACKOFF_CAP_S, _BACKOFF_BASE_S * (2.0**attempt))
    jitter: float = random.uniform(0, delay)  # noqa: S311  # jitter, not crypto
    time.sleep(jitter)


@dataclass(frozen=True)
class _Attempt:
    """Commit phase: publish + create the marker at `base.generation + 1`."""

    base: RootDoc
    work: Path
    attempt: int


@dataclass(frozen=True)
class _Committed:
    """Terminal commit phase: the marker landed (created, or resolved WON)."""

    result: CommitResult


@dataclass(frozen=True)
class _Aborted:
    """Terminal commit phase: lost the race and the change can't replay."""

    reason: str


_CommitPhase = _Attempt | _Committed | _Aborted


@dataclass(frozen=True)
class PublishedWhole:
    """The payload landed as raw bytes at its payload key."""


@dataclass(frozen=True)
class PublishedChunked:
    """The payload landed as a manifest; its packs are published."""


@dataclass(frozen=True)
class NotLanded:
    """The upload definitively did not land — retry with backoff."""


# Discriminated publish outcome: the marker's `transport` field DERIVES from
# this type in `_advance`, so a marker can never claim a transport the publish
# didn't actually use (the inheritance bug is unrepresentable).
PublishOutcome = PublishedWhole | PublishedChunked | NotLanded


def run_commit(
    store: ObjectStore, base: RootDoc, work: Path, ctx: CommitContext, max_attempts: int
) -> CommitResult:
    """Publish `work` as the next generation, rebasing onto the head until won."""
    phase: _CommitPhase = _Attempt(base=base, work=work, attempt=0)
    while True:
        match phase:
            case _Committed():
                return phase.result
            case _Aborted():
                raise ConflictAbortError(phase.reason)
            case _Attempt():
                phase = _advance(store, phase, ctx, max_attempts)


def _advance(
    store: ObjectStore, phase: _Attempt, ctx: CommitContext, max_attempts: int
) -> _CommitPhase:
    """Run one commit attempt and return the next phase.

    Validate + publish the work file and create the marker at
    `base.generation + 1`. A GC-race on publish retries the same base; a win
    commits; a loss consults `decide_rebase` — abort, or rebase onto the current
    HEAD (not just the collided marker — a stale writer must not burn one
    attempt per generation of lag) and try the next generation.
    """
    target = phase.base.generation + 1
    new_uuid = uuid4()
    ctx.validate(phase.work, phase.base)
    policy = ctx.transport_policy(phase.base)
    outcome = _publish_resolved(
        store, phase.work, target, new_uuid, phase.attempt, max_attempts, policy
    )
    match outcome:
        case NotLanded():
            next_attempt = phase.attempt + 1
            _backoff(next_attempt)
            return _Attempt(base=phase.base, work=phase.work, attempt=next_attempt)
        case PublishedWhole():
            transport = "whole"
        case PublishedChunked():
            transport = "chunked"
    new_doc = phase.base.model_copy(
        update={
            "generation": target,
            "payload_uuid": new_uuid,
            "created_at": datetime.now(tz=UTC),
            "writer": writer_info(),
            # Derived from the publish outcome above — never inherited from
            # the base marker, which may have used a different transport.
            "transport": transport,
        }
    )

    result = _try_create_marker(store, new_doc, new_uuid, phase.attempt, max_attempts)
    if result is not None:
        return _Committed(result=result)

    attempt = phase.attempt + 1
    decision = ctx.decide_rebase(attempt, max_attempts)
    match decision:
        case Abort():
            return _Aborted(reason=decision.reason)
        case Replay():
            _backoff(attempt)
            base, work = ctx.replay(phase.work)
            return _Attempt(base=base, work=work, attempt=attempt)


def _publish_resolved(
    store: ObjectStore,
    work: Path,
    generation: int,
    new_uuid: UUID,
    attempt: int,
    max_attempts: int,
    policy: TransportPolicy,
) -> PublishOutcome:
    """Upload a generation's payload, resolving ambiguous outcomes.

    Whole path: one create-only PUT of the raw bytes. Chunked path (payload at
    least `policy.chunk_threshold`): build manifest + novel packs deduped
    against the base manifest, PUT packs, verify/heal them (stalled-writer
    defense — see chunk.verify_packs), then create-only PUT the manifest at
    the payload key. Either way the payload key is unique and immutable, so
    ambiguity resolves with one GET: present means it landed.
    """
    key = format_payload_key(generation, new_uuid)
    threshold = policy.chunk_threshold
    if threshold is not None and work.stat().st_size >= threshold:
        manifest, packs = chunk.build_manifest(
            work, policy.base_manifest, pack_target=policy.pack_target
        )
        chunk.publish_packs(store, packs)
        chunk.verify_packs(store, manifest, chunk.novel_pack_index(packs))
        body = manifest.to_bytes()
        landed: PublishOutcome = PublishedChunked()
    else:
        body = work.read_bytes()
        landed = PublishedWhole()
    try:
        store.put_if_absent(key, body)
    except AmbiguousCasError:
        try:
            store.get(key)
        except ObjectNotFoundError:
            if attempt + 1 >= max_attempts:
                raise ConflictAbortError(
                    f"payload upload kept failing across {max_attempts} attempts"
                ) from None
            return NotLanded()
    return landed


def create_marker_resolving(
    store: ObjectStore, doc: RootDoc, doc_uuid: UUID, base_attempt: int, max_attempts: int
) -> MarkerOutcome:
    """Create `doc`'s marker, resolving ambiguity by GET; retry on ABSENT.

    A marker create is create-only, so a 412/409 and an ambiguous outcome both
    resolve by GETting the marker: our uuid means WON (permanent — the marker is
    immutable and immortal), anyone else's means LOST, absent means our create
    never landed. ABSENT retries the SAME doc (safe: the key is never deleted,
    so re-creating is idempotent against a still-in-flight twin), bounded by
    max_attempts. Returns WON or LOST, never ABSENT — an ABSENT that never
    resolves raises ExternalServiceError.
    """
    target = doc.generation
    local_attempt = 0
    while True:
        try:
            create_marker(store, doc)
        except (PreconditionFailedError, ConditionalConflictError, AmbiguousCasError):
            outcome = resolve_marker(store, target, doc_uuid)
        else:
            return MarkerOutcome.WON
        match outcome:
            case MarkerOutcome.WON | MarkerOutcome.LOST:
                return outcome
            case MarkerOutcome.ABSENT:
                local_attempt += 1
                if base_attempt + local_attempt >= max_attempts:
                    raise ExternalServiceError(
                        f"marker create for generation {target} kept failing "
                        f"across {max_attempts} attempts"
                    )
                _backoff(base_attempt + local_attempt)


def _try_create_marker(
    store: ObjectStore, new_doc: RootDoc, new_uuid: UUID, attempt: int, max_attempts: int
) -> CommitResult | None:
    """Attempt the marker create; return a CommitResult on WIN, else None.

    None means the generation was lost (a rival won it) — the caller rebases
    onto the resolved head and tries the next generation.
    """
    match create_marker_resolving(store, new_doc, new_uuid, attempt, max_attempts):
        case MarkerOutcome.WON:
            write_hint(store, new_doc.generation)
            return CommitResult(
                generation=new_doc.generation, payload_uuid=new_uuid, attempts=attempt + 1
            )
        case MarkerOutcome.LOST:
            return None
        case MarkerOutcome.ABSENT:  # invariant: create_marker_resolving never returns ABSENT
            raise AssertionError("create_marker_resolving returned ABSENT")
