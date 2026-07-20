"""Protocol data models.

Every durable object is immutable and content-addressed. A commit is the
creation of an immutable per-generation MARKER `roots/<gen>` (create-only)
whose body is a `RootDoc`; the catalog it names lives at `payload/<gen>-
<uuid>`. Both keys are derived from `(generation, uuid)` — never stored —
so a key/body mismatch is unrepresentable. The only mutable object is the
advisory `root-hint` (a `HintDoc`, a bare generation number), which no
correctness path may trust as a document source.
"""

from __future__ import annotations

import re
from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from ducklake_serverless.errors import InputValidationError

PAYLOAD_PREFIX = "payload/"
ROOTS_PREFIX = "roots/"
_PAYLOAD_KEY_RE = re.compile(
    r"^payload/(?P<gen>\d{8})-(?P<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$"
)
_MARKER_KEY_RE = re.compile(r"^roots/(?P<gen>\d{8})$")


MAX_GENERATION = 10**8 - 1  # the key format zero-pads to exactly 8 digits


def _check_generation(generation: int) -> None:
    """Reject generations the 8-digit zero-padded key format cannot round-trip."""
    if generation < 0:
        raise InputValidationError(f"generation must be >= 0, got {generation}")
    if generation > MAX_GENERATION:
        # Beyond 8 digits the key would format but never parse back —
        # discovery/GC would silently break. Fail loudly at the source.
        raise InputValidationError(
            f"generation {generation} exceeds the 8-digit key format (max {MAX_GENERATION})"
        )


def format_payload_key(generation: int, payload_uuid: UUID) -> str:
    """Canonical object key for one generation's payload bytes."""
    _check_generation(generation)
    return f"{PAYLOAD_PREFIX}{generation:08d}-{payload_uuid}"


def parse_payload_key(key: str) -> tuple[int, UUID]:
    """Inverse of `format_payload_key`. Raises on any non-canonical key."""
    m = _PAYLOAD_KEY_RE.match(key)
    if m is None:
        raise InputValidationError(f"not a canonical payload key: {key!r}")
    return int(m.group("gen")), UUID(m.group("uuid"))


def format_marker_key(generation: int) -> str:
    """Canonical object key for a generation marker (the commit point)."""
    _check_generation(generation)
    return f"{ROOTS_PREFIX}{generation:08d}"


def parse_marker_key(key: str) -> int:
    """Inverse of `format_marker_key`. Raises on any non-canonical key."""
    m = _MARKER_KEY_RE.match(key)
    if m is None:
        raise InputValidationError(f"not a canonical marker key: {key!r}")
    return int(m.group("gen"))


class WriterInfo(BaseModel):
    """Informational provenance for the writer that published a root."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    lib_version: str
    host: str
    pid: int


class RootDoc(BaseModel):
    """The body of one generation marker (`roots/<gen>`) — immutable.

    `created_at` is informational only — ordering comes exclusively from
    `generation`, never from clocks.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_id: Literal["ducklake-serverless-root/1"] = Field(
        default="ducklake-serverless-root/1", alias="schema"
    )
    generation: int = Field(ge=0, le=MAX_GENERATION)
    payload_uuid: UUID
    created_at: datetime
    writer: WriterInfo
    # Adapter-supplied version tags (e.g. duckdb/ducklake versions) so a future
    # writer can refuse a silent format migration. Empty for an opaque blob;
    # the engine never interprets these — only the payload adapter does.
    pins: dict[str, str] = Field(default_factory=dict)
    # How the payload object stores its bytes: raw ("whole") or as a chunk
    # manifest ("chunked" — see chunk.py). Engine-owned; readers dispatch on
    # this, never on content sniffing. Set explicitly per commit from the
    # publish outcome — NEVER inherited from the base generation's marker.
    transport: Literal["whole", "chunked"] = "whole"
    # Logical payload size in bytes, recorded at commit time (immutable, like
    # everything else in the marker). METADATA, not a read gate: listings and
    # heuristics read it marker-only instead of fetching the payload/manifest;
    # reads still derive truth from the payload itself (sha-verified), and a
    # disagreement is a corruption signal. Derived from the publish outcome,
    # never inherited.
    payload_size: int = Field(default=0, ge=0)

    @property
    def payload_key(self) -> str:
        """Object key of the payload bytes this generation names."""
        return format_payload_key(self.generation, self.payload_uuid)

    @property
    def marker_key(self) -> str:
        """Object key of the marker whose body this is."""
        return format_marker_key(self.generation)

    def to_json_bytes(self) -> bytes:
        """Serialize for the marker object body (schema field aliased).

        `transport` is written only when non-default ("chunked"): RootDoc is
        extra="forbid", so an old reader hard-fails on a chunked marker (it
        genuinely cannot read that generation) while whole-file markers stay
        byte-compatible with pre-transport readers.
        """
        exclude = {"transport"} if self.transport == "whole" else None
        return self.model_dump_json(by_alias=True, exclude=exclude).encode()

    @classmethod
    def from_json_bytes(cls, data: bytes) -> RootDoc:
        """Parse a marker body; raises pydantic.ValidationError on mismatch."""
        return cls.model_validate_json(data)


class HintDoc(BaseModel):
    """The advisory `root-hint` body: a bare latest-generation number.

    Never a document source — only a probe start position, always verified
    by GETting the marker it names. A poisoned or regressed hint costs
    extra probes, never wrong data.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_id: Literal["ducklake-serverless-hint/1"] = Field(
        default="ducklake-serverless-hint/1", alias="schema"
    )
    generation: int = Field(ge=0, le=MAX_GENERATION)

    def to_json_bytes(self) -> bytes:
        """Serialize for the hint object body (schema field aliased)."""
        return self.model_dump_json(by_alias=True).encode()

    @classmethod
    def from_json_bytes(cls, data: bytes) -> HintDoc:
        """Parse a hint body; raises pydantic.ValidationError on mismatch."""
        return cls.model_validate_json(data)


class StatementClass(StrEnum):
    """Replay-safety classification of a recorded SQL statement."""

    BLIND_APPEND = "blind_append"  # INSERT…VALUES / INSERT…SELECT over non-lake sources
    STATE_DEPENDENT_DML = "state_dependent_dml"  # UPDATE/DELETE/lake-reading INSERT
    DDL = "ddl"
    READ = "read"
    VOLATILE = "volatile"  # now()/random()/… — rejected at record time


class Statement(BaseModel):
    """One recorded statement: SQL text plus bound parameters."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    sql: str
    params: tuple[object, ...] = ()
    statement_class: StatementClass


class Changeset(BaseModel):
    """The logical content of one transaction, in execution order."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    statements: tuple[Statement, ...]

    @property
    def has_ddl(self) -> bool:
        """Whether any statement is DDL (always aborts on conflict)."""
        return any(s.statement_class is StatementClass.DDL for s in self.statements)

    @property
    def has_reads(self) -> bool:
        """Whether the transaction read lake state before writing.

        A recorded READ means the caller's later writes may encode decisions
        derived from state that a replay target no longer has — replaying
        the writes alone launders write skew through the append path.
        """
        return any(s.statement_class is StatementClass.READ for s in self.statements)

    @property
    def is_blind_append_only(self) -> bool:
        """Whether every write is a blind append (safe to auto-replay)."""
        writes = [s for s in self.statements if s.statement_class is not StatementClass.READ]
        return bool(writes) and all(
            s.statement_class is StatementClass.BLIND_APPEND for s in writes
        )


class ConflictPolicy(StrEnum):
    """What to do when a commit loses the CAS race."""

    APPEND_ONLY_REPLAY = "append_only_replay"  # default: replay blind appends, abort the rest
    REPLAY_ALL = "replay_all"  # caller asserts their DML is safe to re-execute
    ABORT_ALL = "abort_all"


class Replay(BaseModel):
    """Rebase decision: re-execute the changeset on the winner's generation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["replay"] = "replay"


class Abort(BaseModel):
    """Rebase decision: surface the conflict to the caller."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["abort"] = "abort"
    reason: str


RebaseDecision = Replay | Abort


class LeaseVacant(BaseModel):
    """No lease object exists — acquire by create-only PUT."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["vacant"] = "vacant"


class LeaseAcquirable(BaseModel):
    """A lease object exists but is takeable (expired, ours, or corrupt).

    Acquire by an If-Match overwrite against `etag` — atomic takeover, never
    delete-then-create.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["acquirable"] = "acquirable"
    etag: str


class LeaseHeldByOther(BaseModel):
    """A different holder's lease is still live — not acquirable."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["held_by_other"] = "held_by_other"
    seconds_left: float


LeaseState = LeaseVacant | LeaseAcquirable | LeaseHeldByOther


class CommitResult(BaseModel):
    """Outcome of a successful commit."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    generation: int
    payload_uuid: UUID
    attempts: int


class MaintenanceReport(BaseModel):
    """What a data-plane maintenance pass did (or would do, under dry_run)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    dry_run: bool
    snapshots_expired: tuple[str, ...] = ()
    files_cleaned: tuple[str, ...] = ()
    orphans_deleted: tuple[str, ...] = ()
