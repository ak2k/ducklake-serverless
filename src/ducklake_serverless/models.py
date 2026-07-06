"""Protocol data models.

The root document is the single mutable object in the lake; everything else
is immutable and content-addressed by (generation, uuid). `catalog_key` is a
derived property — never stored — so a generation/key mismatch is
unrepresentable.
"""

from __future__ import annotations

import re
from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from ducklake_serverless.errors import InputValidationError

CATALOG_PREFIX = "catalog/"
_CATALOG_KEY_RE = re.compile(
    r"^catalog/cat-(?P<gen>\d{8})-(?P<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.duckdb$"
)


MAX_GENERATION = 10**8 - 1  # the key format zero-pads to exactly 8 digits


def format_catalog_key(generation: int, catalog_uuid: UUID) -> str:
    """Canonical object key for a catalog generation."""
    if generation < 0:
        raise InputValidationError(f"generation must be >= 0, got {generation}")
    if generation > MAX_GENERATION:
        # Beyond 8 digits the key would format but never parse back —
        # GC would silently stop sweeping. Fail loudly at the source.
        raise InputValidationError(
            f"generation {generation} exceeds the 8-digit key format (max {MAX_GENERATION})"
        )
    return f"{CATALOG_PREFIX}cat-{generation:08d}-{catalog_uuid}.duckdb"


def parse_catalog_key(key: str) -> tuple[int, UUID]:
    """Inverse of `format_catalog_key`. Raises on any non-canonical key."""
    m = _CATALOG_KEY_RE.match(key)
    if m is None:
        raise InputValidationError(f"not a canonical catalog key: {key!r}")
    return int(m.group("gen")), UUID(m.group("uuid"))


class WriterInfo(BaseModel):
    """Informational provenance for the writer that published a root."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    lib_version: str
    host: str
    pid: int


class RootDoc(BaseModel):
    """The root pointer: the only mutable object in the lake.

    `created_at` is informational only — ordering comes exclusively from
    `generation` and CAS, never from clocks.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_id: Literal["ducklake-serverless-root/1"] = Field(
        default="ducklake-serverless-root/1", alias="schema"
    )
    generation: int = Field(ge=0, le=MAX_GENERATION)
    catalog_uuid: UUID
    duckdb_storage_version: str
    ducklake_format_version: str
    created_at: datetime
    writer: WriterInfo

    @property
    def catalog_key(self) -> str:
        """Object key of the catalog generation this root names."""
        return format_catalog_key(self.generation, self.catalog_uuid)

    def to_json_bytes(self) -> bytes:
        """Serialize for the root object body (schema field aliased)."""
        return self.model_dump_json(by_alias=True).encode()

    @classmethod
    def from_json_bytes(cls, data: bytes) -> RootDoc:
        """Parse a root object body; raises pydantic.ValidationError on mismatch."""
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


class CommitResult(BaseModel):
    """Outcome of a successful commit."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    generation: int
    catalog_uuid: UUID
    attempts: int
