"""Root-pointer protocol: bootstrap, read, publish, and ambiguity resolution.

The root is the lake's single serialization point. Publishing a new
generation is one conditional PUT; every conflict and every ambiguous
outcome resolves by re-reading the root and comparing catalog UUIDs — the
UUID is the commit token, so "did my write land?" is always answerable
without retrying the conditional PUT itself.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

import pydantic

from ducklake_serverless.errors import ExternalServiceError, ObjectNotFoundError
from ducklake_serverless.models import RootDoc

if TYPE_CHECKING:
    from uuid import UUID

    from ducklake_serverless.objectstore import ObjectStore

ROOT_KEY = "root"


class CasOutcome(StrEnum):
    """Resolution of an ambiguous or rejected root CAS."""

    WON = "won"
    LOST = "lost"


def read_root(store: ObjectStore) -> tuple[RootDoc, str]:
    """Fetch the current root and the ETag required to CAS against it."""
    result = store.get(ROOT_KEY)
    try:
        doc = RootDoc.from_json_bytes(result.body)
    except pydantic.ValidationError as exc:
        raise ExternalServiceError("root document is malformed") from exc
    return doc, result.etag


def bootstrap_root(store: ObjectStore, doc: RootDoc) -> str:
    """Create the lake's first root (generation 0). Create-only: loses to any racer."""
    return store.put_if_absent(ROOT_KEY, doc.to_json_bytes())


def publish_root(store: ObjectStore, doc: RootDoc, expect_etag: str) -> str:
    """CAS the root forward.

    PreconditionFailedError / ConditionalConflictError / AmbiguousCasError
    propagate — resolve them with `resolve_cas`.
    """
    return store.put_if_match(ROOT_KEY, doc.to_json_bytes(), expect_etag)


def resolve_cas(store: ObjectStore, our_uuid: UUID) -> tuple[CasOutcome, RootDoc, str]:
    """Decide whether an uncertain root CAS actually landed.

    Never retries the PUT: an SDK-level retry of a conditional write can 412
    against our own successful first attempt. Re-reading and comparing the
    commit token is the only safe resolution.
    """
    try:
        doc, etag = read_root(store)
    except ObjectNotFoundError as exc:
        raise ExternalServiceError("root disappeared while resolving a CAS outcome") from exc
    outcome = CasOutcome.WON if doc.catalog_uuid == our_uuid else CasOutcome.LOST
    return outcome, doc, etag
