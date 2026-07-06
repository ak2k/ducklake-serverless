"""Domain errors.

All errors raised across module boundaries should inherit from `AppError`.
Never catch and re-raise external library exceptions unchanged — wrap them
in a domain error so callers can match on intent, not library identity.
"""

from __future__ import annotations


class AppError(Exception):
    """Base class for all application errors."""


class NotFoundError(AppError):
    """Raised when an expected resource is missing."""


class InputValidationError(AppError):
    """Raised when caller-supplied input fails domain validation.

    Distinct from `pydantic.ValidationError`, which is library-internal and
    should be wrapped at the boundary (typically as `ExternalServiceError`
    when upstream data is malformed, or `InputValidationError` when the
    caller passed bad input).
    """


class ExternalServiceError(AppError):
    """Raised when an external dependency fails (transport, 5xx, malformed response)."""


class ObjectNotFoundError(NotFoundError):
    """The requested object key does not exist in the store."""


class PreconditionFailedError(AppError):
    """Conditional write rejected: the precondition did not hold (HTTP 412).

    For a root CAS this means another writer committed first — the caller
    must re-read the root and decide (rebase or resolve-as-won).
    """


class ConditionalConflictError(AppError):
    """Concurrent conditional writes raced mid-flight (HTTP 409).

    Semantically identical to `PreconditionFailedError` for this protocol: re-read
    the root and resolve by commit token.
    """


class AmbiguousCasError(AppError):
    """The CAS request outcome is unknown (timeout/transport failure).

    The write may have landed. Resolve by re-reading the root and comparing
    its catalog UUID against ours — never by retrying the conditional PUT.
    """


class ConflictAbortError(AppError):
    """The transaction lost a commit race and cannot be safely replayed.

    The application should re-read current state, re-decide, and run a fresh
    transaction.
    """


class CatalogHygieneError(AppError):
    """The prepared catalog file failed pre-publish checks.

    A WAL sidecar, bad magic bytes, or a blown size cap means publishing
    could corrupt the lake.
    """


class VersionMismatchError(AppError):
    """Local DuckDB/DuckLake versions do not match the lake's root doc.

    Committing would silently migrate the catalog format for the whole
    fleet. Upgrades are an explicit admin operation.
    """
