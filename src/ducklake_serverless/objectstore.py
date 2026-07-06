"""Typed object-store facade — the only module that imports boto3.

Everything above this layer speaks the `ObjectStore` protocol and domain
errors. CAS semantics live here: `put_if_absent` (If-None-Match: *) and
`put_if_match` (If-Match: <etag>) are the primitives the whole commit
protocol rests on.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Protocol

import botocore.exceptions

from ducklake_serverless.errors import (
    AmbiguousCasError,
    ConditionalConflictError,
    ExternalServiceError,
    ObjectNotFoundError,
    PreconditionFailedError,
)

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


def make_s3_client(endpoint_url: str | None = None, region_name: str | None = None) -> S3Client:
    """Create an S3 client with transport retries DISABLED — required.

    botocore's default retry modes transparently re-send PUTs on 5xx and
    socket timeouts, including conditional PUTs. A retried conditional PUT
    whose first attempt landed 412s against our own write, surfacing a
    definitive-looking PreconditionFailedError for a commit that actually
    succeeded. With retries off, transport failures surface as
    AmbiguousCasError and resolve via the commit token instead.
    """
    import boto3  # noqa: PLC0415  # optional import: only S3-backed stores need boto3
    from botocore.config import Config  # noqa: PLC0415

    return boto3.client(  # pyright: ignore[reportUnknownMemberType, reportReturnType]
        "s3",
        endpoint_url=endpoint_url,
        region_name=region_name,
        config=Config(retries={"max_attempts": 1}),
    )


@dataclass(frozen=True)
class GetResult:
    """An object's body plus the ETag needed for a later conditional PUT."""

    body: bytes
    etag: str


class ObjectStore(Protocol):
    """Minimal store surface the protocol needs; S3 and in-memory fakes satisfy it."""

    def get(self, key: str) -> GetResult:
        """Fetch an object body and its ETag."""
        ...

    def put_if_absent(self, key: str, body: bytes) -> str:
        """Create-only PUT: new ETag, or PreconditionFailedError if the key exists."""
        ...

    def put_if_match(self, key: str, body: bytes, etag: str) -> str:
        """Conditional overwrite: new ETag, or PreconditionFailedError on stale ETag."""
        ...

    def list_prefix(self, prefix: str) -> list[str]:
        """List keys under a prefix."""
        ...

    def delete(self, key: str) -> None:
        """Delete an object (idempotent)."""
        ...


def _error_code(exc: botocore.exceptions.ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def _status(exc: botocore.exceptions.ClientError) -> int:
    return int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))


class S3ObjectStore:
    """`ObjectStore` over a boto3 S3 client scoped to one bucket + key prefix."""

    def __init__(self, client: S3Client, bucket: str, prefix: str = "") -> None:
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.rstrip("/") + "/" if prefix else ""

    def _full(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def get(self, key: str) -> GetResult:
        """Fetch an object body and its ETag."""
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=self._full(key))
        except botocore.exceptions.ClientError as exc:
            if _error_code(exc) in ("NoSuchKey", "404"):
                raise ObjectNotFoundError(key) from exc
            raise ExternalServiceError(f"get {key!r}") from exc
        return GetResult(body=resp["Body"].read(), etag=resp["ETag"].strip('"'))

    def put_if_absent(self, key: str, body: bytes) -> str:
        """Create-only PUT via `If-None-Match: *`."""
        try:
            resp = self._client.put_object(
                Bucket=self._bucket,
                Key=self._full(key),
                Body=body,
                IfNoneMatch="*",
            )
        except botocore.exceptions.ClientError as exc:
            self._raise_conditional(exc, key)
            raise ExternalServiceError(f"put_if_absent {key!r}") from exc
        except botocore.exceptions.BotoCoreError as exc:
            raise AmbiguousCasError(f"put_if_absent {key!r}: outcome unknown") from exc
        return resp["ETag"].strip('"')

    def put_if_match(self, key: str, body: bytes, etag: str) -> str:
        """Conditional overwrite via `If-Match: <etag>` — the CAS primitive."""
        try:
            resp = self._client.put_object(
                Bucket=self._bucket,
                Key=self._full(key),
                Body=body,
                IfMatch=etag,
            )
        except botocore.exceptions.ClientError as exc:
            self._raise_conditional(exc, key)
            raise ExternalServiceError(f"put_if_match {key!r}") from exc
        except botocore.exceptions.BotoCoreError as exc:
            raise AmbiguousCasError(f"put_if_match {key!r}: outcome unknown") from exc
        return resp["ETag"].strip('"')

    def list_prefix(self, prefix: str) -> list[str]:
        """List keys under a prefix (paginated), relative to the store prefix."""
        keys: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=self._bucket, Prefix=self._full(prefix)):
                for obj in page.get("Contents", []):
                    key = obj.get("Key")
                    if key is not None:
                        keys.append(key.removeprefix(self._prefix))
        except botocore.exceptions.ClientError as exc:
            raise ExternalServiceError(f"list {prefix!r}") from exc
        return keys

    def delete(self, key: str) -> None:
        """Delete an object (idempotent)."""
        try:
            self._client.delete_object(Bucket=self._bucket, Key=self._full(key))
        except botocore.exceptions.ClientError as exc:
            raise ExternalServiceError(f"delete {key!r}") from exc

    @staticmethod
    def _raise_conditional(exc: botocore.exceptions.ClientError, key: str) -> None:
        """Map conditional-write failures; fall through for anything else."""
        code = _error_code(exc)
        status = _status(exc)
        if code == "PreconditionFailed" or status == HTTPStatus.PRECONDITION_FAILED:
            raise PreconditionFailedError(key) from exc
        if code in ("NoSuchKey", "404") or status == HTTPStatus.NOT_FOUND:
            # If-Match against a missing key is 404 on real S3 and moto alike.
            raise ObjectNotFoundError(key) from exc
        if code == "ConditionalRequestConflict" or status == HTTPStatus.CONFLICT:
            raise ConditionalConflictError(key) from exc
        if status >= HTTPStatus.INTERNAL_SERVER_ERROR or code in (
            "RequestTimeout",
            "SlowDown",
        ):
            # The write may have landed server-side; resolve via commit token.
            raise AmbiguousCasError(f"{key!r}: outcome unknown") from exc


def verify_conditional_writes(store: ObjectStore, probe_key: str = "cas-probe") -> None:
    """Prove the store ENFORCES conditional writes; raise if it doesn't.

    Some S3-compatible stores (garage as of 1.3.1, older MinIO) silently
    ACCEPT If-None-Match/If-Match headers without enforcing them — every
    writer "wins" every CAS and the lake corrupts with zero errors. Run
    this once against any new endpoint before trusting it with a lake.
    Leaves no residue (probe object is deleted).
    """
    etag = store.put_if_absent(probe_key, b"probe-1")
    try:
        try:
            store.put_if_absent(probe_key, b"probe-2")
        except PreconditionFailedError:
            pass
        else:
            raise ExternalServiceError(
                "store does not enforce If-None-Match — conditional writes "
                "are silently ignored; this store cannot host a lake"
            )
        store.put_if_match(probe_key, b"probe-3", etag)  # correct etag: must succeed
        try:
            store.put_if_match(probe_key, b"probe-4", etag)  # now stale: must 412
        except PreconditionFailedError:
            pass
        else:
            raise ExternalServiceError(
                "store does not enforce If-Match — conditional writes are "
                "silently ignored; this store cannot host a lake"
            )
    finally:
        store.delete(probe_key)


class InMemoryObjectStore:
    """Deterministic fake for unit and stateful property tests.

    A lock makes the conditional writes genuinely atomic under threads —
    real S3 serializes each request server-side, and the torture tests'
    exactly-once invariant is only meaningful if the fake does too.
    """

    def __init__(self) -> None:
        self._objects: dict[str, tuple[bytes, str]] = {}
        self._etag_counter = 0
        self._lock = threading.Lock()

    def _next_etag(self) -> str:
        self._etag_counter += 1
        return f"etag-{self._etag_counter:08d}"

    def get(self, key: str) -> GetResult:
        """Fetch an object body and its ETag."""
        with self._lock:
            try:
                body, etag = self._objects[key]
            except KeyError:
                raise ObjectNotFoundError(key) from None
            return GetResult(body=body, etag=etag)

    def put_if_absent(self, key: str, body: bytes) -> str:
        """Create-only PUT; fails if the key already exists."""
        with self._lock:
            if key in self._objects:
                raise PreconditionFailedError(key)
            etag = self._next_etag()
            self._objects[key] = (body, etag)
            return etag

    def put_if_match(self, key: str, body: bytes, etag: str) -> str:
        """Conditional overwrite; matches real-S3 404/412 semantics."""
        with self._lock:
            current = self._objects.get(key)
            if current is None:
                # Match real S3: If-Match against a missing key is 404, not 412.
                raise ObjectNotFoundError(key)
            if current[1] != etag:
                raise PreconditionFailedError(key)
            new_etag = self._next_etag()
            self._objects[key] = (body, new_etag)
            return new_etag

    def list_prefix(self, prefix: str) -> list[str]:
        """List keys under a prefix, sorted for determinism."""
        with self._lock:
            return sorted(k for k in self._objects if k.startswith(prefix))

    def delete(self, key: str) -> None:
        """Delete an object (idempotent)."""
        with self._lock:
            self._objects.pop(key, None)
