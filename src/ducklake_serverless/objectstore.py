"""Typed object-store facade — the only module that imports boto3.

Everything above this layer speaks the `ObjectStore` protocol and domain
errors. CAS semantics live here: `put_if_absent` (If-None-Match: *) and
`put_if_match` (If-Match: <etag>) are the primitives the whole commit
protocol rests on.
"""

from __future__ import annotations

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


class InMemoryObjectStore:
    """Deterministic fake for unit and stateful property tests."""

    def __init__(self) -> None:
        self._objects: dict[str, tuple[bytes, str]] = {}
        self._etag_counter = 0

    def _next_etag(self) -> str:
        self._etag_counter += 1
        return f"etag-{self._etag_counter:08d}"

    def get(self, key: str) -> GetResult:
        """Fetch an object body and its ETag."""
        try:
            body, etag = self._objects[key]
        except KeyError:
            raise ObjectNotFoundError(key) from None
        return GetResult(body=body, etag=etag)

    def put_if_absent(self, key: str, body: bytes) -> str:
        """Create-only PUT; fails if the key already exists."""
        if key in self._objects:
            raise PreconditionFailedError(key)
        etag = self._next_etag()
        self._objects[key] = (body, etag)
        return etag

    def put_if_match(self, key: str, body: bytes, etag: str) -> str:
        """Conditional overwrite; matches real-S3 404/412 semantics."""
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
        return sorted(k for k in self._objects if k.startswith(prefix))

    def delete(self, key: str) -> None:
        """Delete an object (idempotent)."""
        self._objects.pop(key, None)
