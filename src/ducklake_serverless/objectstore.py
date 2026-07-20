"""Typed object-store facade — the only module that imports boto3.

Everything above this layer speaks the `ObjectStore` protocol and domain
errors. CAS semantics live here: `put_if_absent` (If-None-Match: *) and
`put_if_match` (If-Match: <etag>) are the primitives the whole commit
protocol rests on.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import UTC, datetime
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
    from collections.abc import Callable

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
    """An object's body, its ETag, and the store's write timestamp.

    `last_modified` is the STORE's clock (S3 LastModified), not any
    writer's — the one timestamp all participants can agree on. None when
    a backend doesn't provide it.
    """

    body: bytes
    etag: str
    last_modified: datetime | None = None


@dataclass(frozen=True)
class ObjectMeta:
    """An object's metadata without its body — one HEAD or one listing row.

    `last_modified` is the STORE's clock (see `GetResult`). The pack GC's
    age gate is a cross-clock comparison, so it must only ever compare
    these timestamps against other store-issued timestamps, never against
    the runner's clock.
    """

    key: str
    size: int
    last_modified: datetime | None = None


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

    def put(self, key: str, body: bytes) -> str:
        """Unconditional PUT (last-writer-wins): new ETag. For advisory objects only."""
        ...

    def list_prefix(self, prefix: str) -> list[str]:
        """List keys under a prefix."""
        ...

    def list_meta(self, prefix: str) -> list[ObjectMeta]:
        """List objects under a prefix with size + store-clock mtime."""
        ...

    def head_meta(self, key: str) -> ObjectMeta:
        """One object's metadata without its body; ObjectNotFoundError if absent."""
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
        return GetResult(
            body=resp["Body"].read(),
            etag=resp["ETag"].strip('"'),
            last_modified=resp.get("LastModified"),
        )

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

    def put(self, key: str, body: bytes) -> str:
        """Unconditional PUT — advisory objects (the hint) only.

        No conditional header, so no ambiguity to resolve: on a transport
        failure the caller simply moves on (the hint is best-effort).
        """
        try:
            resp = self._client.put_object(Bucket=self._bucket, Key=self._full(key), Body=body)
        except botocore.exceptions.ClientError as exc:
            raise ExternalServiceError(f"put {key!r}") from exc
        except botocore.exceptions.BotoCoreError as exc:
            raise ExternalServiceError(f"put {key!r}: transport failure") from exc
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

    def list_meta(self, prefix: str) -> list[ObjectMeta]:
        """List objects under a prefix with size + LastModified (store clock)."""
        metas: list[ObjectMeta] = []
        paginator = self._client.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=self._bucket, Prefix=self._full(prefix)):
                for obj in page.get("Contents", []):
                    key = obj.get("Key")
                    if key is not None:
                        metas.append(
                            ObjectMeta(
                                key=key.removeprefix(self._prefix),
                                size=int(obj.get("Size", 0)),
                                last_modified=obj.get("LastModified"),
                            )
                        )
        except botocore.exceptions.ClientError as exc:
            raise ExternalServiceError(f"list_meta {prefix!r}") from exc
        return metas

    def delete(self, key: str) -> None:
        """Delete an object (idempotent)."""
        try:
            self._client.delete_object(Bucket=self._bucket, Key=self._full(key))
        except botocore.exceptions.ClientError as exc:
            raise ExternalServiceError(f"delete {key!r}") from exc

    def head_meta(self, key: str) -> ObjectMeta:
        """Object metadata via a HEAD — no body transfer.

        Serves the streaming reader's `auto` size heuristic and the pack GC's
        pre-delete age recheck.
        """
        try:
            resp = self._client.head_object(Bucket=self._bucket, Key=self._full(key))
        except botocore.exceptions.ClientError as exc:
            if _error_code(exc) in ("NoSuchKey", "404") or _status(exc) == HTTPStatus.NOT_FOUND:
                raise ObjectNotFoundError(key) from exc
            raise ExternalServiceError(f"head {key!r}") from exc
        return ObjectMeta(
            key=key,
            size=int(resp["ContentLength"]),
            last_modified=resp.get("LastModified"),
        )

    def s3_uri(self, key: str) -> str:
        """The `s3://…` URI DuckDB httpfs can ATTACH directly (streaming reads)."""
        return f"s3://{self._bucket}/{self._full(key)}"

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


@dataclass(frozen=True)
class Capabilities:
    """Which conditional-write primitives a store enforces ATOMICALLY.

    Sequential enforcement is necessary but not sufficient: iDrive E2, for
    example, enforces If-Match sequentially AND under concurrency, but
    enforces If-None-Match only sequentially — concurrent create-only PUTs
    all "win" (last-writer-wins), silently losing commits. Only atomic-
    under-concurrency primitives are safe to serialize a lake on.
    """

    atomic_create: bool  # If-None-Match: * exclusive under concurrent PUTs
    atomic_cas: bool  # If-Match: <etag> exclusive under concurrent PUTs

    @property
    def can_host_lake(self) -> bool:
        """Whether a marker-protocol lake can serialize commits on this store.

        The commit path serializes on create-only (`If-None-Match: *`), so
        atomic create is what a lake needs — a CAS-only backend cannot host it.
        `atomic_cas` is measured as a seam for a possible future CAS-based
        fallback but is not load-bearing today.
        """
        return self.atomic_create


def _count_concurrent_winners(racers: int, attempt: Callable[[int], object]) -> int:
    """Fire `racers` barrier-synced `attempt(writer_id)` calls; count winners.

    `attempt` must raise PreconditionFailedError/ConditionalConflictError when
    it definitively LOST the race, and return normally when it won. Any other
    exception — including AmbiguousCasError, whose landing is unknown — makes
    the whole measurement untrustworthy and is re-raised after all threads
    join: a safety gate must fail loud, never certify atomicity from a winner
    count distorted by a swallowed error.
    """
    barrier = threading.Barrier(racers)
    wins: list[int] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    def run(writer_id: int) -> None:
        barrier.wait()  # release all at once to maximize contention
        try:
            attempt(writer_id)
        except (PreconditionFailedError, ConditionalConflictError):
            return  # definitively lost — not a winner
        except Exception as exc:  # noqa: BLE001  # AmbiguousCas/transport/etc — surfaced below
            with lock:
                errors.append(exc)
            return
        with lock:
            wins.append(writer_id)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(racers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise ExternalServiceError(
            f"capability probe indeterminate: {len(errors)} of {racers} contenders "
            f"failed with an ambiguous or unexpected error ({errors[0]!r})"
        )
    return len(wins)


def _count_concurrent_create_winners(store: ObjectStore, key: str, racers: int) -> int:
    """How many of `racers` simultaneous create-only PUTs to `key` succeed."""
    return _count_concurrent_winners(
        racers, lambda writer_id: store.put_if_absent(key, f"w{writer_id}".encode())
    )


def _count_concurrent_cas_winners(store: ObjectStore, key: str, racers: int) -> int:
    """How many of `racers` simultaneous If-Match PUTs (same base etag) succeed."""
    etag = store.put_if_absent(key, b"seed")
    return _count_concurrent_winners(
        racers, lambda writer_id: store.put_if_match(key, f"w{writer_id}".encode(), etag)
    )


def probe_atomic_create(store: ObjectStore, *, racers: int = 6, prefix: str = "cap-probe") -> bool:
    """Whether create-only (`If-None-Match: *`) is atomic under concurrency.

    This is the single primitive a marker-protocol lake needs, so bootstrap
    gates on it directly. Cheaper than `probe_capabilities`: one contention
    round (no CAS seed + race), reserving the full probe for the diagnostic.
    """
    import uuid  # noqa: PLC0415  # only needed for a unique probe key

    create_key = f"{prefix}/create-{uuid.uuid4()}"
    try:
        return _count_concurrent_create_winners(store, create_key, racers) == 1
    finally:
        store.delete(create_key)


def probe_capabilities(
    store: ObjectStore, *, racers: int = 6, prefix: str = "cap-probe"
) -> Capabilities:
    """Measure which primitives the store enforces atomically UNDER CONCURRENCY.

    Runs `racers` barrier-synchronized contenders against a single key for
    each primitive; exactly one winner means atomic, more than one means the
    store serializes that primitive as last-writer-wins (unsafe). Self-
    cleaning. This is what `verify_conditional_writes` should have checked —
    the sequential probe misses stores whose enforcement collapses under
    real concurrency (iDrive E2's If-None-Match).
    """
    import uuid  # noqa: PLC0415  # only needed for unique probe keys

    create_key = f"{prefix}/create-{uuid.uuid4()}"
    cas_key = f"{prefix}/cas-{uuid.uuid4()}"
    try:
        atomic_create = _count_concurrent_create_winners(store, create_key, racers) == 1
        atomic_cas = _count_concurrent_cas_winners(store, cas_key, racers) == 1
    finally:
        store.delete(create_key)
        store.delete(cas_key)
    return Capabilities(atomic_create=atomic_create, atomic_cas=atomic_cas)


class InMemoryObjectStore:
    """Deterministic fake for unit and stateful property tests.

    A lock makes the conditional writes genuinely atomic under threads —
    real S3 serializes each request server-side, and the torture tests'
    exactly-once invariant is only meaningful if the fake does too.
    """

    def __init__(self) -> None:
        self._objects: dict[str, tuple[bytes, str, datetime]] = {}
        self._etag_counter = 0
        self._lock = threading.Lock()

    def _next_etag(self) -> str:
        self._etag_counter += 1
        return f"etag-{self._etag_counter:08d}"

    def get(self, key: str) -> GetResult:
        """Fetch an object body and its ETag."""
        with self._lock:
            try:
                body, etag, written = self._objects[key]
            except KeyError:
                raise ObjectNotFoundError(key) from None
            return GetResult(body=body, etag=etag, last_modified=written)

    def put_if_absent(self, key: str, body: bytes) -> str:
        """Create-only PUT; fails if the key already exists."""
        with self._lock:
            if key in self._objects:
                raise PreconditionFailedError(key)
            etag = self._next_etag()
            self._objects[key] = (body, etag, datetime.now(tz=UTC))
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
            self._objects[key] = (body, new_etag, datetime.now(tz=UTC))
            return new_etag

    def put(self, key: str, body: bytes) -> str:
        """Unconditional overwrite (last-writer-wins)."""
        with self._lock:
            etag = self._next_etag()
            self._objects[key] = (body, etag, datetime.now(tz=UTC))
            return etag

    def list_prefix(self, prefix: str) -> list[str]:
        """List keys under a prefix, sorted for determinism."""
        with self._lock:
            return sorted(k for k in self._objects if k.startswith(prefix))

    def list_meta(self, prefix: str) -> list[ObjectMeta]:
        """List objects under a prefix with size + write time, key-sorted."""
        with self._lock:
            return [
                ObjectMeta(key=k, size=len(body), last_modified=written)
                for k, (body, _, written) in sorted(self._objects.items())
                if k.startswith(prefix)
            ]

    def head_meta(self, key: str) -> ObjectMeta:
        """Object metadata without its body; ObjectNotFoundError if absent."""
        with self._lock:
            try:
                body, _, written = self._objects[key]
            except KeyError:
                raise ObjectNotFoundError(key) from None
            return ObjectMeta(key=key, size=len(body), last_modified=written)

    def delete(self, key: str) -> None:
        """Delete an object (idempotent)."""
        with self._lock:
            self._objects.pop(key, None)
