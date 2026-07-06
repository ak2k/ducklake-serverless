"""S3ObjectStore against moto, including the CAS conformance guard.

The conformance tests assert that moto itself enforces conditional-write
semantics (If-None-Match:* and If-Match both 412 correctly). If a moto
regression made conditional PUTs unconditionally succeed, every
concurrency test in this suite would silently stop testing anything —
these guards fail loudly instead. Requires moto>=5.1.5 (PutObject If-Match
support).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, override

import boto3
import pytest
from moto import mock_aws

from ducklake_serverless.errors import (
    ExternalServiceError,
    ObjectNotFoundError,
    PreconditionFailedError,
)
from ducklake_serverless.objectstore import (
    InMemoryObjectStore,
    S3ObjectStore,
    verify_conditional_writes,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from mypy_boto3_s3.client import S3Client

BUCKET = "test-lake"


@pytest.fixture
def store() -> Iterator[S3ObjectStore]:
    with mock_aws():
        client: S3Client = boto3.client("s3", region_name="us-east-1")  # pyright: ignore[reportUnknownMemberType]  # boto3.client factory is untyped; S3Client annotation restores typing
        client.create_bucket(Bucket=BUCKET)
        yield S3ObjectStore(client, BUCKET, prefix="lake")


def test_get_missing_raises(store: S3ObjectStore) -> None:
    with pytest.raises(ObjectNotFoundError):
        store.get("nope")


def test_put_get_round_trip(store: S3ObjectStore) -> None:
    etag = store.put_if_absent("root", b"v1")
    result = store.get("root")
    assert result.body == b"v1"
    assert result.etag == etag


def test_conformance_if_none_match_412_on_existing(store: S3ObjectStore) -> None:
    store.put_if_absent("root", b"v1")
    with pytest.raises(PreconditionFailedError):
        store.put_if_absent("root", b"v2")
    assert store.get("root").body == b"v1"


def test_conformance_if_match_swaps_on_current_etag(store: S3ObjectStore) -> None:
    etag1 = store.put_if_absent("root", b"v1")
    etag2 = store.put_if_match("root", b"v2", etag1)
    assert etag2 != etag1
    assert store.get("root").body == b"v2"


def test_conformance_if_match_412_on_stale_etag(store: S3ObjectStore) -> None:
    etag1 = store.put_if_absent("root", b"v1")
    store.put_if_match("root", b"v2", etag1)
    with pytest.raises(PreconditionFailedError):
        store.put_if_match("root", b"v3", etag1)
    assert store.get("root").body == b"v2"


def test_conformance_if_match_on_missing_key_is_not_found(store: S3ObjectStore) -> None:
    """Real S3 (and moto) return 404, not 412, for If-Match on a missing key.

    For this protocol a vanished root is a distinct alarm — the lake was
    deleted or never bootstrapped — so it must not masquerade as an
    ordinary lost race.
    """
    with pytest.raises(ObjectNotFoundError):
        store.put_if_match("never-created", b"v1", "etag-of-nothing")


def test_list_prefix_scoped(store: S3ObjectStore) -> None:
    store.put_if_absent("catalog/cat-a.duckdb", b"a")
    store.put_if_absent("catalog/cat-b.duckdb", b"b")
    store.put_if_absent("data/f.parquet", b"d")
    assert store.list_prefix("catalog/") == [
        "catalog/cat-a.duckdb",
        "catalog/cat-b.duckdb",
    ]


def test_delete_then_get_raises(store: S3ObjectStore) -> None:
    store.put_if_absent("tmp", b"x")
    store.delete("tmp")
    with pytest.raises(ObjectNotFoundError):
        store.get("tmp")


def test_verify_conditional_writes_passes_on_conformant_store(store: S3ObjectStore) -> None:
    verify_conditional_writes(store)  # must not raise; leaves no residue
    with pytest.raises(ObjectNotFoundError):
        store.get("cas-probe")


def test_verify_conditional_writes_rejects_ignoring_store() -> None:
    """A store that accepts-but-ignores conditional headers (garage 1.3.1)

    must be rejected loudly — it would corrupt a lake with zero errors.
    """

    class IgnoresConditionals(InMemoryObjectStore):
        @override
        def put_if_absent(self, key: str, body: bytes) -> str:
            with self._lock:  # pyright: ignore[reportPrivateUsage]
                etag = self._next_etag()  # pyright: ignore[reportPrivateUsage]
                self._objects[key] = (body, etag)  # pyright: ignore[reportPrivateUsage]
                return etag

        @override
        def put_if_match(self, key: str, body: bytes, etag: str) -> str:
            with self._lock:  # pyright: ignore[reportPrivateUsage]
                new_etag = self._next_etag()  # pyright: ignore[reportPrivateUsage]
                self._objects[key] = (body, new_etag)  # pyright: ignore[reportPrivateUsage]
                return new_etag

    with pytest.raises(ExternalServiceError, match="does not enforce"):
        verify_conditional_writes(IgnoresConditionals())
