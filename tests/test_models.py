"""Model invariants: catalog-key derivation is total and round-trips."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ducklake_serverless.errors import InputValidationError
from ducklake_serverless.models import (
    HintDoc,
    RootDoc,
    WriterInfo,
    format_catalog_key,
    format_marker_key,
    parse_catalog_key,
    parse_marker_key,
)


def make_root(generation: int = 0, catalog_uuid: UUID | None = None) -> RootDoc:
    return RootDoc(
        generation=generation,
        catalog_uuid=catalog_uuid or uuid4(),
        duckdb_storage_version="v1.4.0",
        ducklake_format_version="0.3",
        created_at=datetime.now(tz=UTC),
        writer=WriterInfo(lib_version="0.1.0", host="test", pid=1),
    )


@given(gen=st.integers(min_value=0, max_value=10**8 - 1), u=st.uuids())
def test_catalog_key_round_trip(gen: int, u: UUID) -> None:
    assert parse_catalog_key(format_catalog_key(gen, u)) == (gen, u)


@given(gen=st.integers(max_value=-1))
def test_negative_generation_rejected(gen: int) -> None:
    with pytest.raises(InputValidationError):
        format_catalog_key(gen, uuid4())


@pytest.mark.parametrize(
    "bad_key",
    [
        "",
        "root",
        "catalog/cat-1-abc.duckdb",  # unpadded generation
        "catalog/cat-00000001-not-a-uuid.duckdb",
        "data/cat-00000001-00000000-0000-0000-0000-000000000000.duckdb",
        "catalog/cat-00000001-00000000-0000-0000-0000-000000000000.duckdb.wal",
    ],
)
def test_non_canonical_keys_rejected(bad_key: str) -> None:
    with pytest.raises(InputValidationError):
        parse_catalog_key(bad_key)


def test_catalog_key_is_derived_not_stored() -> None:
    doc = make_root(generation=7)
    assert doc.catalog_key == format_catalog_key(7, doc.catalog_uuid)
    # The serialized form must not contain a catalog_key field at all —
    # a stored copy could disagree with (generation, uuid).
    assert b"catalog_key" not in doc.to_json_bytes()


@given(gen=st.integers(min_value=0, max_value=10**8 - 1), u=st.uuids())
def test_root_doc_json_round_trip(gen: int, u: UUID) -> None:
    doc = make_root(generation=gen, catalog_uuid=u)
    restored = RootDoc.from_json_bytes(doc.to_json_bytes())
    assert restored == doc
    assert restored.catalog_key == doc.catalog_key


@given(gen=st.integers(min_value=0, max_value=10**8 - 1))
def test_marker_key_round_trip(gen: int) -> None:
    assert parse_marker_key(format_marker_key(gen)) == gen


@pytest.mark.parametrize(
    "bad_key",
    ["", "roots/1", "roots/00000001/extra", "catalog/00000001", "roots/abcdefgh"],
)
def test_non_canonical_marker_keys_rejected(bad_key: str) -> None:
    with pytest.raises(InputValidationError):
        parse_marker_key(bad_key)


@given(gen=st.integers(max_value=-1))
def test_negative_marker_generation_rejected(gen: int) -> None:
    with pytest.raises(InputValidationError):
        format_marker_key(gen)


def test_root_doc_marker_key_matches_generation() -> None:
    doc = make_root(generation=42)
    assert doc.marker_key == format_marker_key(42)
    assert doc.marker_key == "roots/00000042"


def test_hint_doc_round_trip() -> None:
    doc = HintDoc(generation=7)
    restored = HintDoc.from_json_bytes(doc.to_json_bytes())
    assert restored == doc
    assert restored.generation == 7
    assert b"hint/1" in doc.to_json_bytes()
