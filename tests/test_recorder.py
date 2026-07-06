"""Recorder classification: conservative by construction."""

from __future__ import annotations

import pytest

from ducklake_serverless.errors import InputValidationError
from ducklake_serverless.models import StatementClass
from ducklake_serverless.recorder import classify


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO events VALUES (1, 'x')",
        "INSERT INTO events VALUES (?, ?)",
        "INSERT INTO events SELECT * FROM read_parquet('staged.parquet')",
        "INSERT INTO events SELECT a, b FROM read_csv('staged.csv')",
        "INSERT INTO events (id, msg) VALUES (1, 'x'), (2, 'y')",
    ],
)
def test_blind_appends(sql: str) -> None:
    assert classify(sql) is StatementClass.BLIND_APPEND


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE t SET v = 1 WHERE id = 2",
        "DELETE FROM t WHERE id = 2",
        "INSERT INTO t SELECT * FROM other_table",
        "INSERT INTO t SELECT a FROM read_csv('f.csv') JOIN lake_tbl USING (id)",
        "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v",
    ],
)
def test_state_dependent_dml(sql: str) -> None:
    assert classify(sql) is StatementClass.STATE_DEPENDENT_DML


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE t (v INTEGER)",
        "DROP TABLE t",
        "ALTER TABLE t ADD COLUMN w INTEGER",
    ],
)
def test_ddl(sql: str) -> None:
    assert classify(sql) is StatementClass.DDL


def test_read(sql: str = "SELECT * FROM t") -> None:
    assert classify(sql) is StatementClass.READ


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (now())",
        "INSERT INTO t VALUES (random())",
        "INSERT INTO t VALUES (uuid())",
        "INSERT INTO t SELECT current_timestamp",
        "UPDATE t SET v = random()",
    ],
)
def test_volatile_rejected(sql: str) -> None:
    with pytest.raises(InputValidationError, match="volatile"):
        classify(sql)


def test_multi_statement_rejected() -> None:
    with pytest.raises(InputValidationError, match="one statement"):
        classify("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)")


def test_unparseable_rejected() -> None:
    with pytest.raises(InputValidationError, match="parse"):
        classify("THIS IS NOT SQL AT (((")


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT OR REPLACE INTO t VALUES (1)",
        "INSERT OR IGNORE INTO t VALUES (1)",
        "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING",
        "INSERT INTO t VALUES (1) ON CONFLICT DO UPDATE SET v = 2",
        "INSERT INTO t VALUES (1) RETURNING *",
        "INSERT INTO t SELECT * FROM postgres_scan('h', 'd', 't')",
        "INSERT INTO t SELECT * FROM sqlite_scan('f.db', 't')",
    ],
)
def test_upserts_and_external_scans_are_state_dependent(sql: str) -> None:
    """Upsert shapes read existing rows to decide writes; external scans

    re-read live sources on replay. Neither is a blind append.
    """
    assert classify(sql) is StatementClass.STATE_DEPENDENT_DML


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (get_current_timestamp())",
        "INSERT INTO t VALUES (transaction_timestamp())",
        "INSERT INTO t SELECT getenv('HOME')",
    ],
)
def test_volatile_aliases_rejected(sql: str) -> None:
    with pytest.raises(InputValidationError, match="volatile"):
        classify(sql)
