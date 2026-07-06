"""Statement classification: the input to every rebase decision.

Classification must be conservative — an unrecognizable statement is
STATE_DEPENDENT_DML (abort on conflict), never BLIND_APPEND. A false
"blind append" silently replays SQL whose meaning depended on state the
writer never observed; a false "state-dependent" merely costs a retry.

Volatile functions (now(), random(), …) are rejected at record time:
their replay would produce different values than the original execution,
so callers must bind such values as parameters instead.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

if TYPE_CHECKING:
    from sqlglot.expressions import (
        Expr,  # pyright: ignore[reportPrivateImportUsage]  # canonical node type; sqlglot omits it from __all__
    )

from ducklake_serverless.errors import InputValidationError
from ducklake_serverless.models import Statement, StatementClass

# Function names whose value changes between executions. Lowercase.
VOLATILE_FUNCTIONS = frozenset(
    {
        "now",
        "current_timestamp",
        "current_date",
        "current_time",
        "current_localtime",
        "current_localtimestamp",
        "today",
        "random",
        "rand",
        "uuid",
        "gen_random_uuid",
        "uuidv4",
        "uuidv7",
        "nextval",
    }
)

# Table functions that read sources OUTSIDE the lake (staged files). An
# INSERT…SELECT over only these is still a blind append.
_NON_LAKE_SOURCES = frozenset(
    {
        "read_parquet",
        "read_csv",
        "read_csv_auto",
        "read_json",
        "read_json_auto",
        "parquet_scan",
    }
)


def _function_names(tree: Expr) -> set[str]:
    names: set[str] = set()
    for func in tree.find_all(exp.Func):
        if isinstance(func, exp.Anonymous):
            names.add(func.name.lower())
        else:
            names.add(func.sql_name().lower())
    return names


def _reads_lake_tables(insert: exp.Insert) -> bool:
    """Whether an INSERT's source references any lake table.

    The insert target is a table too — skip it; every OTHER table node is a
    read. Table functions (read_parquet etc.) parse as functions, not
    tables, so they never appear here.
    """
    target = insert.this  # pyright: ignore[reportAny]  # sqlglot nodes are untyped
    target_tables: set[int] = {
        id(t)  # pyright: ignore[reportAny]  # sqlglot nodes are untyped
        for t in (target.find_all(exp.Table) if target is not None else ())  # pyright: ignore[reportAny]
    }
    if isinstance(target, exp.Table):
        target_tables.add(id(target))
    return any(t.name != "" and id(t) not in target_tables for t in insert.find_all(exp.Table))


def classify(sql: str) -> StatementClass:  # noqa: PLR0911  # decision table: one return per class
    """Classify one statement for replay safety. Conservative by design."""
    try:
        trees = sqlglot.parse(sql, read="duckdb")
    except ParseError as exc:
        raise InputValidationError(f"cannot parse statement: {sql[:80]}") from exc
    if len(trees) != 1 or trees[0] is None:
        raise InputValidationError(
            "exactly one statement per sql() call — split multi-statement strings"
        )
    tree: Expr = trees[0]

    volatile = _function_names(tree) & VOLATILE_FUNCTIONS
    if volatile:
        raise InputValidationError(
            f"volatile function(s) {sorted(volatile)} are not replayable — "
            "bind the value as a parameter instead"
        )

    if isinstance(tree, (exp.Create, exp.Drop, exp.Alter, exp.TruncateTable, exp.Comment)):
        return StatementClass.DDL
    if isinstance(tree, (exp.Update, exp.Delete, exp.Merge)):
        return StatementClass.STATE_DEPENDENT_DML
    if isinstance(tree, exp.Insert):
        if _reads_lake_tables(tree):
            return StatementClass.STATE_DEPENDENT_DML
        func_names = _function_names(tree)
        unknown_table_funcs = func_names - _NON_LAKE_SOURCES
        # Pure VALUES or SELECT over staged files only: safe to replay.
        if not unknown_table_funcs or all(
            not name.startswith("read_") for name in unknown_table_funcs
        ):
            return StatementClass.BLIND_APPEND
        return StatementClass.STATE_DEPENDENT_DML
    if isinstance(tree, exp.Select):
        return StatementClass.READ
    # Anything unrecognized: the conservative bucket.
    return StatementClass.STATE_DEPENDENT_DML


def record(sql: str, params: tuple[object, ...] = ()) -> Statement:
    """Classify and freeze one statement for the changeset."""
    return Statement(sql=sql, params=params, statement_class=classify(sql))
