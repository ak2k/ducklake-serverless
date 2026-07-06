"""Typed facade over duckdb — the only module that imports it.

duckdb's Python API is loosely typed; per the repo contract, pyright
suppressions for it live here and nowhere else. The facade also owns the
version-probe discipline: the ducklake extension auto-migrates a catalog's
format on ATTACH when versions differ, which would silently rewrite the
lake for every other client — so probes read the catalog as a *plain*
DuckDB file, never through a ducklake ATTACH.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import duckdb

from ducklake_serverless.errors import ExternalServiceError

if TYPE_CHECKING:
    from pathlib import Path

DUCKDB_VERSION: str = duckdb.__version__

# DuckDB files start with a 4KB header block: 8 checksum bytes, then "DUCK".
MAGIC_OFFSET = 8
MAGIC = b"DUCK"


def probe_ducklake_format_version(catalog_path: Path) -> str:
    """Read the DuckLake format version from a catalog file without attaching it.

    Opens the file as a plain DuckDB database (read-only) and reads the
    `ducklake_metadata` table directly — a `ducklake:` ATTACH could trigger
    an in-place format migration, which is exactly what this probe exists
    to prevent.
    """
    try:
        con = duckdb.connect(str(catalog_path), read_only=True)
    except duckdb.Error as exc:
        raise ExternalServiceError(f"cannot open catalog {catalog_path}") from exc
    try:
        row = con.execute(  # pyright: ignore[reportUnknownMemberType]
            "SELECT value FROM ducklake_metadata WHERE key = 'version'"
        ).fetchone()
    except duckdb.Error as exc:
        raise ExternalServiceError(
            f"{catalog_path} is not a DuckLake catalog (no ducklake_metadata)"
        ) from exc
    finally:
        con.close()
    if row is None:
        raise ExternalServiceError(f"{catalog_path}: ducklake_metadata has no version key")
    return str(row[0])  # pyright: ignore[reportAny]  # duckdb rows are untyped; version is TEXT


@dataclass(frozen=True)
class S3Credentials:
    """Connection settings for DuckDB's httpfs when DATA_PATH is s3://.

    Becomes a session-scoped `CREATE SECRET` — never interpolated into
    user SQL, never persisted into the catalog file.
    """

    access_key_id: str
    secret_access_key: str
    endpoint: str | None = None  # host[:port], no scheme
    region: str = "us-east-1"
    use_ssl: bool = True
    url_style: str = "path"  # S3-compatible stores generally need path-style


class LakeConnection:
    """A DuckDB connection with one local DuckLake catalog attached as `lake`."""

    def __init__(
        self,
        catalog_path: Path,
        data_path: str | None,
        *,
        read_only: bool = False,
        s3_credentials: S3Credentials | None = None,
    ) -> None:
        self._con = duckdb.connect()
        try:
            self._con.execute("INSTALL ducklake; LOAD ducklake;")
            if s3_credentials is not None:
                self._create_s3_secret(s3_credentials)
            options = ["READ_ONLY"] if read_only else []
            if data_path is not None:
                options.append(f"DATA_PATH '{data_path}'")
            opts = f" ({', '.join(options)})" if options else ""
            self._con.execute(f"ATTACH 'ducklake:{catalog_path}' AS lake{opts}")
            self._con.execute("USE lake")
        except duckdb.Error as exc:
            self._con.close()
            raise ExternalServiceError(f"attach failed for {catalog_path}") from exc

    def _create_s3_secret(self, creds: S3Credentials) -> None:
        """Install httpfs and register a session-scoped S3 secret."""
        self._con.execute("INSTALL httpfs; LOAD httpfs;")
        clauses = [
            "TYPE s3",
            "KEY_ID ?",
            "SECRET ?",
            "REGION ?",
            f"URL_STYLE '{creds.url_style}'",
            f"USE_SSL {'true' if creds.use_ssl else 'false'}",
        ]
        params: list[str] = [creds.access_key_id, creds.secret_access_key, creds.region]
        if creds.endpoint is not None:
            clauses.append("ENDPOINT ?")
            params.append(creds.endpoint)
        sql = f"CREATE SECRET lake_s3 ({', '.join(clauses)})"
        self._con.execute(sql, params)  # pyright: ignore[reportUnknownMemberType]

    def execute(self, sql: str, params: tuple[object, ...] = ()) -> list[tuple[object, ...]]:
        """Run one statement; returns fetched rows (empty for DML/DDL)."""
        try:
            cursor = self._con.execute(sql, params) if params else self._con.execute(sql)  # pyright: ignore[reportUnknownMemberType]
            return cursor.fetchall()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        except duckdb.Error as exc:
            raise ExternalServiceError(f"statement failed: {sql[:80]}") from exc

    def snapshot_ids(self) -> list[int]:
        """Snapshot ids currently in the catalog, ascending."""
        rows = self.execute("SELECT snapshot_id FROM lake.snapshots() ORDER BY 1")
        return [int(r[0]) for r in rows]  # pyright: ignore[reportArgumentType]

    def close(self) -> None:
        """Detach (checkpointing the catalog file) and close the connection."""
        try:
            self._con.execute("USE memory")
            self._con.execute("DETACH lake")
        except duckdb.Error as exc:
            raise ExternalServiceError("detach failed") from exc
        finally:
            self._con.close()

    def abandon(self) -> None:
        """Close without caring about checkpoint state (error paths only)."""
        self._con.close()
