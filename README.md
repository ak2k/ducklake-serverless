# ducklake-serverless

**Multi-writer [DuckLake](https://ducklake.select/) with no catalog server — the
entire lakehouse, catalog included, lives in one object-storage bucket.**

> Status: early development. The commit protocol core (P0) exists; the
> transaction envelope, rebase, and GC are being built. Not affiliated with
> DuckDB Labs or the DuckLake project.

## The idea

DuckLake's design puts lakehouse metadata in a SQL database. That database is
the one component that needs a running server — Postgres for multi-writer, or
a local DuckDB file for single-user. This library removes the server:

- The **entire catalog is an immutable, versioned DuckDB file** in the bucket
  (`catalog/cat-<generation>-<uuid>.duckdb`) — each generation is a complete,
  stock DuckLake catalog readable by any DuckLake-aware tool.
- A single tiny **root object** points at the current generation. It is the
  only mutable key in the lake.
- Commits are **compare-and-swap**: prepare everything (Parquet data files +
  the next catalog generation), then publish with one conditional PUT
  (`If-Match: <etag>`). Exactly one concurrent writer wins; losers rebase and
  retry.
- Readers need zero custom code: resolve the root, then
  `ATTACH 'ducklake:…' (READ_ONLY)` — the official frozen-DuckLake pattern.

No sidecar, no epoch holder, no failover story, no lock service. Writers can
be Lambdas. The serialization point is the S3 conditional write itself —
supported by AWS S3 (since 2024), GCS, Azure, R2, MinIO, and iDrive E2
(verified empirically).

## Concurrency semantics

Same deal as Delta Lake and Iceberg's optimistic concurrency, applied to the
DuckLake format:

- **Blind appends** (`INSERT … VALUES`, `INSERT … SELECT` over non-lake
  sources such as staged Parquet) rebase and retry automatically — concurrent
  appenders never see conflicts.
- **State-dependent DML** (`UPDATE`, `DELETE`, lake-reading inserts) aborts
  cleanly when it loses a race; the application re-reads and re-decides.
  Re-executing such SQL against state the writer never observed is write skew,
  so it is opt-in (`replay_all`), never the default.
- **DDL** conflicts always abort. Run migrations from one place.

The root doc also pins the DuckDB storage version and DuckLake format version:
a writer with mismatched local versions refuses to commit rather than silently
auto-migrating the catalog for the whole fleet. Upgrades are explicit.

## Development

```bash
uv sync
make check   # ruff + basedpyright strict + pytest (moto CAS conformance included)
```

The test suite includes conformance guards asserting that the S3 fake (moto)
actually enforces conditional-write semantics — if those regress, concurrency
tests fail loudly instead of becoming vacuous.

See `AGENTS.md` for the contribution contract and the protocol invariants
that must not be weakened.
