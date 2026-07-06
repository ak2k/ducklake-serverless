# ducklake-serverless

**Multi-writer [DuckLake](https://ducklake.select/) with no catalog server — the
entire lakehouse, catalog included, lives in one object-storage bucket.**

> Status: early development, functionally complete. Commit protocol,
> transaction envelope, rebase-on-conflict, catalog GC, and data-plane
> maintenance (snapshot expiry + orphan-Parquet cleanup) are implemented
> and tested — hermetically, against MinIO and SeaweedFS in CI, and live
> against iDrive E2. Not affiliated with DuckDB Labs or the DuckLake
> project.

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

**Verify your endpoint before trusting it**: some S3-compatible stores
accept `If-Match`/`If-None-Match` headers without enforcing them (garage
1.3.1 does — every conditional PUT "succeeds"), which would corrupt a lake
with zero errors. `verify_conditional_writes(store)` probes this in one
round-trip; the integration and live test lanes run it automatically.

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

## Usage

```python
from pathlib import Path
from ducklake_serverless.objectstore import S3ObjectStore, make_s3_client
from ducklake_serverless.session import Lake

client = make_s3_client(endpoint_url="https://<s3-compatible-endpoint>")
store = S3ObjectStore(client, "my-bucket", prefix="lake")
lake = Lake(store, workdir=Path("/tmp/lake-work"), data_path="s3://my-bucket/lake/data")

lake.bootstrap()                       # once, creates generation 0 + root

with lake.transaction() as tx:         # concurrent writers just do this
    tx.sql("CREATE TABLE events (id INTEGER, msg VARCHAR)")

with lake.transaction() as tx:
    tx.sql("INSERT INTO events VALUES (?, ?)", (1, "hello"))

with lake.reader() as con:             # readers: stock frozen-DuckLake attach
    print(con.execute("SELECT * FROM events"))
```

Always create S3 clients with `make_s3_client` — it disables botocore's
transport retries, which would otherwise silently re-send conditional PUTs
and corrupt the commit protocol's conflict detection.

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
