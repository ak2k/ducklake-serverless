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
- Each commit **creates one immutable generation marker** `roots/<generation>`
  (Delta-log-shaped) — a create-only PUT (`If-None-Match: *`) whose body names
  that generation's catalog. Exactly one writer wins each generation; the
  marker is never overwritten and never deleted. A tiny mutable `root-hint`
  points at roughly the latest generation, purely to save readers a few probes.
- Commits are **create-only compare-and-swap**: stage everything (Parquet +
  next catalog), then create the next marker. Losers rebase onto the current
  head and retry. Crucially, an ambiguous outcome (a timeout) is resolved by
  one GET of the marker you tried to create — *exact and permanent*, so "did my
  commit land?" never becomes the caller's problem to reconcile.
- Readers need zero custom code: resolve the head marker, then
  `ATTACH 'ducklake:…' (READ_ONLY)` — the official frozen-DuckLake pattern.

No sidecar, no epoch holder, no failover story, no lock service. Writers can
be Lambdas. The serialization point is the S3 conditional write itself —
supported by AWS S3 (since 2024), GCS, Azure, R2, MinIO, and iDrive E2
(verified empirically). Because commits are create-only, they depend only on
`If-None-Match` — the more widely-implemented half of the primitive.

**Verify your endpoint before trusting it**: some S3-compatible stores
accept `If-Match`/`If-None-Match` headers without enforcing them, and some
enforce them only sequentially (fine for one writer, silently lossy under
concurrent ones) — either would corrupt a lake with zero errors. See the
[live-tested compatibility table](docs/compatibility.md) — re-verified
weekly in CI and on every backend version bump, so it cannot go stale.
`verify_conditional_writes(store)` checks sequential enforcement in one
round-trip (the integration lane runs it automatically);
`probe_capabilities(store)` races concurrent writers to check *atomic*
enforcement, and `Lake.bootstrap()` gates on it — refusing any backend
whose create-only isn't atomic under concurrency.

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

Each marker also pins the DuckDB storage version and DuckLake format version:
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

lake.bootstrap()                       # once, creates generation 0 + its marker

with lake.transaction() as tx:         # concurrent writers just do this
    tx.sql("CREATE TABLE events (id INTEGER, msg VARCHAR)")

with lake.transaction() as tx:
    tx.sql("INSERT INTO events VALUES (?, ?)", (1, "hello"))

with lake.reader() as con:             # readers: stock frozen-DuckLake attach
    print(con.execute("SELECT * FROM events"))
```

Always create S3 clients with `make_s3_client` — it disables botocore's
transport retries. With immutable markers a self-412 from an SDK retry
resolves cleanly as WON, but disabling retries keeps the resolution path
simple.

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
