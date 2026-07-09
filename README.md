# ducklake-serverless

**A serverless, versioned, ACID object store on plain S3 — no metadata server.**
Any payload (a file, a DuckDB/SQLite database, an arbitrary artifact) gets
atomic compare-and-swap versioning, time travel, and concurrent-safe commits,
using only S3 conditional writes. [DuckLake](https://ducklake.select/) is the
flagship adapter: multi-writer DuckLake with the whole lakehouse — catalog
included — living in one bucket.

> Status: early development, never deployed. Commit protocol, transaction
> envelope, rebase-on-conflict, GC, and DuckLake data-plane maintenance are
> implemented and tested — hermetically, and against real MinIO and SeaweedFS
> backends in CI. Not affiliated with DuckDB Labs or the DuckLake project.

## The idea

A mutable, versioned artifact lives entirely in an object-storage prefix — no
Postgres, no lock service, no sidecar:

- Each generation's **bytes are one immutable object** at `payload/<gen>-<uuid>`.
- Each commit **creates one immutable generation marker** `roots/<gen>` — a
  create-only PUT (`If-None-Match: *`) whose body names that generation's
  payload. Exactly one writer wins each generation; the marker is never
  overwritten or deleted. A tiny mutable `root-hint` points at roughly the
  latest generation, purely to save readers a few probes.
- Commits are **create-only compare-and-swap**: stage the payload, then create
  the next marker. An ambiguous outcome (a timeout) is resolved by one GET of
  the marker you tried to create — *exact and permanent*, so "did my commit
  land?" is never the caller's problem to reconcile.

The serialization point is the S3 conditional write itself. **AWS S3 (since
2024), MinIO, and SeaweedFS are verified — empirically raced in CI — to enforce
it atomically under concurrency.** GCS, Azure, and R2 document the primitive but
have not been raced here, so probe them before you trust an endpoint. Writers on
an atomic backend can be Lambdas.

**Verify your endpoint before trusting it**: some S3-compatible stores accept
`If-None-Match` without enforcing it, and some enforce it only sequentially
(fine for one writer, silently lossy under concurrent ones) — either would
corrupt a store with zero errors. See the
[live-tested compatibility table](docs/compatibility.md) (re-verified weekly in
CI). `probe_capabilities(store)` races concurrent writers to check *atomic*
enforcement, and `bootstrap()` gates on it — refusing any backend whose
create-only isn't atomic under concurrency (pass `verify_backend=False` for a
deliberately single-writer store).

## Architecture: engine + adapters

The commit/marker/CAS/lease/GC machinery is **payload-agnostic** — it moves
bytes and never imports `duckdb`. A `Payload`/`CommitContext` adapter teaches
it the few payload-specific things it needs (pre-publish validation, version
pins, and — for a mergeable payload — how to rebase). This split is enforced by
a test that imports every engine-core module in a fresh interpreter and asserts
`duckdb` was never pulled in.

- **`BlobStore`** — the general-purpose adapter. Any bytes, versioned. A lost
  commit race aborts (a blob can't be merged); re-read and re-write.
- **`Lake`** (DuckLake) — the flagship adapter. Full DuckLake catalog with SQL
  transactions, blind-append rebase-on-conflict, and DuckDB/DuckLake version
  pinning. Each generation is a complete, stock DuckLake catalog readable by any
  DuckLake-aware tool via `ATTACH 'ducklake:…' (READ_ONLY)`.

## Usage — BlobStore (any payload)

```python
from pathlib import Path
from ducklake_serverless.objectstore import S3ObjectStore, make_s3_client
from ducklake_serverless.blob import BlobStore

client = make_s3_client(endpoint_url="https://<s3-compatible-endpoint>")
store = S3ObjectStore(client, "my-bucket", prefix="artifact")
bs = BlobStore(store, workdir=Path("/tmp/blob-work"))

bs.bootstrap(b"v0")            # once: generation 0
bs.write(b"v1")                # commit the next generation (aborts on a lost race)
assert bs.read() == b"v1"      # current bytes
bs.head().generation           # 1
```

## Usage — DuckLake

```python
from pathlib import Path
from ducklake_serverless.objectstore import S3ObjectStore, make_s3_client
from ducklake_serverless.session import Lake

client = make_s3_client(endpoint_url="https://<s3-compatible-endpoint>")
store = S3ObjectStore(client, "my-bucket", prefix="lake")
lake = Lake(store, workdir=Path("/tmp/lake-work"), data_path="s3://my-bucket/lake/data")

lake.bootstrap()                       # once: generation 0 + its marker

with lake.transaction() as tx:         # concurrent writers just do this
    tx.sql("CREATE TABLE events (id INTEGER, msg VARCHAR)")
with lake.transaction() as tx:
    tx.sql("INSERT INTO events VALUES (?, ?)", (1, "hello"))

with lake.reader() as con:             # stock frozen-DuckLake attach
    print(con.execute("SELECT * FROM events"))
```

DuckLake concurrency (optimistic, Delta/Iceberg-style): **blind appends** rebase
and retry automatically; **state-dependent DML** (`UPDATE`/`DELETE`/lake-reading
inserts) aborts on a lost race (re-executing it against unobserved state is
write skew, so replay is opt-in via `replay_all`); **DDL** always aborts.

Always create S3 clients with `make_s3_client` — it disables botocore's
transport retries so the create-only resolution path stays simple.

## Development

```bash
uv sync
make check   # ruff + basedpyright strict + pytest (moto CAS conformance included)
```

See `AGENTS.md` for the contribution contract and protocol invariants, and
[`docs/ROADMAP.md`](docs/ROADMAP.md) for what's planned and deliberately deferred.
