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

Large payloads commit as **content-addressed chunks** (fixed-offset chunks in
~8 MiB packs, deduplicated against the previous generation), so an
offset-stable edit to a big payload — the shape of DuckDB/SQLite block writes
— uploads and stores only the novel bytes. (Fixed-offset means an insertion
that *shifts* content re-chunks everything after it; the design is tuned for
database files, not documents.) The `[fsspec]` extra adds a read-only
filesystem with **selective reads** over chunked generations — any byte range
costs only the pack slices that cover it, with every fully-covered chunk
hash-verified (partial edge chunks can't be — there are no whole-chunk bytes
to hash).

## Positioning — when to use this, and when not to

**Use Delta Lake (delta-rs) if your problem is "tables on S3."** It is mature,
multi-writer on plain S3 (via the same conditional-write primitive), and read
by everything — Spark, polars, pandas, DuckDB itself. This project does not
compete with that, and a table-append benchmark won't change the answer.

This project is for two problems Delta doesn't address:

1. **A mutable *database* on S3, not a table.** Delta versions tables; it has
   no story for an arbitrary DuckDB/SQLite file — views, macros, schema and
   all — or any other opaque artifact. `BlobStore` gives any payload atomic
   multi-writer versioning with time travel, on S3 alone. That's a different
   category, not a faster horse.
2. **Serverless multi-writer DuckLake.** DuckLake's own argument against
   Delta/Iceberg is that lakehouse metadata belongs in a real database (no
   JSON-log compaction, no file-listing walls, cross-table transactions).
   But upstream multi-writer DuckLake requires a database server (Postgres,
   per its own recommendation) —
   surrendering "just a bucket" exactly where Delta keeps it. This transport
   closes that gap: the catalog database itself becomes the versioned
   payload, and the whole lakehouse — catalog included — is one S3 prefix.

The trade is explicit and inherited from DuckLake's design, and we'd choose
it anyway: **one catalog = one commit chain**. What that buys:

- **Cross-table ACID.** One transaction can touch many tables; one marker CAS
  commits them all. Delta on plain S3 cannot span two tables atomically (each
  table's log is its own commit domain; catalog-managed Delta can, but that
  reintroduces a catalog service); Iceberg likewise needs catalog-level
  support for it.
- **Whole-database time travel.** `gen/<n>` is a mutually consistent snapshot
  of *every* table, not per-table version vectors you correlate by hand.
- **One correctness story.** A single linear generation chain is the anchor
  for the commit protocol, chunk dedup, and GC safety proofs.

What it costs: writers to *disjoint* tables still contend on one marker —
Delta commits them in parallel, we serialize them (the loser rebases cheaply;
chunking means a retry re-uploads only novel bytes). At this design point — a
compact catalog, a handful of writers, zero services — contention is S3 round
trips, not a bottleneck. If you need truly independent write domains, run two
lakes: that's Delta's granularity, with the boundary made explicit.

Nearby systems, for orientation: **sqlite-s3vfs** reads a database from S3 by
range but has no multi-writer commit story; **Litestream** replicates a single
writer; **s3ql** checkpoints its metadata database to the bucket but must
enforce a single mount for exactly that reason; **JuiceFS** achieves
multi-writer POSIX-over-S3 by reintroducing a metadata service (Redis et al.)
— the dependency this design exists to avoid; **DynamoDB commit coordination**
(what Delta-on-S3 required before S3 grew conditional writes) is the sidecar
that `If-None-Match` made unnecessary — this project is a bet on that
primitive, all the way down.

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

See [`docs/DESIGN.md`](docs/DESIGN.md) for the consolidated invariants,
failure asymmetry, and accepted residual risks, `AGENTS.md` for the
contribution contract, and
[`docs/ROADMAP.md`](docs/ROADMAP.md) for what's planned and deliberately deferred.
