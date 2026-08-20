# Roadmap

Planned and deliberately deferred work. The engine/adapter split (payload-
agnostic engine + `BlobStore` + DuckLake `Lake`) and the chunked /
content-addressed transport (fixed-offset chunks + ~8 MiB packs, two-cycle
pack GC — see [`DESIGN.md`](DESIGN.md)) are done; everything below is
additive.

## Done — chunked / content-addressed storage (2026-07, `work-19437`)

Implemented as designed: fixed-offset chunks (64 KiB default, entry-capped
scaling) + ~8 MiB content-addressed packs + a per-generation manifest,
deduped strictly against the base manifest, windowed parallel reconstruct,
threshold-gated per adapter (`chunk_threshold`; whole-file remains below it
and keeps the httpfs streaming reader — the keep-both fork was resolved by
the threshold). Pack GC is a two-cycle tombstone mark-sweep with a fenced
ledger; invariants and accepted residuals in [`DESIGN.md`](DESIGN.md).

Residue worth keeping here:

- **Measured payoff (real E2, ~40 ms RTT, 42 MB payload):** whole-file cold
  open ~2 s and O(size) per commit → chunks+packs ~700 ms cold open, ~30×
  smaller per-commit upload/storage. Fixed-offset beat content-defined
  chunking (DuckDB keeps block offsets stable). Serial fetch is a cliff
  (~96 s) — reconstruct is windowed-parallel (`ThreadPoolExecutor`; NOT
  aiobotocore — packing keeps object counts low, threads suffice).
- Scratch benchmarks (outside this repo): `catalog_chunk_probe.py`,
  `attach_bench.py` — rerun the shape against the REAL implementation when
  tuning `chunk_threshold` / `DEFAULT_PACK_TARGET` for a workload.
- Possible follow-ups, demand-gated: pack repack/compaction (partially-dead
  packs currently retain until fully unreferenced), heal-path retry for
  flaky-transport writers (see DESIGN.md residuals), pack compression via
  the manifest's `compression` field.

## Done — fsspec read-adapter with selective reads (2026-07, `work-crn6r`)

`fsspec_fs.GenerationFileSystem` (`[fsspec]` extra): generations as read-only
file-likes (`head`, `gen/<n>`), with CHUNKED generations served by
manifest-translated ranged GETs of only the covering pack slices — this
dissolves the streaming-vs-chunking tradeoff for every fsspec-aware reader
(plus per-chunk hash verification of fully-covered chunks, which httpfs never
had). Coalesces contiguous same-pack runs into single GETs. Known limitation,
verified against duckdb 1.5 + upstream docs: DuckDB's ATTACH opens database
files through its C++ filesystem only (native + httpfs) and never consults
registered fsspec filesystems — attaching a chunked catalog still goes through
local reconstruction (`Lake.reader()`), which is windowed and cached; DuckDB
*scan* functions (read_parquet/read_csv/read_blob) DO go through the
filesystem selectively. `cat_file`/`cat_ranges` are overridden to hit the
range readers directly (no readahead-cache inflation; ranges fan out
concurrently) — the primitives pyarrow/dask actually batch through.

## Pre-deployment checklist (before first production lake)

Local drills done 2026-07-20 (see `scripts/soak_crash_drill.py`, rerunnable):
soak with genuinely-elapsing grace vs wet GC (full tombstone->delete
lifecycles, byte-identical head throughout) and SIGKILL crash-recovery
(writers + GC killed mid-flight; convergence after every kill), both against
real SeaweedFS. The chunk-size rescale boundary runs end-to-end in the
hermetic suite (MAX_ENTRIES monkeypatched tiny).

Remaining, deliberately deferred:

- [ ] One integration run against REAL AWS S3 (the canonical store; MinIO/
      SeaweedFS are proxies): `DUCKLAKE_IT_*` at a scratch bucket, run the
      integration lane + `scripts/soak_crash_drill.py`. Probe R2/GCS too if
      they will host lakes.
- [ ] Overnight default-floor soak: `soak_crash_drill.py --grace-seconds
      3600 --rounds 100` (no unsafe flag — the true default path).
- [ ] GB-scale throughput/memory envelope (windowed reconstruct at real
      payload sizes) — perf, not correctness; when a real large-payload
      workload exists.

## Deferred — physical `core/` + `adapters/` reorg

Move engine modules into `src/ducklake_serverless/core/` and the DuckLake
modules into `src/ducklake_serverless/adapters/ducklake/` (with `BlobStore` at
`adapters/blob.py`). Cosmetic: the duckdb-free boundary is already enforced by
`tests/test_engine_boundary.py`, so this only makes the structure match the
logical split. Notes when doing it:
- `gc.py` is already duckdb-free (Lake only under `TYPE_CHECKING`) → `core/`.
- `generation.py` pulls `duckdb` via `engine.MAGIC` (used by `check_hygiene`) →
  either split `check_hygiene`/`publish_generation` to the DuckLake adapter, or
  move `generation.py` wholesale to `adapters/ducklake/` (it's DuckLake-only
  today). `GenerationCache` and `gc.collect` are generic — promote to `core/`
  when a non-DuckLake consumer needs them.
- Add `__init__` re-exports for the duckdb-free public API (`BlobStore`,
  `S3ObjectStore`, `make_s3_client`, probes) — keep `__init__` duckdb-free so
  the boundary test stays green (do NOT re-export `Lake`).

## Planned adapters & surface

- **`DuckDBStore`** — version a plain (non-DuckLake) DuckDB database file.
  Between `BlobStore` and `Lake`: DuckDB-magic `validate` + a `connection()` that
  `ATTACH`es the reconstructed file read-only, but no DuckLake semantics
  (wholesale mutation, abort-on-conflict). Low marginal value over `BlobStore`
  + hygiene — demand-gated.
- **CLI** (`[cli]` extra) — `put` / `get` / `history` / `gc` over `BlobStore`
  (and DuckLake). The face that makes this a usable general-purpose utility.
  Add `[project.scripts]`, re-lock `uv.lock`, expose a `nix run .#<cli>` app.

## Open, non-blocking

- **Naming / positioning** — the project is now an engine with DuckLake as one
  adapter; `ducklake-serverless` is really the adapter name. Decide the public
  project name before any release. One candidate framing is worked out in
  "Open — positioning: a coordination engine?" below.
- A broadly-adopted CLI utility (à la restic/litestream) would ideally be a
  single static binary; Python is right for now given the DuckLake dependency
  and the existing tested codebase, but note the tension.

## Open — positioning: a coordination engine?

A candidate answer to the naming question above. Undecided; recorded so the
argument does not have to be rebuilt from scratch.

**The framing:** a serverless CAS-coordination engine, with DuckLake as its
flagship adapter. The engine already supplies most of what people stand up
ZooKeeper / etcd / Raft to get: a linearizable, dense, append-only log
(`roots/<gen>`, every slot claimed exactly once, never rewritten) and a
fencing capability — both consequences of the commit CAS, not features
bolted onto it. The third piece is `probe_capabilities` + the compatibility
table, which answer "does this endpoint actually enforce conditional writes
*under concurrency*" — the half that homegrown S3 locks skip entirely, and
the half that is expensive to get right.

**What the fencing capability actually is.** Not the generation number: that
is a public value anyone can read, and possession of a non-exclusive value
fences nothing. The exclusive capability is *having won the create of
generation N*, evidenced by the marker's uuid — ours means WON forever
(`root.resolve_marker`). The token is the `(generation, uuid)` pair. It
fences writes to THIS chain; it does not fence a third-party resource unless
that resource participates in the same check.

**The boundary is the credibility**, and would have to be stated as loudly as
the capability: no watches (S3 has no push — everything polls), S3
round-trip latency rather than sub-millisecond, no membership at all and
liveness only as far as a lease TTL implies it, and no fencing on anyone's
*serving* path — a demoted holder can still answer stale reads, which is
client routing and not ours. A different point on the curve from Raft, not a
replacement for it.

**Deliberately outside any such surface:** `lease.py` as a general-purpose
distributed lock. It is correct for GC *because* overlapping sweeps are
idempotent (its own docstring leads with the requirement); published under a
lock-shaped name, callers will take it for mutual exclusion without reading
the contract. Leader election likewise stays unshipped — the CAS already
fences writes without a lease that can be wrong, and an API by that name
would imply a split-brain guarantee this layer cannot deliver.

**What it would cost:** less than a subsystem, more than a rename.
`probe_capabilities` is already public, but `BlobStore` exposes
write/read/head — not "claim slot N with my token" — and slot-claiming lives
inside `commit.py` today. The log-as-primitive needs real surface.

### Motivating consumer (2026-08): HA for a Quack-served catalog

Quack is DuckDB's client/server protocol (beta in v1.5.3); its DuckLake
integration reintroduces the single-owning-process server this project
positions against, with no replication protocol shipped. A wrapper giving it
HA needs a fencing token, an "overtaken" signal, and reconstruct-from-latest.

The trap, and the reason this is not just wiring: **the overtaken signal only
exists under `ConflictPolicy.ABORT_ALL`.** Under the default policy
`run_commit` rebases onto the head until won, so a blind append that loses a
race is retried rather than reported — a demoted primary would keep
committing forever, interleaved with the real one, and never learn it lost.
The engine's headline feature, making lost races invisible to concurrent
writers, is exactly what makes single-primary HA undetectable. An HA path
must opt out of it.

Second obstacle, load-bearing before any of this is believable:
`check_hygiene` requires a cleanly checkpointed, WAL-free file, so publishing
from a live server means a CHECKPOINT plus a brief write-quiesce per
generation. Measure that window first — if it is fat, the rest is moot.
