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
filesystem selectively.

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
  project name before any release.
- A broadly-adopted CLI utility (à la restic/litestream) would ideally be a
  single static binary; Python is right for now given the DuckLake dependency
  and the existing tested codebase, but note the tension.
