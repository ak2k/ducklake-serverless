# Roadmap

Planned and deliberately deferred work. The engine/adapter split (payload-
agnostic engine + `BlobStore` + DuckLake `Lake`) is done and boundary-locked;
everything below is additive.

## Deferred — chunked / content-addressed storage

Replace the whole-file-per-generation transport with content-addressed
**fixed-offset chunks + ~8 MB packs + a per-generation manifest**, deduped
against the base generation's manifest, reconstructed by parallel pack fetch.

- **Why deferred:** measured — DuckLake catalogs stay ~5–6 MB unless data
  inlining is on, so chunking is marginal for the DuckLake adapter today; and it
  breaks the httpfs streaming reader (a generation would no longer be a single
  attachable object). Revisit when a real large-payload workload exists (an
  inlined catalog, or a large `BlobStore` blob).
- **Measured payoff (real E2, ~40 ms RTT, 42 MB payload):** whole-file cold open
  ~2 s and O(size) per commit → chunks+packs ~700 ms cold open, ~30× smaller
  per-commit upload/storage. Fixed-offset beat content-defined chunking (DuckDB
  keeps block offsets stable across checkpoints). Serial chunk fetch is a hard
  cliff (~96 s at 16 KB/42 MB) — reconstruct **must** be parallel
  (`ThreadPoolExecutor`; do NOT switch to aiobotocore — packing keeps object
  counts low, so threads suffice).
- **Design decision needed first:** chunk-and-retire-streaming vs keep-both
  transports (see the "Step 2 transport" fork).
- Scratch benchmarks live outside this repo: `catalog_chunk_probe.py`,
  `attach_bench.py` (measure dedup and cold-open against a real catalog/MinIO/E2).

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
- **fsspec read-adapter** — expose a generation as a read-only file-like so any
  fsspec-aware tool (pandas/polars/duckdb) can read it by URL. Falls out of the
  `Payload.materialize` reconstruction path. Gate behind a `[fsspec]` extra.
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
