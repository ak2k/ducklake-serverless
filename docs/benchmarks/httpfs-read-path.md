# Streaming read path (`reader(stream=…)`) — benchmark & crossover

`Lake.reader()` fetches the catalog two ways:

- **download** (default): one bulk `GET` of the catalog `.duckdb`, attach the
  local copy.
- **stream** (`stream=True` / `"auto"`): `ATTACH 'ducklake:s3://…' (READ_ONLY)`
  directly over DuckDB `httpfs`, which range-reads only the blocks a query
  touches — no download.

This note records why the default is download and where streaming wins, so the
`STREAM_MIN_BYTES` threshold in `session.py` is grounded rather than guessed.

## What was measured

A counting reverse-proxy in front of MinIO tallied exact bytes and `GET` count
for a metadata-selective read (`count(*)` on one small table), with the catalog
grown by an unrelated large table. (Scripts were scratch; the numbers below are
the reproducible result.)

| catalog | download bytes | download GETs | httpfs bytes | httpfs GETs |
|--------:|---------------:|:-------------:|-------------:|:-----------:|
| 3.4 MB  | 3,420,160      | 1             | 3,420,164    | 14          |
| 4.2 MB  | 4,206,592      | 1             | 3,682,308    | 15          |
| 6.0 MB  | 6,041,600      | 1             | 4,206,596    | 17          |
| 6.6 MB  | 6,565,888      | 1             | 4,206,596    | 17          |

## Findings

1. **httpfs is genuinely selective.** As the catalog grows via the *other*
   table, httpfs bytes **flatten** (~4.2 MB — a fixed DuckLake metadata base +
   only the queried table) while download grows with the whole file. At 6.6 MB
   httpfs already fetches ~36 % fewer bytes, and the gap widens with size.
2. **…but it costs ~16 range GETs vs one download.** So the net win requires the
   skipped bytes to beat the round-trip cost of the extra requests.
3. **DuckLake catalogs are extremely compact** — ~4 bytes per column-stat-row;
   even 3,200 files of a 250-column table only added ~3 MB. So a catalog stays
   single-digit MB well past 100k data files (with normal compaction).

## The crossover

Model a read as `GETs × RTT + bytes / BW`. httpfs wins once the full-catalog
download transfer exceeds the extra-request penalty:

```
full_bytes / BW  >  (httpfs_GETs − 1) × RTT
full_bytes       >  ~15 × RTT × BW
```

At **RTT = 30 ms, BW = 80 MB/s → ~36 MB**. Below that, download's single
request wins; above it, download's whole-file transfer dominates and httpfs's
flat ~4 MB pulls ahead (a 200 MB catalog: download ~2.5 s vs httpfs ~0.5 s).
Higher RTT raises the crossover; on localhost (≈0 RTT) it's a wash at these
sizes (measured: ~41 ms both).

The crossover point is a model estimate: DuckLake metadata is too compact to
build a 36 MB catalog in a benchmark (millions of files), but the confirmed
byte-flattening + request counts fix both terms.

## Decision

- **Default `stream=False`** — the common case has a small catalog, where one
  download beats ~16 GETs on any backend.
- **`stream=True`** — force httpfs; for a genuinely large catalog over a
  high-latency backend, or a read-only environment with no local scratch disk
  (streaming needs no temp file). Requires an S3-backed store + credentials.
- **`stream="auto"`** — stream only when the store is S3-backed and the catalog
  is ≥ `STREAM_MIN_BYTES` (32 MiB, just under the ~36 MB modelled crossover);
  otherwise download.

Reaching the streaming-wins regime means a large uncompacted file count — the
same condition that calls for compaction anyway. So streaming is the right tool
for a big or behind-on-compaction lake on a remote backend, and download is
correct everywhere else.
