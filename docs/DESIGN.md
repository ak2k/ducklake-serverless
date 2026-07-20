# Design: invariants, failure asymmetry, and accepted residuals

The protocol reference is the module docstrings (`root.py` for the marker
protocol, `chunk.py` for the pack transport, `gc.py` for the sweeps). This
document consolidates what must never be weakened, which way each failure
falls, and the residual risks we accept deliberately — the format of
machokeeper's THREAT-MODEL and bincache/cache's DESIGN, applied here.

## The failure asymmetry

Every failure in this system lands on one of two sides:

- **Availability** — a commit aborts, GC skips a cycle, a pack lingers, a
  reader retries. Annoying, retryable, nothing lost.
- **Durability** — a referenced pack or live generation is deleted.
  Permanent; with content-addressed dedup one wrong pack delete destroys
  every generation referencing it (shared fate).

The design rule: **every ambiguity must resolve toward availability.** A
review finding on the durability side is a stop-the-world bug (e.g. the
twin-write mtime hole, fixed); a finding on the availability side is a
cost/benefit call and may be accepted below.

## Load-bearing invariants

Weakening any of these silently breaks a safety proof somewhere else.

1. **Markers are immutable and immortal.** `roots/<gen>` is create-only and
   never deleted. Ambiguous commit outcomes resolve by one GET of the exact
   marker — exact and permanent. GC sweeps `payload/` and `packs/` only.
2. **Commit serializes on atomic create-only.** `If-None-Match: *` must be
   atomic under concurrency; `bootstrap()` probes and refuses backends that
   enforce it only sequentially (iDrive E2) or not at all (garage, `rclone
   serve s3`). Documentation lies; only the live probe tells the truth.
3. **Transport is marker-declared, never content-sniffed, on the read
   path.** Readers dispatch on `RootDoc.transport`; it is set per commit
   from the publish outcome (`PublishOutcome` union — a marker claiming a
   transport the publish didn't use is unrepresentable). Content sniffing
   exists ONLY in GC's orphan fallback, where a false positive merely
   over-retains.
4. **Manifest entries are FULL.** Every entry names its pack directly —
   never delta-to-base. A retained manifest alone suffices to mark every
   pack it depends on.
5. **The dedup source is strictly the base generation's manifest.** No
   global index, no other generation. Together with (4) this is the GC
   mark induction: any manifest landing after GC's listing descends through
   committed bases to a marked ancestor; everything novel above it is
   younger than the run and grace-protected.
6. **Pack deletion is two-cycle.** Unreferenced + store-clock age > grace
   → tombstone (cycle K, record only); still cold a full grace later →
   delete (cycle K+1), with a pre-delete re-HEAD sparing anything whose
   mtime went young. Referenced-again packs resurrect. The FSM lives in the
   pure `decide_pack_sweep`; refusal and deletion are mutually exclusive by
   type (`RefuseSweep | SweepActions`).
7. **The tombstone ledger write is fenced and is the commit point.** It is
   written `put_if_match` against the ETag read at load (create-only when
   absent), BEFORE any delete; a fence failure means a rival GC cycle
   interleaved and the stale cycle aborts with zero deletes. Lease + fence,
   not lease alone.
8. **All age gates compare store-issued timestamps only.** The runner's
   clock never participates (`_store_now` probes the store's clock; same
   discipline as `lease.py`).
9. **`MIN_PACK_GRACE` (1 h) is load-bearing twice**: it must outlast a
   stalled writer's packs-landed→manifest-landed gap, AND it dwarfs the
   ~1–2 s whole-second truncation of real stores' LastModified (verified
   against SeaweedFS). Do not lower it on the theory that either concern
   is handled elsewhere.
10. **Writers self-heal before the manifest lands.** `verify_packs`
    unconditionally re-PUTs every novel pack (heals swept packs AND
    refreshes mtime for tombstoned-but-doomed ones — the writer half of
    the stalled-writer defense; the GC half is the pre-delete re-HEAD)
    and HEAD-checks base packs, failing the commit loudly if one vanished.
11. **Deletion-path decisions are pure functions over discriminated
    unions.** `decide_pack_sweep(resolved, metas, tombstones, now)` does no
    I/O; `ResolvedPayload` and `PackSweepPlan` are folded exhaustively —
    an unhandled case is a type error, not a silently unmarked pack.
12. **The engine core is duckdb-free** — enforced by
    `tests/test_engine_boundary.py`, not by directory layout.

## Guardrails on the deletion path

Defense-in-depth beyond the invariants; each fails toward availability:

- **Refusals**: a committed chunked generation with an unreadable manifest,
  or a committed manifest referencing a pack absent even after a direct
  re-HEAD, aborts the sweep. Orphan anomalies log-and-skip (one unreadable
  orphan must not wedge GC forever).
- **Mass-delete circuit breaker**: a sweep that would delete >40% of listed
  packs refuses — a bad listing or broken mark pass must not mass-mutate.
- **Unknown age = young**: an object without LastModified is never
  tombstoned or deleted.
- **Corrupt tombstone ledger = reset coldness**: packs wait extra cycles;
  a delete always requires a surviving, aged ledger row.
- **Attacker-shaped orphans are confined**: a payload crafted to sniff as a
  manifest can only ADD to the mark set (over-retention); it cannot reach
  `committed_refs`, trip refusals, or increase deletes.

## Residual risks (accepted, documented)

Availability-side by construction; revisit triggers noted.

- **Stalled-writer ms-race.** S3 has no conditional DELETE, so a writer
  that stalled past 2× grace AND whose refresh-PUT lands inside GC's
  ms-scale re-HEAD→DELETE gap still loses its commit's packs. Exposure is
  (stall > 2× grace) ∧ (ms-race) — strictly stronger than grace-only
  systems (Delta VACUUM, Iceberg remove_orphan_files, restic), which lose
  on stall > grace alone. Verified against SOTA 2026-07.
- **Heal path is single-attempt.** `verify_packs`' refresh-PUT has no
  retry/ambiguity loop; one transient transport failure aborts the whole
  commit loudly (nothing published, caller retries). Retrying an
  UNCONDITIONAL put has no clean did-it-land resolution, so the loop would
  add complexity for a reliability nicety. Revisit if writers run over
  genuinely flaky transport (e.g. Lambdas on a lossy path).
- **HEAD-then-GET size-cap TOCTOU.** An out-of-band writer swapping an
  object between the size HEAD and the GET defeats the manifest size cap
  → one unbounded GET (memory pressure), then parse failure → RefuseSweep.
  Delay/DoS only; an adversary with bucket write access is outside the
  trust model anyway.
- **Tombstone stamps use the cycle-start probe.** Tombstones look older by
  the resolve/decide duration (sub-second to seconds) — marginally
  anti-conservative against the ≥1 h grace floor. Negligible by
  construction; noted because it is the one hoisting direction that is not
  conservative.
- **Orphan-manifest retention DoS.** An attacker with lake write access can
  spray manifest-shaped blobs that lose marker races and, while in-window,
  mark real dead packs (blocking their reclamation) and cost GC one
  size-capped GET+parse each per cycle. Bounded to over-retention within
  the retention window; sustained cost only with sustained write access.
- **No repack.** Partially-dead packs are retained until fully
  unreferenced (raw-concatenation packs, no framing). Storage overhead,
  never correctness.
- **A whole-file lake's markers are readable by pre-transport readers;
  chunked markers are not.** `transport` serializes only when `"chunked"`,
  so old readers hard-fail (extra="forbid") exactly and only on
  generations they genuinely cannot read.

## Verification map

- Pure FSM + safety properties: `tests/test_pack_gc.py` (incl. hypothesis
  transition-legality), `tests/test_chunk.py` (incl. edit-roundtrip
  property, forced multi-window/back-ref reconstruct).
- Interleavings and injected faults: wrapper stores in `test_pack_gc.py` /
  `test_fault_injection.py` (GC-strikes-in-the-gap heal, rival ledger
  write fence, ambiguous pack PUTs).
- Concurrency: `tests/test_torture.py` chunked writers-vs-GC variant
  (exactly-once, byte-identical head, referenced ⊆ surviving).
- Real-store semantics: `tests/test_integration_minio.py`
  chunked-lifecycle test (real ETags on the fence, real LastModified
  granularity, real listings) — MinIO + SeaweedFS in CI.
- Backend conformance: `probe_capabilities` + the weekly-refreshed
  [compatibility table](compatibility.md).
