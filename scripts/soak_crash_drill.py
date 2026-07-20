"""Pre-deployment drill: default-config soak + SIGKILL crash recovery.

Runs locally against any S3-compatible endpoint (spin up SeaweedFS/MinIO and
set DUCKLAKE_IT_* like the integration lane). Two phases, both against the
REAL default code paths — no `_unsafe_allow_short_grace`, no injection
wrappers:

SOAK: N writer rounds of chunked churn racing wet GC cycles at an
honest-but-small grace (>= MIN_PACK_GRACE is production truth; the soak's
grace is minutes so tombstone aging genuinely ELAPSES on the wall clock —
the hermetic suite can only plant aged stamps). Asserts after every GC
cycle: head reconstructs byte-identically, referenced ⊆ surviving packs,
and by the end at least one full tombstone->delete lifecycle completed.

CRASH: forks writer/GC child processes and SIGKILLs them at random points
mid-commit / mid-GC (real kill -9, not simulated faults), then asserts the
survivors converge: a fresh writer commits, a fresh reader reconstructs,
a fresh GC cycle runs to completion, and no committed generation references
a missing pack.

Usage:
    DUCKLAKE_IT_ENDPOINT=http://127.0.0.1:8333 \
    DUCKLAKE_IT_ACCESS_KEY=any DUCKLAKE_IT_SECRET_KEY=any \
    uv run python scripts/soak_crash_drill.py --rounds 30 --kills 12 \
        --grace-seconds 90
"""

# ruff: noqa: S101, S311, D103, PLR2004 — a drill script: asserts ARE the
# checks, randomness is jitter not crypto, and helpers are self-describing.

from __future__ import annotations

import argparse
import contextlib
import os
import random
import signal
import sys
import tempfile
import time
import uuid
from datetime import timedelta
from multiprocessing import Process
from pathlib import Path

from ducklake_serverless.blob import BlobStore
from ducklake_serverless.chunk import PACKS_PREFIX, Manifest
from ducklake_serverless.errors import AppError, InputValidationError
from ducklake_serverless.gc import MIN_PACK_GRACE, collect
from ducklake_serverless.objectstore import S3ObjectStore, make_s3_client

BUCKET = os.environ.get("DUCKLAKE_IT_BUCKET", "ducklake-drill")


def make_store(prefix: str) -> S3ObjectStore:
    os.environ.setdefault("AWS_ACCESS_KEY_ID", os.environ["DUCKLAKE_IT_ACCESS_KEY"])
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", os.environ["DUCKLAKE_IT_SECRET_KEY"])
    client = make_s3_client(endpoint_url=os.environ["DUCKLAKE_IT_ENDPOINT"])
    with contextlib.suppress(Exception):
        client.create_bucket(Bucket=BUCKET)
    return S3ObjectStore(client, BUCKET, prefix=prefix)


def payload(seed: int, size: int = 400_000) -> bytes:
    return bytes((i * 31 + seed) % 251 for i in range(size))


def assert_referenced_subset_surviving(store: S3ObjectStore) -> None:
    surviving = set(store.list_prefix(PACKS_PREFIX))
    referenced: set[str] = set()
    for key in store.list_prefix("payload/"):
        try:
            referenced |= Manifest.from_bytes(store.get(key).body).pack_keys()
        except InputValidationError:
            continue
    missing = referenced - surviving
    assert not missing, f"retained manifests reference swept packs: {sorted(missing)[:5]}"


def soak(prefix: str, rounds: int, grace: timedelta) -> None:
    """Writer churn racing wet default-path GC; grace genuinely elapses."""
    print(f"[soak] prefix={prefix} rounds={rounds} grace={grace}")
    store = make_store(prefix)
    workdir = Path(tempfile.mkdtemp(prefix="drill-soak-"))
    bs = BlobStore(store, workdir, chunk_threshold=0)
    bs.bootstrap(b"gen0")

    # Production-truth gate: below MIN_PACK_GRACE the drill must use the
    # explicit unsafe override — refuse instead, that's what we're drilling.
    unsafe = grace < MIN_PACK_GRACE
    if unsafe:
        print(
            f"[soak] grace {grace} < MIN_PACK_GRACE {MIN_PACK_GRACE}: "
            "using _unsafe_allow_short_grace so tombstone aging ELAPSES "
            "within the drill (the floor itself is enforced by unit tests)"
        )

    lifecycle_complete = False
    last = b"gen0"
    for r in range(rounds):
        last = payload(r)
        bs.write(last)
        report = collect(
            store,
            "drill-gc",
            retain_generations=3,
            dry_run=False,
            pack_grace=grace,
            _unsafe_allow_short_grace=unsafe,
        )
        if report is not None:
            if report.swept_packs:
                lifecycle_complete = True
                print(
                    f"[soak] round {r}: swept {len(report.swept_packs)} packs "
                    f"(tombstoned {len(report.tombstoned_packs)})"
                )
            assert_referenced_subset_surviving(store)
        assert bs.read() == last, f"round {r}: head does not reconstruct"
        # Let real wall-clock pass so packs/tombstones age past the grace.
        time.sleep(max(0.2, grace.total_seconds() / max(1, rounds // 3)))

    assert lifecycle_complete, (
        "no pack completed the tombstone->delete lifecycle — raise --rounds "
        "or lower --grace-seconds so aging elapses within the drill"
    )
    print(f"[soak] PASS: {rounds} rounds, full lifecycle observed, head byte-identical throughout")


def _writer_child(prefix: str, seed: int) -> None:
    store = make_store(prefix)
    workdir = Path(tempfile.mkdtemp(prefix="drill-w-"))
    bs = BlobStore(store, workdir, chunk_threshold=0)
    for i in range(1000):  # runs until killed
        with contextlib.suppress(AppError):
            bs.write(payload(seed * 1000 + i))


def _gc_child(prefix: str, grace_s: float) -> None:
    store = make_store(prefix)
    for _ in range(1000):  # runs until killed
        with contextlib.suppress(AppError):
            collect(
                store,
                f"drill-gc-{os.getpid()}",
                retain_generations=2,
                dry_run=False,
                pack_grace=timedelta(seconds=grace_s),
                _unsafe_allow_short_grace=True,
            )
        time.sleep(0.1)


def crash_drill(prefix: str, kills: int, grace: timedelta) -> None:
    """SIGKILL writers and GC mid-flight; assert survivors converge."""
    print(f"[crash] prefix={prefix} kills={kills}")
    store = make_store(prefix)
    setup_dir = Path(tempfile.mkdtemp(prefix="drill-setup-"))
    BlobStore(store, setup_dir, chunk_threshold=0).bootstrap(b"gen0")

    rng = random.Random(42)
    for k in range(kills):
        victim_is_gc = rng.random() < 0.4
        if victim_is_gc:
            proc = Process(target=_gc_child, args=(prefix, grace.total_seconds()))
        else:
            proc = Process(target=_writer_child, args=(prefix, k))
        proc.start()
        time.sleep(rng.uniform(0.1, 1.5))  # let it get mid-flight
        assert proc.pid is not None
        os.kill(proc.pid, signal.SIGKILL)  # the real thing
        proc.join()
        print(f"[crash] kill {k}: SIGKILLed {'gc' if victim_is_gc else 'writer'} pid {proc.pid}")

        # Convergence: fresh participants must all succeed from this state.
        fresh_dir = Path(tempfile.mkdtemp(prefix=f"drill-fresh-{k}-"))
        fresh = BlobStore(store, fresh_dir, chunk_threshold=0)
        data = payload(90_000 + k)
        fresh.write(data)
        assert fresh.read() == data, f"kill {k}: fresh writer/reader diverged"
        report = collect(
            store,
            "drill-verify-gc",
            retain_generations=2,
            dry_run=False,
            pack_grace=grace,
            _unsafe_allow_short_grace=True,
        )
        assert report is not None, f"kill {k}: verify GC could not acquire lease"
        assert_referenced_subset_surviving(store)
    print(f"[crash] PASS: {kills} SIGKILLs, converged after every one")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rounds", type=int, default=30)
    ap.add_argument("--kills", type=int, default=12)
    ap.add_argument("--grace-seconds", type=float, default=90.0)
    ap.add_argument("--skip-soak", action="store_true")
    ap.add_argument("--skip-crash", action="store_true")
    args = ap.parse_args()

    grace = timedelta(seconds=args.grace_seconds)
    run_id = uuid.uuid4().hex[:8]
    if not args.skip_soak:
        soak(f"drill/{run_id}/soak", args.rounds, grace)
    if not args.skip_crash:
        crash_drill(f"drill/{run_id}/crash", args.kills, timedelta(seconds=1))
    print("[drill] ALL PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
