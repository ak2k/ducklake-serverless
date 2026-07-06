"""Probe one S3-compatible backend for conditional-write enforcement.

Emits a JSON result document and exits 0 iff the observed verdict matches
--expected — so a Renovate bump that changes a backend's behavior turns
into a failing check on exactly the version that changed the answer.

Run from the repo root: uv run python scripts/compat_probe.py ...
"""

from __future__ import annotations

import argparse
import contextlib
import datetime
import json
import sys
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import boto3
import botocore.exceptions
from botocore.config import Config

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

PROBE_BODY_1 = b'{"probe": 1}'
PROBE_BODY_2 = b'{"probe": 2}'


def _status(exc: botocore.exceptions.ClientError) -> int:
    """HTTP status of a ClientError."""
    return int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))


def probe(client: S3Client, bucket: str, key: str) -> dict[str, str]:
    """Four-probe CAS conformance check. Returns per-probe outcomes."""
    results: dict[str, str] = {}

    def put(name: str, **kwargs: object) -> None:
        try:
            client.put_object(Bucket=bucket, Key=key, **kwargs)  # pyright: ignore[reportArgumentType]  # kwargs shape varies by probe
            results[name] = "200"
        except botocore.exceptions.ClientError as exc:
            results[name] = str(_status(exc))

    put("create_only_fresh", Body=PROBE_BODY_1, IfNoneMatch="*")
    put("create_only_existing", Body=PROBE_BODY_1, IfNoneMatch="*")
    head = client.head_object(Bucket=bucket, Key=key)
    etag = head["ETag"].strip('"')
    put("if_match_correct", Body=PROBE_BODY_2, IfMatch=etag)
    put("if_match_stale", Body=PROBE_BODY_2, IfMatch=etag)
    client.delete_object(Bucket=bucket, Key=key)
    return results


def verdict_of(results: dict[str, str]) -> str:
    """Classify: enforce = both negative probes 412; ignore = both 200."""
    fresh_ok = results.get("create_only_fresh") == "200"
    match_ok = results.get("if_match_correct") == "200"
    existing = results.get("create_only_existing")
    stale = results.get("if_match_stale")
    if not (fresh_ok and match_ok):
        return "broken"
    if existing == "412" and stale == "412":
        return "enforce"
    if existing == "200" and stale == "200":
        return "ignore"
    return "mixed"


def run_moto() -> tuple[dict[str, str], str]:
    """In-process moto probe (no server needed)."""
    import importlib.metadata  # noqa: PLC0415  # moto is a dev-only dep; keep the script importable without it

    from moto import mock_aws  # noqa: PLC0415  # same

    version = importlib.metadata.version("moto")
    with mock_aws():
        client: S3Client = boto3.client("s3", region_name="us-east-1")  # pyright: ignore[reportUnknownMemberType]  # boto3 factory untyped
        client.create_bucket(Bucket="compat-probe")
        results = probe(client, "compat-probe", f"probe/{uuid.uuid4()}")
    return results, version


def run_endpoint(endpoint: str, access_key: str, secret_key: str, bucket: str) -> dict[str, str]:
    """Probe a live S3-compatible endpoint."""
    client: S3Client = boto3.client(  # pyright: ignore[reportUnknownMemberType]  # boto3 factory untyped
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(retries={"max_attempts": 2}),
    )
    with contextlib.suppress(botocore.exceptions.ClientError):
        client.create_bucket(Bucket=bucket)  # exists, or created out-of-band (garage)
    return probe(client, bucket, f"probe/{uuid.uuid4()}")


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", required=True)
    parser.add_argument("--version", required=True, help="Version/tag under test")
    parser.add_argument("--expected", required=True, choices=["enforce", "ignore"])
    parser.add_argument("--endpoint", default=None, help="Omit for moto (in-process)")
    parser.add_argument("--access-key", default="probe")
    parser.add_argument("--secret-key", default="probe")
    parser.add_argument("--bucket", default="compat-probe")
    parser.add_argument("--out", required=True, help="Output JSON path")
    # argparse Namespace attrs are Any; bind them once with real types here.
    ns = parser.parse_args()
    backend: str = ns.backend  # pyright: ignore[reportAny]
    arg_version: str = ns.version  # pyright: ignore[reportAny]
    expected: str = ns.expected  # pyright: ignore[reportAny]
    endpoint: str | None = ns.endpoint  # pyright: ignore[reportAny]
    access_key: str = ns.access_key  # pyright: ignore[reportAny]
    secret_key: str = ns.secret_key  # pyright: ignore[reportAny]
    bucket: str = ns.bucket  # pyright: ignore[reportAny]
    out: str = ns.out  # pyright: ignore[reportAny]

    if backend == "moto":
        results, version = run_moto()
    else:
        if endpoint is None:
            parser.error("--endpoint is required for non-moto backends")
        results = run_endpoint(endpoint, access_key, secret_key, bucket)
        version = arg_version

    verdict = verdict_of(results)
    doc = {
        "backend": backend,
        "version": version,
        "verdict": verdict,
        "expected": expected,
        "probes": results,
        "tested_at": datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%d"),
        "provenance": "ci",
    }
    Path(out).write_text(json.dumps(doc, indent=2) + "\n")
    print(json.dumps(doc, indent=2))

    if verdict != expected:
        print(
            f"\nVERDICT CHANGED: {backend} {version} — expected "
            f"'{expected}', observed '{verdict}'. If this is a Renovate "
            "bump, the new version changed conditional-write behavior: update "
            "the expected verdict AND docs/compatibility.md.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
