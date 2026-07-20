# Agent instructions

Contract for agents working in this repo. Read first; overrides defaults.

## Project divergences from the template (declared, not drift)

| Divergence | Rationale |
|---|---|
| `boto3` for S3, not `httpx` | Hand-rolling SigV4 + conditional-write headers over httpx recreates the exact bug class this library exists to prevent. `objectstore.py` is the ONLY module importing boto3; its `ClientError`s are wrapped into `errors.py` domain errors at that boundary. |
| `duckdb` behind a typed facade | duckdb's Python API is loosely typed; basedpyright suppressions live only in `engine.py` (same treatment the template gives structlog). No other module imports duckdb. |
| Sync core, no `anyio` | The commit path is inherently sequential (fetch → attach → SQL → detach → upload → CAS) and duckdb's API is sync. Parallelism in this system lives BETWEEN processes, arbitrated by S3 CAS — not inside one. An anyio wrapper may come later for consumers; the protocol core stays sync. |

Protocol invariants agents must not weaken — the CONSOLIDATED list, the
failure asymmetry (every ambiguity resolves toward availability, never
durability), and the accepted residual risks live in
[`docs/DESIGN.md`](docs/DESIGN.md). Read it before touching `gc.py`,
`chunk.py`, or `commit.py`. Highlights:

- A commit is the create-only PUT of an immutable per-generation marker
  `roots/<gen>` (`If-None-Match: *`). Exactly one body ever exists per
  generation, markers are NEVER deleted (immortal), and they are dense
  (0..head, no gaps). Catalog generations and data files are likewise
  immutable and uniquely named.
- `root-hint` is the ONLY mutable object — an advisory latest-generation
  number, never a document source. Every RootDoc comes from a marker;
  the hint is only a probe start position, always GET-verified.
- Ambiguity resolution is EXACT and PERMANENT: on a 412 or ambiguous marker
  create, GET the marker — our uuid means WON forever, another's means LOST,
  absent means retry the SAME doc (`root.resolve_marker`). Never retry a
  conditional PUT to answer "did it land?".
- `catalog_key` / `marker_key` are derived from `(generation, uuid)`, never
  stored.
- Only blind appends auto-replay on conflict; state-dependent DML aborts unless
  the caller opted into `replay_all`. A loser rebases onto the resolved HEAD,
  not the single collided marker.
- Version fields in the marker doc gate commits; auto-migration of the catalog
  format is forbidden — BOTH pins are enforced: duckdb_storage_version before
  attach, ducklake_format_version before publish.
- GC sweeps `catalog/` only — NEVER `roots/`. It never deletes the current
  generation or any generation inside the retention window; readers pinned to
  retained generations keep their catalog ATTACH unconditionally. Their DATA
  reads are durable for min(catalog retention window, expire_older_than +
  physical_delete_delay) — plan expire_older_than around the longest reader pin.
- Non-dry-run data maintenance enforces MIN_PHYSICAL_DELAY: the orphan pass
  must never race an in-flight writer's staged-but-uncommitted Parquet.
- S3 clients SHOULD disable transport retries (`make_s3_client`); with markers
  the own-write evidence is immortal, so a self-412 from an SDK retry resolves
  WON — but disabling retries keeps the resolution path simple.

- Chunked transport (see docs/DESIGN.md for the full list): manifest
  entries are FULL and the dedup source is strictly the base manifest —
  both load-bearing for the GC mark induction. Pack deletion is two-cycle
  tombstone with a fenced ledger write as the commit point. Transport is
  marker-declared; readers never content-sniff. All age gates compare
  store-issued timestamps; MIN_PACK_GRACE is load-bearing twice (stalled
  writers AND store-clock second-granularity).

## Stack (one per concern; substitutes are bans)

| Concern | Use | Not |
|---|---|---|
| Package manager | `uv` | pip, poetry, pipenv, pyenv |
| Lint / format | `ruff` + `ruff format` | black, isort, flake8, pylint |
| Type checker | `basedpyright` strict | mypy, pyright |
| Boundary validation | Pydantic v2 (`extra="forbid"`, `frozen=True`) | dataclasses *(at boundaries)*, attrs, TypedDict |
| HTTP | `httpx` | requests, aiohttp, urllib |
| Async runtime | `anyio` | raw asyncio |
| Logging | `structlog` | stdlib logging, `print()` |
| Paths | `pathlib.Path` | `os.path` |
| Tests | `pytest` + `hypothesis` | unittest |
| Errors | subclass `ducklake_serverless.errors.AppError` | bare `Exception`, string errors |

The **`Not` column bans these for boundary validation**, not everywhere. Internally,
a frozen `@dataclass(frozen=True)` is the right tool for a trusted value type that
never crosses an edge — Pydantic earns its validation cost only at boundaries.

## When you need it, use

Not every project hits these concerns. When yours does, this is the
choice that fits the rest of the stack — don't substitute.

| Concern | Use |
|---|---|
| Retry / backoff | `stamina` |
| CLI framework | `typer` (paired with `rich` for human-facing output) |
| Time injection in tests | `time-machine` |
| HTTP rate limiting (outbound) | `aiolimiter` |
| SQL | `sqlalchemy 2.0` Core (ORM only when session identity-map earns its keep) |
| Persistent on-disk cache (survives restarts) | `diskcache` (SQLite-backed; wrap in a typed `cache.py` facade — see gotcha). In-process memoization is stdlib `functools.cache`; in-memory TTL/LRU is `cachetools` — don't reach for `diskcache` until you need persistence across runs |
| Async file I/O | `anyio.Path` |
| SAST / taint analysis | `opengrep` (Semgrep-OSS fork; source→sink dataflow that ruff-`S` can't do) |

The baseline already ships ruff-`S` (bandit) for syntactic security lints and a
gitleaks workflow for secrets — that covers most projects. Reach for `opengrep`
only when the service is internet-facing or deserializes / SQL-builds untrusted
input, where interprocedural taint tracking earns its keep. It's an external
binary (not `uv`-installable), so wire it into CI / the nix shell, not `make check`.

## Inner loop

```
make fix     # autofix + full check
make check   # full check (CI runs this)
```

Both must be green. `filterwarnings = ["error"]` and `xfail_strict = true`
are load-bearing — deprecation warnings and unexpected passes are real
failures, not noise. Nix users: `nix develop` first; everything else is identical.

## Principles

1. **Boundaries fail loudly.** Pydantic at every external edge with
   `extra="forbid"` unless an explicit comment justifies otherwise. Domain
   errors wrap third-party exceptions — `httpx.RequestError` never leaks
   to a caller. Shape drift is an alarm, not a silent fallthrough.

2. **Suppress with cost.** Every `# pyright: ignore[code]` and `# noqa: code`
   names the specific rule and a one-line reason. Suppression is annotated
   debt — visible, greppable, justified — not a workaround.

3. **Copy the canonical example.** Inventing a new shape for any of these
   is a deliberate choice. The defaults are:
   - service: `src/ducklake_serverless/example_service.py`
   - config:  `src/ducklake_serverless/config.py`
   - logging: `src/ducklake_serverless/log.py`
   - test:    `tests/test_example_service.py`
   - error:   `src/ducklake_serverless/errors.py`

4. **Diverge with a comment.** When tuning a default below (or deviating
   from the stack table), leave `# DIVERGE: <reason>` so future readers
   don't "fix" it back.

5. **Ask when guessing.** Unknown boundary shape, irresolvable type error,
   missing test infrastructure → ask. Don't invent the shape, don't
   suppress just to make it pass.

6. **Tests assert behavior, not implementation.** `hypothesis` for property
   tests at parsing boundaries; `httpx.MockTransport` for hermetic HTTP;
   `monkeypatch.setenv` for env-driven settings; assert on domain errors,
   not on HTTP status codes leaking through.

7. **Imports declare intent.** `from __future__ import annotations` at the
   top of every module; runtime-only third-party types inside `if
   TYPE_CHECKING:`.

8. **Async runtime is anyio.** Not raw asyncio. Compose with sync at the
   edges via `anyio.from_thread` / `anyio.to_thread`.

## Appropriate divergence

Defaults assume a **strict greenfield service**. These profiles are
legitimate alternatives — tune as a set, not piecemeal.

### Profile A — Distributable library or CLI

| Knob | Default | Tune to |
|---|---|---|
| `requires-python` | `>=3.12` | `>=3.10` (or per support window) |
| Ruff `D*` (docstrings) | enabled | drop for small internal surface |
| `pythonVersion` (basedpyright) | `"3.12"` | match `requires-python` lower bound |
| Coverage `fail_under` | 80 | 60 (CLI argv is hard to cover) |
| `PLR0913` (too many args) | strict | ignore (verb signatures are wide) |

**CLI stack:** `typer` + `rich` (human output) + `stamina` (retry) — the "when you
need it" picks, standardized so every CLI looks the same. Adding `typer` needs one
ruff stanza so its parameter-default idiom doesn't trip `B008`:

```toml
[tool.ruff.lint.flake8-bugbear]
extend-immutable-calls = ["typer.Argument", "typer.Option"]
```

Also: `vulture` for unused public API (gap between ruff and coverage).
Tune `ignore_decorators` for decorator-registered handlers.

For publishing: `hatch-vcs` derives version from git tags; pair with
`.git_archival.txt` + `.gitattributes export-subst` so flake-via-tarball
consumers resolve versions without `.git`. https://github.com/ofek/hatch-vcs

### Profile B — Reverse-engineering / scraping

| Knob | Default | Tune to |
|---|---|---|
| HTTP transport | `httpx` default | `httpx` + `httpx-curl-cffi` transport (fingerprint evasion; keeps the httpx API and `MockTransport` tests) |
| Pydantic `extra` | `"forbid"` | `"ignore"` (no stable upstream schema) |
| `typeCheckingMode` | `"strict"` | `"standard"` + strict per-module on public API |
| `reportMissingTypeStubs` | `"warning"` | `false` |
| `reportAny` | `"warning"` | `"none"` |

Knobs apply per-module too — relax `extra="ignore"` on the one parser facing an
unstable upstream, not project-wide. Keep the inner loop hermetic: mark live
network/browser tests `@pytest.mark.live` and default-deselect with `-m 'not live'`
in addopts; CI opts in. Heavy native deps (`nodriver`, `camoufox`) belong in a
`[project.optional-dependencies]` `browser` extra so the core install stays
wheel-light — CI still builds the extra.

### Profile C — Single-file script

Skip the template. PEP 723 inline header:

```python
#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx", "structlog"]
# ///
```

Graduate to the template once the script grows past one file or acquires a test.

### Profile D — Data / analytics pipeline

Batch/sync transforms over a data engine — not a service. Tune as a set:

| Knob | Default | Tune to |
|---|---|---|
| `httpx` + `anyio` | baseline deps | drop both (batch/sync; no live HTTP or async runtime) |
| Data engine | — | add one: `duckdb` / `pandas` / `polars` (+ `sqlglot` to build SQL safely) |
| `typeCheckingMode` | `"strict"` | keep strict; downgrade the three `reportUnknown*` in the data-adapter module only (untyped engines — see "Known gotchas") |
| Coverage `fail_under` | 80 | 50–70 (orchestration + I/O are integration-tested; property-test the parsers) |
| Regression gate | unit asserts | + snapshot / golden outputs; pin `PYTHONHASHSEED=0` so ordering is deterministic |
| SQL lint | — | `sqlfluff`, wired into `make` beside `ruff` (e.g. a `sql-lint` target) |

## Known gotchas (non-obvious from the toolchain)

- `structlog.get_logger()` returns `Any`. Annotate via
  `if TYPE_CHECKING: from structlog.stdlib import BoundLogger`, then suppress
  the RHS with `# pyright: ignore[reportAny]` + reason.
- Use `http.HTTPStatus.NOT_FOUND` — stdlib, well-typed. Not `httpx.codes.NOT_FOUND`
  (mistyped by httpx as tuple) and not bare `404` (PLR2004).
- `extra="forbid"` Pydantic models raise on any unknown upstream field.
  Intentional: drift fails at the boundary, not silently corrupting downstream.
- **Untyped data engines** (`duckdb`, `pandas`, `sqlglot`) flood `strict` mode with
  `reportUnknown*`. Don't scatter `# pyright: ignore`. Isolate the untyped surface in
  one adapter module and downgrade exactly three reports at the top of that file:
  ```python
  # pyright: reportUnknownMemberType=warning, reportUnknownArgumentType=warning, reportUnknownVariableType=warning
  ```
  The rest of the codebase stays strict; the boundary is greppable and contained.
- **`diskcache` is untyped and sync.** No `py.typed`, so `strict` floods `reportUnknown*`
  (same class as the data engines above). Don't scatter `# pyright: ignore` — isolate it
  behind one typed `cache.py` facade. Its API is sync SQLite: fine in a CLI/batch, but in
  an async service wrap reads/writes in `anyio.to_thread` inside that facade (Principle 8).
