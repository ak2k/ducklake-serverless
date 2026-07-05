# Agent instructions

Contract for agents working in this repo. Read first; overrides defaults.

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
| Errors | subclass `myproject.errors.AppError` | bare `Exception`, string errors |

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
   - service: `src/myproject/example_service.py`
   - config:  `src/myproject/config.py`
   - logging: `src/myproject/log.py`
   - test:    `tests/test_example_service.py`
   - error:   `src/myproject/errors.py`

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
  (mis-typed by httpx as tuple) and not bare `404` (PLR2004).
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
