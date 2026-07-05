# python-starter

A starter template for new Python projects, optimized for agentic
development *and* modern Python best practices.

Philosophy: collapse the option space at the project level so an AI coding
agent (or any contributor) writes the same shape of code every time — the
TypeScript-strict experience for Python.

## Quick start

```bash
gh repo create my-project --template ak2k/python-starter --public --clone
cd my-project

# Rename the package — portable; also fixes the MYPROJECT_ env prefix and moves src/
make rename NEW=your_pkg_name

# Install + verify
uv sync
make check
```

### Nix users

The flake exposes two dev shells, two venv packages, plus `nix fmt` and `nix flake check`:

```bash
nix develop              # default: python + uv + make. `uv sync` populates .venv.
nix develop .#pure       # uv2nix editable venv. No .venv. Worktree edits live.
nix build .#default      # runtime venv: project + [project.dependencies]
nix build .#dev          # dev venv: adds [dependency-groups].dev (pytest, ruff, ...)
nix fmt                  # autoformat flake.nix (RFC-166 via nixfmt)
nix flake check          # statix lint + nixfmt --check on flake.nix
```

Pick `default` for daily work — `uv add` / `uv lock` / `uv run` all mutate state
naturally and `make check` is the same command as the non-Nix path. Pick `.#pure`
when you want fully Nix-resolved deps with one-step onboarding; the trade-off is
that `uv.lock` changes require exiting and re-entering the shell so Nix can
re-resolve. The `packages` outputs are for nixpkgs PRs, NixOS modules, or
downstream Nix consumers — not needed for daily work.

direnv auto-activation: edit `.envrc` to uncomment `use flake`, then `direnv allow`.

uv remains the source of truth — `pyproject.toml` + `uv.lock` drive everything.
The Nix layer is a lens, built via [uv2nix](https://pyproject-nix.github.io/uv2nix/).

## What's in here

| File | Purpose |
|---|---|
| `pyproject.toml` | uv deps + ruff strict + basedpyright strict + pytest config — single source of truth |
| `AGENTS.md` | Contract for AI agents. Stack banlist + inner loop + divergence guide. |
| `CLAUDE.md` | Symlink → `AGENTS.md` (compat shim for tools that read CLAUDE.md only) |
| `Makefile` | `make check` = full inner loop. CI runs the same command. |
| `scripts/rename.py` | One-shot package rename (`make rename NEW=...`). Delete after use. |
| `src/myproject/example_service.py` | Canonical service shape — copy for new services |
| `tests/test_example_service.py` | Canonical test shape — copy for new tests |
| `src/myproject/errors.py` | Domain error hierarchy |
| `.github/workflows/ci.yml` | CI runs `make check` |
| `flake.nix` + `.envrc` | Optional Nix layer: `default` (uv-managed) + `.#pure` (uv2nix editable) dev shells; `packages.{default,dev}` venvs |

## Philosophy

One tool per concern, banned substitutes, strict types at the boundary,
canonical examples to pattern-match against. The contract lives in
[AGENTS.md](AGENTS.md) — including the "Appropriate divergence" section for
libraries / CLIs / scrapers / data pipelines / one-off scripts.

## Related work

- [Ranteck/PyStrict](https://github.com/Ranteck/PyStrict-strict-python) —
  strict-mode template that inspired the type-checker config here.
- [osprey-oss/cookiecutter-uv](https://github.com/osprey-oss/cookiecutter-uv) —
  heavier mainstream alternative (cookiecutter + extensive Jinja templating).
- [astral-sh/uv](https://github.com/astral-sh/uv),
  [astral-sh/ruff](https://github.com/astral-sh/ruff),
  [DetachHead/basedpyright](https://github.com/DetachHead/basedpyright) —
  upstream tools.

## License

MIT.
