"""Guard: no starter placeholder token may survive a package rename.

The template ships with a placeholder package name that `make rename` (see
`scripts/rename.py`) substitutes across the tree. A manual or half-finished
rename can leave the token behind in docs or config — the exact failure this
template was found shipping downstream. This test fails any spawned repo where
a stray token remains, regardless of *how* the rename was done.

Self-disabling in the un-renamed template: while the placeholder source
directory still exists, the token is expected everywhere and the check is
skipped.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

# Assembled in two halves so the one-shot renamer never rewrites this guard's
# own sentinel (it replaces only the literal token), and so this file never
# matches itself.
PLACEHOLDER = "my" + "project"

ROOT = Path(__file__).resolve().parents[1]

# The renamer legitimately carries the placeholder until you delete it after
# renaming; everything else must be clean.
_EXCLUDE = frozenset({"scripts/rename.py"})


def _tracked_files() -> list[str]:
    result = subprocess.run(
        ["git", "ls-files"],  # noqa: S607  # git resolved from PATH by design
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.splitlines()


def test_no_placeholder_token_remains() -> None:
    """Once renamed, the placeholder token must not survive in any tracked file."""
    if (ROOT / "src" / PLACEHOLDER).is_dir():
        return  # un-renamed template: the placeholder is expected everywhere

    offenders: list[str] = []
    for rel in _tracked_files():
        if rel in _EXCLUDE:
            continue
        try:
            text = (ROOT / rel).read_text(encoding="utf-8")
        except (UnicodeDecodeError, FileNotFoundError):
            continue  # binaries / staged-then-removed paths
        if PLACEHOLDER in text.lower():
            offenders.append(rel)

    assert not offenders, (
        f"the starter placeholder {PLACEHOLDER!r} survived the rename in {offenders}. "
        "Re-run `make rename NEW=<pkg>` or finish the substitution by hand so no "
        "spawned repo ships an un-renamed reference."
    )
