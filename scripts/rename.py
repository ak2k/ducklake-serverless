#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# ///
"""One-shot rename of the starter package `myproject` -> a chosen name.

Replaces every occurrence across tracked files — the lowercase package name *and*
the uppercase `MYPROJECT_` env prefix (which a plain `sed s/myproject/.../` misses
and leaves the settings tests broken) — then `git mv`s the source directory.
Portable across macOS/Linux: no `sed -i` dialect differences.

The directory move happens *before* any content rewrite, so a name collision or
git error aborts with zero files touched rather than a half-renamed tree (the very
failure mode the manual sed left behind).

Usage:
    make rename NEW=your_pkg_name
    uv run scripts/rename.py your_pkg_name

After running: `uv lock && uv sync && make check`, then delete this script.
"""

from __future__ import annotations

import keyword
import subprocess
import sys
from pathlib import Path

OLD = "myproject"


def _git(*args: str, root: Path) -> str:
    return subprocess.run(  # noqa: S603  # args are literal git tokens, never user-supplied
        ["git", *args],  # noqa: S607  # partial path: git is resolved from PATH by design
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def main(argv: list[str]) -> int:
    """Rename the package across tracked files; return a process exit code."""
    if len(argv) != 2 or not argv[1].isidentifier() or keyword.iskeyword(argv[1]):
        sys.stderr.write(
            "usage: uv run scripts/rename.py <new_pkg_name>  "
            "(a valid Python identifier, not a reserved keyword)\n"
        )
        return 2
    new = argv[1]
    if new == OLD:
        sys.stderr.write(f"already named {OLD!r} — nothing to do\n")
        return 0

    root = Path(__file__).resolve().parent.parent
    try:
        _git("rev-parse", "--is-inside-work-tree", root=root)
    except (subprocess.CalledProcessError, FileNotFoundError):
        sys.stderr.write(f"{root} is not a git repository — clone the template first\n")
        return 1
    if not (root / "src" / OLD).is_dir():
        sys.stderr.write(f"src/{OLD} not found — already renamed?\n")
        return 1
    if (root / "src" / new).exists():
        sys.stderr.write(f"src/{new} already exists — refusing to overwrite\n")
        return 1

    # Move first: a collision or git error aborts here, before any file is rewritten.
    _git("mv", f"src/{OLD}", f"src/{new}", root=root)

    self_path = Path(__file__).resolve()
    replacements = ((OLD, new), (OLD.upper(), new.upper()))
    changed = 0
    for rel in _git("ls-files", root=root).splitlines():
        path = root / rel
        if path.resolve() == self_path:
            continue  # don't rewrite the renamer itself
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, FileNotFoundError):
            continue  # skip binaries / files staged-but-removed
        new_text = text
        for old, repl in replacements:
            new_text = new_text.replace(old, repl)
        if new_text != text:
            path.write_text(new_text, encoding="utf-8")
            changed += 1

    sys.stdout.write(
        f"moved src/{OLD} -> src/{new} and rewrote {changed} file(s).\n"
        "next: `uv lock && uv sync && make check`, then delete scripts/rename.py.\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
