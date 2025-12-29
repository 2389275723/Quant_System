#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Sweep & patch common pandas scalar .fillna() crashes.

What it fixes (safe, targeted):
1) pd.to_numeric(df.get("col", default), errors="coerce").fillna(fill)
   -> _num_col(df, "col", fill=fill, default=default)
   (always returns a Series aligned to df.index; never calls .fillna on a scalar)

2) something.get(...).fillna(fill)
   -> _num_scalar(something.get(...), fill)
   (converts to numeric scalar; never calls .fillna (scalar-safe))

It will:
- make a .bak backup before writing
- insert helper functions into a file only if needed (idempotent)

Usage (from project root):
  python tools/sweep_fillna_scalar.py --dry-run
  python tools/sweep_fillna_scalar.py --apply

You can narrow scope:
  python tools/sweep_fillna_scalar.py --apply --root src

"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple


PATCH_MARKER = "AUTO_PATCH_FILLNA_SCALAR_GUARD_2025_12_29"


HELPERS = f"""
# --- {PATCH_MARKER}
def _num_scalar(x, fill=0.0):
    \"\"\"Scalar-safe numeric conversion (no .fillna on scalars).\"\"\"
    import pandas as pd
    y = pd.to_numeric(x, errors="coerce")
    try:
        return fill if pd.isna(y) else float(y)
    except Exception:
        return fill


def _num_col(df, col, fill=0.0, default=None):
    \"\"\"Series-safe numeric column getter.

    If df has the column: returns numeric Series with .fillna(fill)
    If missing: returns constant Series aligned to df.index (default if provided else fill)
    \"\"\"
    import pandas as pd
    if hasattr(df, "columns") and hasattr(df, "index") and col in getattr(df, "columns"):
        return pd.to_numeric(df[col], errors="coerce").fillna(fill)

    idx = getattr(df, "index", None)
    const = fill if default is None else default
    if idx is None:
        return pd.Series([const], dtype="float64")
    return pd.Series(const, index=idx, dtype="float64")
# --- END {PATCH_MARKER}
""".lstrip("\n")


@dataclass
class Change:
    file: Path
    n_repl: int
    preview: List[Tuple[int, str, str]]  # (lineno, before, after)


def _insert_helpers_if_needed(text: str) -> str:
    if PATCH_MARKER in text:
        return text

    # Insert after initial import block
    lines = text.splitlines()
    insert_at = 0
    saw_import = False
    for i, line in enumerate(lines):
        if line.startswith("import ") or line.startswith("from "):
            saw_import = True
            insert_at = i + 1
            continue
        if saw_import and line.strip() == "":
            insert_at = i + 1
            break

    new_lines = lines[:insert_at] + [""] + HELPERS.splitlines() + [""] + lines[insert_at:]
    return "\n".join(new_lines) + ("\n" if text.endswith("\n") else "")


def _compute_preview(text: str, old: str, new: str) -> List[Tuple[int, str, str]]:
    """Find a few occurrences of old string and map to line numbers for preview."""
    preview = []
    start = 0
    for _ in range(8):  # limit preview items per file
        idx = text.find(old, start)
        if idx == -1:
            break
        lineno = text.count("\n", 0, idx) + 1
        preview.append((lineno, old.strip(), new.strip()))
        start = idx + len(old)
    return preview


def patch_text(text: str) -> Tuple[str, int, List[Tuple[int, str, str]], bool]:
    """Return (new_text, n_replacements, preview, helpers_needed)."""
    n_total = 0
    previews: List[Tuple[int, str, str]] = []
    helpers_needed = False

    # Pattern A: pd.to_numeric(<df>.get("col", default), errors="coerce").fillna(fill)
    # Captures: df, col, default?, fill
    pat_a = re.compile(
        r"""pd\.to_numeric\(\s*(?P<df>[A-Za-z_]\w*)\.get\(\s*(?P<q>["'])(?P<col>[^"']+)(?P=q)\s*(?:,\s*(?P<default>[^)]+?))?\s*\)\s*,\s*errors\s*=\s*(?P<q2>["'])coerce(?P=q2)\s*\)\s*\.fillna\(\s*(?P<fill>[^)]+?)\s*\)"""
    )

    def repl_a(m: re.Match) -> str:
        nonlocal helpers_needed
        helpers_needed = True
        df = m.group("df")
        col = m.group("col")
        default = m.group("default")
        fill = m.group("fill")
        if default is None:
            # if no explicit default, use fill as default constant when missing
            return f'_num_col({df}, "{col}", fill={fill}, default={fill})'
        return f'_num_col({df}, "{col}", fill={fill}, default={default})'

    # Because this is a regex rewrite, do a preview by capturing original substrings
    for m in list(pat_a.finditer(text))[:8]:
        old = m.group(0)
        new = repl_a(m)
        previews += _compute_preview(text, old, new)

    text2, n = pat_a.subn(repl_a, text)
    n_total += n
    text = text2

    # Pattern B: <obj>.get(...).fillna(fill)  (scalar-style usage)
    # Example: d.get("r_ret20", 0.0).fillna(0.0)
    pat_b = re.compile(
        r"""(?P<obj>[A-Za-z_]\w*)\.get\(\s*(?P<inside>[^)]+?)\s*\)\s*\.fillna\(\s*(?P<fill>[^)]+?)\s*\)"""
    )

    def repl_b(m: re.Match) -> str:
        nonlocal helpers_needed
        helpers_needed = True
        obj = m.group("obj")
        inside = m.group("inside")
        fill = m.group("fill")
        return f"_num_scalar({obj}.get({inside}), {fill})"

    # Preview a few
    for m in list(pat_b.finditer(text))[:8]:
        old = m.group(0)
        new = repl_b(m)
        previews += _compute_preview(text, old, new)

    text2, n = pat_b.subn(repl_b, text)
    n_total += n
    text = text2

    # Insert helpers if we used them
    if helpers_needed and n_total > 0:
        text = _insert_helpers_if_needed(text)

    return text, n_total, previews, helpers_needed


def iter_py_files(root: Path) -> List[Path]:
    out: List[Path] = []
    for p in root.rglob("*.py"):
        parts = {x.lower() for x in p.parts}
        if any(x in parts for x in {".venv", "venv", "__pycache__", ".git", "site-packages"}):
            continue
        out.append(p)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="src", help="Root folder to scan (default: src)")
    ap.add_argument("--apply", action="store_true", help="Apply changes in-place (creates .bak backups)")
    ap.add_argument("--dry-run", action="store_true", help="Only show what would change (default if neither flag set)")
    args = ap.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"[ERR] root not found: {root.resolve()}")
        return 2

    dry_run = args.dry_run or (not args.apply)

    changes: List[Change] = []
    for f in iter_py_files(root):
        try:
            text = f.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            # skip non-utf8
            continue

        new_text, n_repl, preview, _ = patch_text(text)
        if n_repl <= 0:
            continue

        changes.append(Change(file=f, n_repl=n_repl, preview=preview))

        if not dry_run:
            bak = f.with_suffix(f.suffix + ".bak")
            if not bak.exists():
                bak.write_text(text, encoding="utf-8")
            f.write_text(new_text, encoding="utf-8")

    if not changes:
        print("[OK] No risky patterns found. Nothing to do.")
        return 0

    print(f"[{'DRY' if dry_run else 'APPLY'}] Patched candidates: {len(changes)}")
    for c in changes:
        print(f" - {c.file.as_posix()}  (replacements={c.n_repl})")
        for (lineno, before, after) in c.preview[:3]:
            print(f"    L{lineno}: {before}")
            print(f"        -> {after}")

    if dry_run:
        print("\nRun with --apply to write changes (creates .bak backups).")
    else:
        print("\n[OK] Changes applied. Backups created with .bak suffix.")
        print("建议跑：python -m py_compile src && run_night_job.bat ... 再验证")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
