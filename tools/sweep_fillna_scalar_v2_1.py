#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Sweep & patch common pandas scalar .fillna() crashes (v2.1).

This version always prints a summary (even when nothing changes) so you won't see a "blank run".

Run (from project root):
  python tools\sweep_fillna_scalar_v2_1.py --dry-run --root src
  python tools\sweep_fillna_scalar_v2_1.py --apply   --root src
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


def _num_any(x, fill=0.0):
    \"\"\"Generic numeric conversion that works for Series or scalar.\"\"\"
    import pandas as pd
    y = pd.to_numeric(x, errors="coerce")
    if hasattr(y, "fillna"):
        return y.fillna(fill)
    try:
        return fill if pd.isna(y) else float(y)
    except Exception:
        return fill
# --- END {PATCH_MARKER}
""".lstrip("\n")


@dataclass
class Change:
    file: Path
    n_repl: int
    preview: List[Tuple[int, str, str]]


def _ensure_helpers(text: str) -> str:
    if PATCH_MARKER not in text:
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
    # Marker present, ensure _num_any exists
    if "def _num_any" in text:
        return text
    end_pat = re.compile(rf"(^# --- END {re.escape(PATCH_MARKER)}\s*$)", re.MULTILINE)
    m = end_pat.search(text)
    if not m:
        return text + ("\n" if not text.endswith("\n") else "") + HELPERS
    # Insert _num_any before end marker by replacing whole helper block with HELPERS (safe upgrade)
    # Find start marker line
    start_pat = re.compile(rf"(^# --- {re.escape(PATCH_MARKER)}\s*$)", re.MULTILINE)
    s = start_pat.search(text)
    if not s:
        return text + ("\n" if not text.endswith("\n") else "") + HELPERS
    return text[:s.start()] + HELPERS + text[m.end():]


def _compute_preview(text: str, old: str, new: str) -> List[Tuple[int, str, str]]:
    out = []
    start = 0
    for _ in range(6):
        idx = text.find(old, start)
        if idx == -1:
            break
        lineno = text.count("\n", 0, idx) + 1
        out.append((lineno, old.strip(), new.strip()))
        start = idx + len(old)
    return out


def patch_text(text: str) -> Tuple[str, int, List[Tuple[int, str, str]], bool]:
    n_total = 0
    previews: List[Tuple[int, str, str]] = []
    used_helpers = False

    # A) pd.to_numeric(df.get("col", default), errors="coerce").fillna(fill)
    pat_a = re.compile(
        r"""pd\.to_numeric\(\s*(?P<df>[A-Za-z_]\w*)\.get\(\s*(?P<q>["'])(?P<col>[^"']+)(?P=q)\s*(?:,\s*(?P<default>[^)]+?))?\s*\)\s*,\s*errors\s*=\s*(?P<q2>["'])coerce(?P=q2)\s*\)\s*\.fillna\(\s*(?P<fill>[^)]+?)\s*\)"""
    )

    def repl_a(m: re.Match) -> str:
        nonlocal used_helpers
        used_helpers = True
        df = m.group("df")
        col = m.group("col")
        default = m.group("default")
        fill = m.group("fill")
        if default is None:
            return f'_num_col({df}, "{col}", fill={fill}, default={fill})'
        return f'_num_col({df}, "{col}", fill={fill}, default={default})'

    for m in list(pat_a.finditer(text))[:4]:
        old = m.group(0)
        new = repl_a(m)
        previews += _compute_preview(text, old, new)

    text, n = pat_a.subn(repl_a, text)
    n_total += n

    # B) obj.get(...).fillna(fill)
    pat_b = re.compile(
        r"""(?P<obj>[A-Za-z_]\w*)\.get\(\s*(?P<inside>[^)]+?)\s*\)\s*\.fillna\(\s*(?P<fill>[^)]+?)\s*\)"""
    )

    def repl_b(m: re.Match) -> str:
        nonlocal used_helpers
        used_helpers = True
        return f"_num_scalar({m.group('obj')}.get({m.group('inside')}), {m.group('fill')})"

    for m in list(pat_b.finditer(text))[:4]:
        old = m.group(0)
        new = repl_b(m)
        previews += _compute_preview(text, old, new)

    text, n = pat_b.subn(repl_b, text)
    n_total += n

    # C) pd.to_numeric(x, errors="coerce").fillna(fill)
    pat_c = re.compile(
        r"""pd\.to_numeric\(\s*(?P<x>[^)]+?)\s*,\s*errors\s*=\s*(?P<q>["'])coerce(?P=q)\s*\)\s*\.fillna\(\s*(?P<fill>[^)]+?)\s*\)"""
    )

    def repl_c(m: re.Match) -> str:
        nonlocal used_helpers
        x = m.group("x").strip()
        fill = m.group("fill").strip()
        # Skip if already safe helper call
        if x.startswith("_num_") or "_num_col" in x or "_num_scalar" in x or "_num_any" in x:
            return m.group(0)
        used_helpers = True
        return f"_num_any({x}, {fill})"

    for m in list(pat_c.finditer(text))[:4]:
        old = m.group(0)
        new = repl_c(m)
        if old != new:
            previews += _compute_preview(text, old, new)

    # Count only changes by comparing
    before = text
    text = pat_c.sub(repl_c, text)
    n_total += (0 if text == before else 1)

    if used_helpers and (text != before or n_total > 0):
        text = _ensure_helpers(text)

    return text, n_total, previews, used_helpers


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
    ap.add_argument("--root", default="src")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"[ERR] root not found: {root.resolve()}")
        return 2

    dry_run = args.dry_run or (not args.apply)
    print(f"[START] root={root.as_posix()}  mode={'DRY' if dry_run else 'APPLY'}")

    changes: List[Change] = []
    for f in iter_py_files(root):
        try:
            text = f.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        new_text, n_repl, preview, _ = patch_text(text)
        if new_text == text:
            continue

        changes.append(Change(f, n_repl, preview))

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
        print(f" - {c.file.as_posix()}  (replacements~={c.n_repl})")
        for lineno, before, after in c.preview[:4]:
            print(f"    L{lineno}: {before}")
            print(f"        -> {after}")

    if dry_run:
        print("\nRun with --apply to write changes (creates .bak backups).")
    else:
        print("\n[OK] Changes applied. Backups created with .bak suffix.")
        print("Next: python -m compileall -q src")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
