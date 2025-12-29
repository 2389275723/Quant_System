#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Sweep & patch common pandas scalar .fillna() crashes (v2).

What it fixes (safe, targeted):
A) pd.to_numeric(df.get("col", default), errors="coerce").fillna(fill)
   -> _num_col(df, "col", fill=fill, default=default)
   (always returns a Series aligned to df.index; never calls .fillna on a scalar)

B) obj.get(...).fillna(fill)
   -> _num_scalar(obj.get(...), fill)
   (scalar-safe numeric conversion; no .fillna on scalars)

C) pd.to_numeric(x, errors="coerce").fillna(fill)   (generic)
   -> _num_any(x, fill)
   (returns Series if x is Series-like; returns scalar float if x is scalar)

It will:
- make a .bak backup before writing
- insert helper functions into a file only if needed (idempotent; v2 can upgrade v1 helpers)

Usage (from project root):
  python tools/sweep_fillna_scalar_v2.py --dry-run --root src
  python tools/sweep_fillna_scalar_v2.py --apply   --root src

Notes:
- Prefer running --dry-run first.
- After apply: python -m compileall -q src && run_night_job.bat ...

"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple


PATCH_MARKER = "AUTO_PATCH_FILLNA_SCALAR_GUARD_2025_12_29"


HELPERS_V2 = f"""
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
    \"\"\"Generic numeric conversion that works for Series or scalar.

    - If x is Series-like: returns Series with .fillna(fill)
    - If x is scalar: returns float (or fill if NaN)
    \"\"\"
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
    preview: List[Tuple[int, str, str]]  # (lineno, before, after)


def _ensure_helpers_v2(text: str) -> str:
    """
    Ensure helper block exists and includes _num_any.
    - If marker missing: insert full v2 block after imports.
    - If marker present but _num_any missing: inject _num_any into the block.
    """
    if PATCH_MARKER not in text:
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
        new_lines = lines[:insert_at] + [""] + HELPERS_V2.splitlines() + [""] + lines[insert_at:]
        return "\n".join(new_lines) + ("\n" if text.endswith("\n") else "")

    # Marker present: upgrade if needed
    if "def _num_any" in text:
        return text

    # Inject _num_any before END marker within the helper block
    insert_num_any = r