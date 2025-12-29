from __future__ import annotations


# --- AUTO_PATCH_FILLNA_SCALAR_GUARD_2025_12_29
def _num_scalar(x, fill=0.0):
    """Scalar-safe numeric conversion (no .fillna on scalars)."""
    import pandas as pd
    y = pd.to_numeric(x, errors="coerce")
    try:
        return fill if pd.isna(y) else float(y)
    except Exception:
        return fill


def _num_col(df, col, fill=0.0, default=None):
    """Series-safe numeric column getter.

    If df has the column: returns numeric Series with .fillna(fill)
    If missing: returns constant Series aligned to df.index (default if provided else fill)
    """
    import pandas as pd
    if hasattr(df, "columns") and hasattr(df, "index") and col in getattr(df, "columns"):
        return pd.to_numeric(df[col], errors="coerce").fillna(fill)

    idx = getattr(df, "index", None)
    const = fill if default is None else default
    if idx is None:
        return pd.Series([const], dtype="float64")
    return pd.Series(const, index=idx, dtype="float64")


def _num_any(x, fill=0.0):
    """Generic numeric conversion that works for Series or scalar."""
    import pandas as pd
    y = pd.to_numeric(x, errors="coerce")
    if hasattr(y, "fillna"):
        return y.fillna(fill)
    try:
        return fill if pd.isna(y) else float(y)
    except Exception:
        return fill
# --- END AUTO_PATCH_FILLNA_SCALAR_GUARD_2025_12_29

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class GateResult:
    ok: bool
    reason: str
    details: str | None = None


def kill_switch(stop_file: str) -> GateResult:
    if os.path.exists(stop_file):
        return GateResult(False, "KILL_SWITCH", f"STOP file exists: {stop_file}")
    return GateResult(True, "OK")


def fat_finger_check(orders: pd.DataFrame, max_lines: int, max_notional_per_order: float) -> GateResult:
    if orders is None:
        return GateResult(False, "NO_ORDERS")
    if len(orders) == 0:
        return GateResult(True, "OK", "no orders")
    if len(orders) > int(max_lines):
        return GateResult(False, "TOO_MANY_LINES", f"lines={len(orders)} > {max_lines}")
    if "notional" in orders.columns:
        mx = float(_num_any(orders["notional"], 0.0).max())
        if mx > float(max_notional_per_order):
            return GateResult(False, "ORDER_TOO_LARGE", f"max_notional={mx:.2f} > {max_notional_per_order}")
    return GateResult(True, "OK")
