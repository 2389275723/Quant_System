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

from dataclasses import dataclass
from typing import Dict

import pandas as pd


@dataclass
class Regime:
    name: str
    score_multiplier: float
    note: str


def detect_regime(universe_df: pd.DataFrame) -> Regime:
    """V1.5 RegimeEngine (lightweight scaffold).

    In production you would:
      - use official index snapshots (HS300/ZZ500/ZZ1000) MA20
      - add drawdown circuit-breaker
    Here we use universe mean return as a proxy.
    """
    if universe_df is None or universe_df.empty or "pct_chg" not in universe_df.columns:
        return Regime("UNKNOWN", 1.0, "no data")

    mean_ret = _num_any(universe_df["pct_chg"], 0.0).mean()
    if mean_ret < -0.8:
        return Regime("RISK_OFF", 0.7, f"universe mean pct_chg={mean_ret:.2f}%")
    if mean_ret > 0.8:
        return Regime("RISK_ON", 1.1, f"universe mean pct_chg={mean_ret:.2f}%")
    return Regime("NEUTRAL", 1.0, f"universe mean pct_chg={mean_ret:.2f}%")
