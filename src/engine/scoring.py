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

from typing import Dict, Tuple

import numpy as np
import pandas as pd


def _safe_col(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if col in df.columns:
        return df[col].astype(float)
    return pd.Series([default] * len(df), index=df.index, dtype=float)


def compute_rule_scores(df: pd.DataFrame, weights: Dict[str, float]) -> pd.DataFrame:
    """Compute base rule_score and sub-scores (trend/flow/fund)."""
    if df is None or df.empty:
        return df
    out = df.copy()

    # Use preprocessed rank columns if available, else fallback to raw
    trend = _safe_col(out, "f_ret1__rank", 0.5)
    flow = _safe_col(out, "f_amount_log__rank", 0.5)
    fund = _safe_col(out, "f_turnover__rank", 0.5)

    out["trend_score"] = trend
    out["flow_score"] = flow
    out["fund_score"] = fund

    w_trend = float(weights.get("trend", 0.5))
    w_flow = float(weights.get("flow", 0.3))
    w_fund = float(weights.get("fund", 0.2))

    out["score_rule"] = (w_trend * trend + w_flow * flow + w_fund * fund)

    # final_score starts as rule score (dual-head model can adjust later)
    out["final_score"] = out["score_rule"]
    return out


def apply_vol_damper(df: pd.DataFrame, eps: float = 1e-6) -> pd.DataFrame:
    """V1.5 Volatility Damper (scaffold).
    Without rolling vol, we use turnover as a proxy (higher turnover => higher 'vol').
    In production: use 20d realized volatility.

    NOTE:
    - df.get("turnover_rate") may return a scalar when the column is missing.
      pd.to_numeric(scalar) returns a scalar float, which does NOT have .fillna().
    - We therefore always construct a Series aligned to df.index.
    """
    if df is None or df.empty:
        return df
    out = df.copy()

    # turnover_rate proxy (always a Series aligned to out.index)
    if "turnover_rate" in out.columns:
        vol_proxy = _num_any(out["turnover_rate"], 0.0)
    else:
        vol_proxy = pd.Series(0.0, index=out.index, dtype="float64")
    out["vol_proxy"] = vol_proxy

    # final_score (always a Series aligned to out.index)
    if "final_score" in out.columns:
        base_final = _num_any(out["final_score"], 0.0)
    else:
        base_final = pd.Series(0.0, index=out.index, dtype="float64")

    out["final_score"] = base_final / (vol_proxy + eps)
    return out


def rank_scores(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    out = out.sort_values("final_score", ascending=False).reset_index(drop=True)
    out["rank"] = np.arange(1, len(out) + 1, dtype=int)
    out["rank_rule"] = out["rank"]
    out["rank_final"] = out["rank"]
    return out
