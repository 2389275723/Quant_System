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

import pandas as pd
import numpy as np


def compute_factors(df: pd.DataFrame) -> pd.DataFrame:
    """Compute a minimal set of *atomic* factors.

    For demo / V1.5 scaffold, we only use today's snapshot fields.
    In production you'd merge rolling windows (MA/volatility) with historical bars.
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    # atomic-ish factors
    # 注意：不同数据源/网关可能裁剪字段(例如只返回 close)，导致 pct_chg/amount 等字段缺失。
    # 原实现 out.get("amount", 0.0).astype(...) 在字段缺失时会返回 float 默认值，从而触发：
    #   'float' object has no attribute 'astype'
    if "pct_chg" in out.columns:
        out["f_ret1"] = _num_any(out["pct_chg"], 0.0) / 100.0
    else:
        out["f_ret1"] = 0.0

    if "turnover_rate" in out.columns:
        out["f_turnover"] = _num_any(out["turnover_rate"], 0.0) / 100.0
    else:
        out["f_turnover"] = 0.0

    if "amount" in out.columns:
        out["f_amount_log"] = np.log1p(_num_any(out["amount"], 0.0))
    else:
        out["f_amount_log"] = 0.0

    # simple size factor
    if "circ_mv" in out.columns:
        out["f_circ_mv_log"] = np.log1p(out["circ_mv"].astype(float))
    else:
        out["f_circ_mv_log"] = np.nan

    return out
