from __future__ import annotations

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
        out["f_ret1"] = pd.to_numeric(out["pct_chg"], errors="coerce").fillna(0.0) / 100.0
    else:
        out["f_ret1"] = 0.0

    if "turnover_rate" in out.columns:
        out["f_turnover"] = pd.to_numeric(out["turnover_rate"], errors="coerce").fillna(0.0) / 100.0
    else:
        out["f_turnover"] = 0.0

    if "amount" in out.columns:
        out["f_amount_log"] = np.log1p(pd.to_numeric(out["amount"], errors="coerce").fillna(0.0))
    else:
        out["f_amount_log"] = 0.0

    # simple size factor
    if "circ_mv" in out.columns:
        out["f_circ_mv_log"] = np.log1p(out["circ_mv"].astype(float))
    else:
        out["f_circ_mv_log"] = np.nan

    return out
