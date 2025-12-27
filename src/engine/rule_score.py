from __future__ import annotations

from typing import Any, Dict
import pandas as pd
import numpy as np

def compute_rule_score(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    d = df.copy()

    # safe numeric
    for c in ["r_ret20","r_near_high","r_rsi6","r_surge5","r_ma20_range","circ_mv","turnover_rate"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    # trend component
    d["trend_score"] = (
        40.0 * d.get("r_ret20", 0.0).fillna(0.0)
        + 20.0 * d.get("r_near_high", 0.0).fillna(0.0)
        + 20.0 * d.get("r_rsi6", 0.0).fillna(0.0)
        + 20.0 * d.get("r_ma20_range", 0.0).fillna(0.0)
    )

    # fund component: prefer smaller circ_mv (可改)
    if "circ_mv" in d.columns:
        size_rank = d["circ_mv"].rank(pct=True, ascending=True)  # smaller => lower rank
        d["fund_score"] = 20.0 * (1.0 - size_rank.fillna(0.5))
    else:
        d["fund_score"] = 10.0

    # flow component: prefer reasonable turnover (avoid极低流动性)
    if "turnover_rate" in d.columns:
        tr = d["turnover_rate"].fillna(0.0)
        # Penalize too low turnover
        flow = tr.rank(pct=True, ascending=True)
        d["flow_score"] = 20.0 * flow
    else:
        d["flow_score"] = 0.0

    d["score_rule"] = d["trend_score"] + d["fund_score"] + d["flow_score"]

    # Only universe members can keep score, others set to -inf
    d.loc[d.get("universe_flag", 0) != 1, "score_rule"] = -1e9

    # rank
    d["rank_rule"] = d["score_rule"].rank(ascending=False, method="first").astype(int)

    return d
