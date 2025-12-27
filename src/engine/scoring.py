from __future__ import annotations

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
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    vol_proxy = out.get("turnover_rate", 0.0)
    vol_proxy = pd.to_numeric(vol_proxy, errors="coerce").fillna(0.0)
    out["vol_proxy"] = vol_proxy
    out["final_score"] = out["final_score"] / (vol_proxy + eps)
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
