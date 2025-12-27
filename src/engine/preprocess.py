from __future__ import annotations

from typing import List, Tuple

import numpy as np
import pandas as pd


def winsorize_series(s: pd.Series, lower_q: float = 0.01, upper_q: float = 0.99) -> pd.Series:
    if s.dropna().empty:
        return s
    lo = s.quantile(lower_q)
    hi = s.quantile(upper_q)
    return s.clip(lower=lo, upper=hi)


def rank_pct(s: pd.Series, ascending: bool = True) -> pd.Series:
    # pct rank in [0, 1]
    # Use 'average' to avoid ties explosion
    r = s.rank(method="average", ascending=ascending)
    if len(r) <= 1:
        return pd.Series([0.5] * len(r), index=r.index)
    return (r - 1) / (len(r) - 1)


def zscore(s: pd.Series) -> pd.Series:
    mu = s.mean()
    sd = s.std(ddof=0)
    if sd == 0 or np.isnan(sd):
        return (s - mu) * 0.0
    return (s - mu) / sd


def preprocess_factors(df: pd.DataFrame, factor_cols: List[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    for c in factor_cols:
        if c not in out.columns:
            continue
        out[f"{c}__w"] = winsorize_series(out[c].astype(float))
        out[f"{c}__rank"] = rank_pct(out[f"{c}__w"], ascending=True)
        out[f"{c}__z"] = zscore(out[f"{c}__w"])
    return out
