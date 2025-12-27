from __future__ import annotations

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

    mean_ret = pd.to_numeric(universe_df["pct_chg"], errors="coerce").fillna(0.0).mean()
    if mean_ret < -0.8:
        return Regime("RISK_OFF", 0.7, f"universe mean pct_chg={mean_ret:.2f}%")
    if mean_ret > 0.8:
        return Regime("RISK_ON", 1.1, f"universe mean pct_chg={mean_ret:.2f}%")
    return Regime("NEUTRAL", 1.0, f"universe mean pct_chg={mean_ret:.2f}%")
