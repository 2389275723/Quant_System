from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class StrengthDecision:
    allow_new_positions: bool
    exposure_multiplier: float
    note: str


def strength_gate(picks: pd.DataFrame, min_final_score: float) -> StrengthDecision:
    """V1.5 Ranking Trap protection (scaffold).

    If the TopN is weak, we reduce exposure / forbid new positions.
    """
    if picks is None or picks.empty or "final_score" not in picks.columns:
        return StrengthDecision(True, 1.0, "no picks")

    top = picks.sort_values("rank_final" if "rank_final" in picks.columns else "rank", ascending=True).head(1)
    top_score = float(top["final_score"].iloc[0]) if not top.empty else 0.0
    if top_score < float(min_final_score):
        return StrengthDecision(False, 0.5, f"top final_score={top_score:.4f} < {min_final_score}")
    return StrengthDecision(True, 1.0, f"top final_score={top_score:.4f} ok")
