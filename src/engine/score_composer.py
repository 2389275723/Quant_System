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

from typing import Any, Dict, Optional
import pandas as pd
import numpy as np

def compose_scores(df: pd.DataFrame, model_ens: pd.DataFrame | None, cfg: Dict[str, Any]) -> pd.DataFrame:
    """Merge rule scores with model ensemble outputs, apply risk gates.

    Returns df with final_score, risk_gate_action, risk_prob, risk_severity, disagreement.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    d = df.copy()

    model_cfg = cfg.get("model", {}) or {}
    mode = model_cfg.get("mode", "shadow")
    gate = model_cfg.get("risk_gate", {}) or {}
    veto_cfg = gate.get("veto", {}) or {}
    down_cfg = gate.get("downweight", {}) or {}

    # default final_score = rule_score
    d["final_score"] = _num_col(d, "score_rule", fill=-1e9, default=-1e9)
    d["risk_gate_action"] = "PASS"
    d["risk_prob"] = 0.0
    d["risk_severity"] = 1
    d["disagreement"] = 0.0
    d["alpha_score"] = 0.0
    d["confidence"] = 0.0

    if model_ens is not None and not model_ens.empty:
        # model_ens is aligned by index order with d if created from same candidates list.
        # We merge by positional index, then keep ts_code alignment check when possible.
        me = model_ens.copy()
        # attach
        for col in ["alpha_score","risk_prob","risk_severity","disagreement","confidence"]:
            if col in me.columns:
                d[col] = _num_any(me[col], d[col])

        # rerank alpha (optional)
        if mode == "rerank":
            # alpha_score in [-3,3] -> boost in [-15,15]
            alpha_boost = d["alpha_score"].clip(-3, 3) * 5.0
            ml_weight = 0.25  # default 25%
            # If you want config-controlled, add cfg key later.
            d["final_score"] = d["final_score"] + ml_weight * alpha_boost

        # risk gating
        sev_ge = int(veto_cfg.get("severity_ge", 3))
        prob_gt = float(veto_cfg.get("prob_gt", 0.30))
        k = float(down_cfg.get("k", 0.50))

        veto_mask = (d["risk_severity"] >= sev_ge) & (d["risk_prob"] > prob_gt)
        d.loc[veto_mask, "final_score"] = -1e9
        d.loc[veto_mask, "risk_gate_action"] = "VETO"

        # downweight for others
        dw_mask = ~veto_mask
        d.loc[dw_mask, "final_score"] = d.loc[dw_mask, "final_score"] * (1.0 - k * d.loc[dw_mask, "risk_prob"].clip(0,1))
        d.loc[(dw_mask) & (d["risk_prob"] > 0), "risk_gate_action"] = "DOWNWEIGHT"

    # enforce universe hard rule again
    d.loc[d.get("universe_flag", 0) != 1, "final_score"] = -1e9
    d["rank_final"] = d["final_score"].rank(ascending=False, method="first").astype(int)

    return d
