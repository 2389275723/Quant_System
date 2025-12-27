from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import pandas as pd

from src.data.tushare_bars import gateway_query


@dataclass
class TushareMarketCtxConfig:
    enabled: bool = True
    # optional endpoints; your gateway may not support them even if points are enough
    enable_moneyflow_ind_ths: bool = True
    enable_limit_cpt_list: bool = True
    timeout_sec: int = 15


def fetch_industry_moneyflow(trade_date: str) -> pd.DataFrame:
    """Tushare `moneyflow_ind_ths` (requires points on official)."""
    df = gateway_query(
        "moneyflow_ind_ths",
        params={"trade_date": trade_date},
        fields="trade_date,ts_code,industry_name,net_amount,net_amount_rate",
    )
    # numeric
    for c in ["net_amount", "net_amount_rate"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def fetch_concept_limitup(trade_date: str) -> pd.DataFrame:
    """Tushare `limit_cpt_list` (concept-level limit up/down)."""
    df = gateway_query(
        "limit_cpt_list",
        params={"trade_date": trade_date},
        fields="trade_date,ts_code,name,up_nums,down_nums,limit_nums",
    )
    for c in ["up_nums", "down_nums", "limit_nums"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def build_tushare_market_context(trade_date: str, topk: int = 8) -> Dict[str, object]:
    """Best-effort build market context from Tushare optional endpoints."""
    ctx: Dict[str, object] = {"trade_date": trade_date}

    # moneyflow by industry (THS)
    try:
        mf = fetch_industry_moneyflow(trade_date)
        if not mf.empty and "net_amount" in mf.columns:
            mf2 = mf.sort_values("net_amount", ascending=False).head(int(topk))
            # UI expects `industry_moneyflow`
            ctx["industry_moneyflow"] = mf2[[c for c in mf2.columns if c in ["industry_name", "net_amount", "net_amount_rate"]]].to_dict("records")
    except Exception as e:
        ctx["industry_moneyflow_error"] = str(e)

    # concept limit-up counts
    try:
        lim = fetch_concept_limitup(trade_date)
        if not lim.empty:
            # prioritize up_nums if available
            key = "up_nums" if "up_nums" in lim.columns else ("limit_nums" if "limit_nums" in lim.columns else None)
            if key:
                lim2 = lim.sort_values(key, ascending=False).head(int(topk))
                cols = [c for c in ["name", "up_nums", "limit_nums", "down_nums"] if c in lim2.columns]
                # UI expects `concept_limitups`
                ctx["concept_limitups"] = lim2[cols].to_dict("records")
    except Exception as e:
        ctx["concept_limitup_error"] = str(e)

    return ctx
