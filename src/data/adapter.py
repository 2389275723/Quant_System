from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

import pandas as pd
import numpy as np

from ..core.timeutil import fmt_ts, now_cn

def clean_daily_bars(df: pd.DataFrame) -> pd.DataFrame:
    """统一字段/类型：trade_date(ts), ts_code, OHLCV, amount"""
    if df is None or df.empty:
        return pd.DataFrame()

    d = df.copy()
    # normalize trade_date
    if "trade_date" in d.columns:
        d["trade_date"] = d["trade_date"].astype(str).str.replace("-", "")
    # code normalization
    if "ts_code" not in d.columns and "code" in d.columns:
        d["ts_code"] = d["code"]
    # Ensure numeric columns
    num_cols = ["open","high","low","close","pct_chg","vol","amount","turnover_rate","circ_mv","total_mv"]
    for c in num_cols:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")
    # Keep minimal columns
    keep = [c for c in [
        "trade_date","ts_code","name","industry","open","high","low","close",
        "pct_chg","vol","amount","turnover_rate","circ_mv","total_mv"
    ] if c in d.columns]
    d = d[keep]
    return d

def attach_audit(df: pd.DataFrame, source_name: str, source_url: str, run_id: str, strategy_id: str,
                 strategy_version: str, config_hash: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    d = df.copy()
    d["source_name"] = source_name
    d["source_url"] = source_url
    d["ingest_time"] = fmt_ts(now_cn())
    d["run_id"] = run_id
    d["strategy_id"] = strategy_id
    d["strategy_version"] = strategy_version
    d["config_hash"] = config_hash
    return d
