from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

import pandas as pd

from ..core.config import resolve_path
from ..utils.trade_date import normalize_trade_date

def _read_csv(path: str) -> pd.DataFrame:
    p = resolve_path(path)
    try:
        return pd.read_csv(p, dtype=str)
    except FileNotFoundError:
        return pd.DataFrame()

class ManualCSVSource:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.paths = (cfg.get("data_source", {}) or {}).get("manual_csv", {}) or {}

    def get_trade_cal(self) -> pd.DataFrame:
        df = _read_csv(self.paths.get("trade_cal_path", ""))
        # expected cols: cal_date, is_open (1/0)
        if df.empty:
            return df
        if "cal_date" in df.columns:
            df["cal_date"] = df["cal_date"].astype(str).str.replace("-", "")
        if "is_open" in df.columns:
            df["is_open"] = df["is_open"].astype(str)
        return df

    def get_daily_bars(self, end_trade_date: str, lookback_days: int = 60) -> pd.DataFrame:
        df = _read_csv(self.paths.get("bars_path", ""))
        if df.empty:
            return df
        if "trade_date" not in df.columns:
            return pd.DataFrame()

        # Normalize both source and filter to tolerate YYYYMMDD / YYYY-MM-DD
        df["_trade_date_norm"] = df["trade_date"].apply(lambda x: normalize_trade_date(x, sep=""))
        df = df[df["_trade_date_norm"] != ""]

        end_td = normalize_trade_date(end_trade_date, sep="")
        if end_td:
            df = df[df["_trade_date_norm"] <= end_td]

        # Keep last N distinct dates based on normalized digits
        dates = sorted(df["_trade_date_norm"].unique().tolist())
        if lookback_days > 0 and len(dates) > lookback_days:
            keep = set(dates[-lookback_days:])
            df = df[df["_trade_date_norm"].isin(keep)]

        # Return canonical YYYY-MM-DD for downstream consumers
        df["trade_date"] = df["_trade_date_norm"].apply(lambda x: normalize_trade_date(x, sep="-"))
        return df.drop(columns=["_trade_date_norm"])

    def get_daily_basic(self, trade_date: str) -> pd.DataFrame:
        df = _read_csv(self.paths.get("daily_basic_path", ""))
        if df.empty:
            return df
        if "trade_date" not in df.columns:
            return pd.DataFrame()

        df["_trade_date_norm"] = df["trade_date"].apply(lambda x: normalize_trade_date(x, sep=""))
        df = df[df["_trade_date_norm"] != ""]
        target = normalize_trade_date(trade_date, sep="")
        if target:
            df = df[df["_trade_date_norm"] == target]
        df["trade_date"] = df["_trade_date_norm"].apply(lambda x: normalize_trade_date(x, sep="-"))
        return df.drop(columns=["_trade_date_norm"]).copy()

    def get_auction_quotes(self, trade_date: str) -> pd.DataFrame:
        df = _read_csv(self.paths.get("auction_path", ""))
        if df.empty:
            return df
        if "trade_date" not in df.columns:
            return pd.DataFrame()

        df["_trade_date_norm"] = df["trade_date"].apply(lambda x: normalize_trade_date(x, sep=""))
        df = df[df["_trade_date_norm"] != ""]
        target = normalize_trade_date(trade_date, sep="")
        if target:
            df = df[df["_trade_date_norm"] == target]
        df["trade_date"] = df["_trade_date_norm"].apply(lambda x: normalize_trade_date(x, sep="-"))
        return df.drop(columns=["_trade_date_norm"]).copy()

    def get_ptrade_exports(self) -> Dict[str, str]:
        return {
            "positions": resolve_path(self.paths.get("ptrade_positions_path", "")),
            "asset": resolve_path(self.paths.get("ptrade_asset_path", "")),
            "exec_report": resolve_path(self.paths.get("exec_report_path", "")),
        }
