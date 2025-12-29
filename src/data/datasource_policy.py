from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from ..core.config import resolve_path
from .manual_csv import ManualCSVSource
from .tushare_proxy import TushareProxySource

class DataSourcePolicy:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        mode = (cfg.get("data_source", {}) or {}).get("mode", "manual_csv")
        self.mode = mode

        if mode == "manual_csv":
            self.src = ManualCSVSource(cfg)
        elif mode == "cache_only":
            # cache_only uses manual CSV readers but only reads local cache paths
            self.src = ManualCSVSource(cfg)
        elif mode in ("tushare_proxy", "tushare_official"):
            self.src = TushareProxySource(cfg, official=(mode == "tushare_official"))
        else:
            raise ValueError(f"Unknown data_source.mode={mode}")

    def get_trade_cal(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        try:
            return self.src.get_trade_cal(start_date=start_date, end_date=end_date)
        except TypeError:
            # backward compatibility with sources that don't accept date range
            return self.src.get_trade_cal()

    def get_daily_bars(self, end_trade_date: str, lookback_days: int = 60) -> pd.DataFrame:
        return self.src.get_daily_bars(end_trade_date=end_trade_date, lookback_days=lookback_days)

    def get_daily_basic(self, trade_date: str) -> pd.DataFrame:
        return self.src.get_daily_basic(trade_date=trade_date)

    def get_universe_files(self) -> Tuple[str, str]:
        ds_cfg = self.cfg.get("data_source", {}).get("manual_csv", {}) or {}
        idx300 = resolve_path(ds_cfg.get("idx300_path", ""))
        idx500 = resolve_path(ds_cfg.get("idx500_path", ""))
        return idx300, idx500

    def get_auction_quotes(self, trade_date: str) -> pd.DataFrame:
        return self.src.get_auction_quotes(trade_date=trade_date)

    def get_ptrade_exports(self) -> Dict[str, str]:
        return self.src.get_ptrade_exports()
