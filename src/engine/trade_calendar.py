from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd

from ..core.timeutil import now_cn

class TradingCalendar:
    def __init__(self, cfg: Dict[str, Any], trade_cal_df: pd.DataFrame | None):
        self.cfg = cfg
        self.strict = bool((cfg.get("trade_cal", {}) or {}).get("strict", True))
        self.tz = (cfg.get("trade_cal", {}) or {}).get("timezone", "Asia/Shanghai")
        self.trade_cal_df = trade_cal_df if trade_cal_df is not None else pd.DataFrame()
        self._open_set = set()
        if not self.trade_cal_df.empty and "cal_date" in self.trade_cal_df.columns and "is_open" in self.trade_cal_df.columns:
            for _, r in self.trade_cal_df.iterrows():
                if str(r["is_open"]) in ("1", "Y", "y", "true", "True"):
                    self._open_set.add(str(r["cal_date"]).replace("-", ""))

    def today(self) -> str:
        return now_cn(self.tz).strftime("%Y%m%d")

    def is_trade_day(self, yyyymmdd: str) -> bool:
        d = str(yyyymmdd)
        if self._open_set:
            return d in self._open_set

        # no trade_cal
        if self.strict:
            return False

        # dev fallback only
        try:
            dt = datetime.strptime(d, "%Y%m%d")
            return dt.weekday() < 5
        except Exception:
            return False
