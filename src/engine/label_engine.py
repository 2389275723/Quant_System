from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

def _mdd_from_prices(prices: pd.Series) -> float:
    if prices is None or prices.dropna().empty:
        return float("nan")
    peak = prices.cummax()
    dd = prices / (peak + 1e-12) - 1.0
    return float(dd.min())

class LabelEngine:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg

    def fill_labels(self, conn, bars: pd.DataFrame, asof_date: str, config_hash: str, run_id: str) -> None:
        """Compute y3/y7 labels for a given asof_date and store into label_daily.

        防未来：Universe membership 固定为 asof_date 当日 snapshot_raw 的 universe_flag=1。
        """
        if bars is None or bars.empty:
            return

        asof_date = str(asof_date)
        bars = bars.copy()
        bars["trade_date"] = bars["trade_date"].astype(str).str.replace("-", "")
        bars = bars.sort_values(["ts_code","trade_date"])

        # trading dates list from all bars (market calendar proxy)
        dates = sorted(bars["trade_date"].unique().tolist())
        if asof_date not in dates:
            return
        idx = dates.index(asof_date)
        if idx + 7 >= len(dates):
            return  # not enough future days

        d1 = dates[idx + 1]
        d3 = dates[idx + 3]
        d7 = dates[idx + 7]

        # Universe fixed at asof_date
        uni = pd.read_sql_query(
            "SELECT ts_code FROM snapshot_raw WHERE trade_date=? AND config_hash=? AND universe_flag=1",
            conn,
            params=(asof_date, config_hash),
        )
        if uni.empty:
            return
        uni_codes = set(uni["ts_code"].astype(str).tolist())

        # pivot for quick access
        cols_need = ["ts_code","trade_date","open","close"]
        use = bars[[c for c in cols_need if c in bars.columns]].copy()
        for c in ["open","close"]:
            if c in use.columns:
                use[c] = pd.to_numeric(use[c], errors="coerce")
        # Build dict: (code,date)->(open,close)
        # For performance, pivot
        open_p = use.pivot(index="trade_date", columns="ts_code", values="open")
        close_p = use.pivot(index="trade_date", columns="ts_code", values="close")

        records = []
        # For universe eq return, only include codes with coverage
        ret3_list = []
        ret7_list = []

        for code in sorted(uni_codes):
            cov3 = 1
            cov7 = 1
            o1 = open_p.get(code, pd.Series(dtype=float)).get(d1, np.nan)
            c3 = close_p.get(code, pd.Series(dtype=float)).get(d3, np.nan)
            c7 = close_p.get(code, pd.Series(dtype=float)).get(d7, np.nan)

            ret3 = np.nan
            ret7 = np.nan
            mdd3 = np.nan
            mdd7 = np.nan

            if pd.isna(o1) or pd.isna(c3):
                cov3 = 0
            else:
                ret3 = float(c3 / (o1 + 1e-12) - 1.0)
                # prices from d1..d3
                px = close_p.get(code, pd.Series(dtype=float)).reindex([d1, dates[idx+2], d3]).astype(float)
                mdd3 = _mdd_from_prices(px)

            if pd.isna(o1) or pd.isna(c7):
                cov7 = 0
            else:
                ret7 = float(c7 / (o1 + 1e-12) - 1.0)
                # prices from d1..d7
                px = close_p.get(code, pd.Series(dtype=float)).reindex(dates[idx+1:idx+8]).astype(float)
                mdd7 = _mdd_from_prices(px)

            if cov3 == 1:
                ret3_list.append(ret3)
            if cov7 == 1:
                ret7_list.append(ret7)

            records.append({
                "asof_date": asof_date,
                "ts_code": code,
                "ret_3d": None if cov3 == 0 else ret3,
                "ret_7d": None if cov7 == 0 else ret7,
                "coverage_3d": cov3,
                "coverage_7d": cov7,
                "mdd_3d": None if cov3 == 0 else mdd3,
                "mdd_7d": None if cov7 == 0 else mdd7,
            })

        # Universe equal-weight baseline
        uni_eq_3 = float(np.nanmean(ret3_list)) if ret3_list else np.nan
        uni_eq_7 = float(np.nanmean(ret7_list)) if ret7_list else np.nan

        for r in records:
            if r["coverage_3d"] == 1 and r["ret_3d"] is not None and pd.notna(uni_eq_3):
                r["excess_3d"] = float(r["ret_3d"] - uni_eq_3)
            else:
                r["excess_3d"] = None
            if r["coverage_7d"] == 1 and r["ret_7d"] is not None and pd.notna(uni_eq_7):
                r["excess_7d"] = float(r["ret_7d"] - uni_eq_7)
            else:
                r["excess_7d"] = None

        # upsert labels (idempotent for same config_hash)
        cur = conn.cursor()
        for r in records:
            cur.execute(
                """INSERT OR REPLACE INTO label_daily(
                       asof_date, ts_code, ret_3d, ret_7d, excess_3d, excess_7d,
                       mdd_3d, mdd_7d, coverage_3d, coverage_7d, run_id, config_hash
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    r["asof_date"], r["ts_code"],
                    r.get("ret_3d"), r.get("ret_7d"),
                    r.get("excess_3d"), r.get("excess_7d"),
                    r.get("mdd_3d"), r.get("mdd_7d"),
                    r.get("coverage_3d", 0), r.get("coverage_7d", 0),
                    run_id, config_hash
                )
            )
        conn.commit()
