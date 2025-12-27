from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Tuple

import pandas as pd

from ..core.timeutil import fmt_ts, now_cn
from .gates import asset_check as asset_check_fn

def read_real_asset(asset_csv_path: str) -> Dict[str, Any]:
    try:
        df = pd.read_csv(asset_csv_path)
        # try common fields: total_assets, cash, market_value
        row = df.iloc[0].to_dict() if not df.empty else {}
        # normalize
        out = {}
        for k in row:
            out[k] = row[k]
        return out
    except Exception:
        return {}

def read_real_positions(positions_csv_path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(positions_csv_path, dtype={"ts_code": str, "symbol": str})
        return df
    except Exception:
        return pd.DataFrame()

class ReconciliationEngine:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg

    def run_asset_check_and_log(self, conn, trade_date: str, config_hash: str, run_id: str,
                                expected_total_assets: float, real_total_assets: float) -> Tuple[bool, float, str]:
        ok, dev_ratio, detail = asset_check_fn(expected_total_assets, real_total_assets, self.cfg)
        conn.execute(
            """INSERT OR REPLACE INTO reconciliation_log(
                   trade_date, run_id, config_hash, expected_total_assets, real_total_assets,
                   dev_ratio, ok, detail, created_at
               ) VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                str(trade_date), run_id, config_hash,
                float(expected_total_assets) if expected_total_assets is not None else None,
                float(real_total_assets) if real_total_assets is not None else None,
                float(dev_ratio), 1 if ok else 0,
                detail, fmt_ts(now_cn())
            )
        )
        conn.commit()
        return ok, dev_ratio, detail
