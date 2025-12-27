from __future__ import annotations

import logging
import os
import traceback
from typing import Any, Dict

import pandas as pd

from ..core.timeutil import now_cn, fmt_ts
from ..core.sqlite_store import upsert_status
from ..data.datasource_policy import DataSourcePolicy
from ..engine.trade_calendar import TradingCalendar
from ..engine.reconciliation import read_real_asset, ReconciliationEngine
from ..core.config import resolve_path

from ..engine.gates import kill_switch_active

def _gen_run_id(trade_date: str) -> str:
    return f"{trade_date}_{now_cn().strftime('%H%M%S')}"

def run_close_job(conn, cfg: Dict[str, Any], cfg_hash: str, code_hash: str) -> None:
    strategy = cfg.get("strategy", {}) or {}
    strategy_id = strategy.get("strategy_id", "STRAT")

    ds = DataSourcePolicy(cfg)
    trade_cal_df = ds.get_trade_cal()
    cal = TradingCalendar(cfg, trade_cal_df)

    trade_date = os.environ.get('QUANT_TRADE_DATE', '') or cal.today()
    run_id = _gen_run_id(trade_date)
    started_at = fmt_ts(now_cn())

    try:
        if not cal.is_trade_day(trade_date):
            upsert_status(conn, "last_run_msg", f"CLOSE_JOB NOT_TRADE_DAY: {trade_date}", fmt_ts(now_cn()))
            return

        paths = ds.get_ptrade_exports()
        real_asset = read_real_asset(paths.get("asset",""))
        real_total_assets = None
        for k in ["total_assets","total_asset","total"]:
            if k in real_asset:
                try:
                    real_total_assets = float(real_asset[k])
                    break
                except Exception:
                    pass
        if real_total_assets is None:
            # best effort: any numeric
            for v in real_asset.values():
                try:
                    f = float(v)
                    if f > 0:
                        real_total_assets = f
                        break
                except Exception:
                    pass

        if real_total_assets is not None:
            upsert_status(conn, "expected_total_assets", f"{real_total_assets:.2f}", fmt_ts(now_cn()))

        conn.execute(
            """INSERT INTO execution_log(trade_date, stage, run_id, strategy_id, config_hash, code_hash,
                   started_at, finished_at, ok, detail)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (trade_date, "CLOSE_JOB", run_id, strategy_id, cfg_hash, code_hash, started_at, fmt_ts(now_cn()), 1, "OK")
        )
        conn.commit()

        upsert_status(conn, "last_run_msg", f"CLOSE_JOB OK: {trade_date} total_assets={real_total_assets}", fmt_ts(now_cn()))

    except Exception as e:
        detail = traceback.format_exc()
        logging.error("Close Job FAILED: %s\n%s", e, detail)
        conn.execute(
            """INSERT INTO execution_log(trade_date, stage, run_id, strategy_id, config_hash, code_hash,
                   started_at, finished_at, ok, detail)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (trade_date, "CLOSE_JOB", run_id, strategy_id, cfg_hash, code_hash, started_at, fmt_ts(now_cn()), 0, detail)
        )
        conn.commit()
        upsert_status(conn, "last_run_msg", f"CLOSE_JOB FAILED: {e}", fmt_ts(now_cn()))
