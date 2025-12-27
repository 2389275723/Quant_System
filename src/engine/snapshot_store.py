from __future__ import annotations

from typing import Any, Dict
import sqlite3
import pandas as pd

def insert_snapshot_raw(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    cols = [
        "trade_date","ts_code","name","industry",
        "open","high","low","close","pct_chg","vol","amount","turnover_rate","circ_mv","total_mv",
        "universe_flag","filter_flags",
        "f_ma20","f_ma20_range","f_rsi6","f_ret20","f_near_high","f_surge5",
        "score_rule","trend_score","fund_score","flow_score","rank_rule",
        "source_name","source_url","ingest_time","run_id","strategy_id","strategy_version","config_hash","payload_hash",
    ]
    cols = [c for c in cols if c in df.columns]
    data = df[cols].to_dict(orient="records")

    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT OR IGNORE INTO snapshot_raw({','.join(cols)}) VALUES({placeholders})"
    cur = conn.cursor()
    for r in data:
        cur.execute(sql, tuple(r.get(c) for c in cols))
    conn.commit()
    return cur.rowcount

def insert_picks_daily(conn: sqlite3.Connection, df: pd.DataFrame, trade_date: str, run_id: str, config_hash: str) -> None:
    if df is None or df.empty:
        return
    use = df.copy()
    use["trade_date"] = str(trade_date)
    use["run_id"] = run_id
    use["config_hash"] = config_hash

    cols = [
        "trade_date","ts_code","rank","rank_rule","final_score","score_rule","trend_score","fund_score","flow_score","filter_flags",
        "risk_gate_action","risk_prob","risk_severity","disagreement","alpha_score","confidence",
        "run_id","config_hash",
    ]
    cols = [c for c in cols if c in use.columns]
    data = use[cols].to_dict(orient="records")

    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT OR REPLACE INTO picks_daily({','.join(cols)}) VALUES({placeholders})"
    cur = conn.cursor()
    for r in data:
        cur.execute(sql, tuple(r.get(c) for c in cols))
    conn.commit()

def insert_model_scores(conn: sqlite3.Connection, trade_date: str, df: pd.DataFrame, provider: str,
                        run_id: str, config_hash: str, prompt_hash: str, model_name: str, degraded_reason: str) -> None:
    if df is None or df.empty:
        return
    use = df.copy()
    use["trade_date"] = str(trade_date)
    use["provider"] = provider
    use["run_id"] = run_id
    use["config_hash"] = config_hash
    use["prompt_hash"] = prompt_hash
    use["model_name"] = model_name
    use["degraded_reason"] = degraded_reason

    cols = ["trade_date","ts_code","provider","alpha_score","risk_prob","risk_severity","risk_flags",
            "confidence","prompt_hash","model_name","degraded_reason","ingest_time","run_id","config_hash"]
    if "ts_code" not in use.columns:
        # align by index later outside
        return
    from ..core.timeutil import fmt_ts, now_cn
    use["ingest_time"] = fmt_ts(now_cn())

    data = use[cols].to_dict(orient="records")
    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT OR REPLACE INTO model_scores_daily({','.join(cols)}) VALUES({placeholders})"
    cur = conn.cursor()
    for r in data:
        # risk_flags as json string
        rf = r.get("risk_flags")
        if isinstance(rf, (list, dict)):
            import json
            r["risk_flags"] = json.dumps(rf, ensure_ascii=False)
        cur.execute(sql, tuple(r.get(c) for c in cols))
    conn.commit()

def insert_targets(conn: sqlite3.Connection, trade_date: str, targets: pd.DataFrame, run_id: str, config_hash: str) -> None:
    if targets is None or targets.empty:
        return
    use = targets.copy()
    use["trade_date"] = str(trade_date)
    use["run_id"] = run_id
    use["config_hash"] = config_hash
    cols = ["trade_date","ts_code","target_weight","rank","final_score","reason","run_id","config_hash"]
    cols = [c for c in cols if c in use.columns]
    data = use[cols].to_dict(orient="records")
    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT OR REPLACE INTO targets_daily({','.join(cols)}) VALUES({placeholders})"
    cur = conn.cursor()
    for r in data:
        cur.execute(sql, tuple(r.get(c) for c in cols))
    conn.commit()

def insert_orders(conn: sqlite3.Connection, trade_date: str, orders: list[dict], run_id: str, config_hash: str) -> None:
    if not orders:
        return
    from ..core.timeutil import fmt_ts, now_cn
    created_at = fmt_ts(now_cn())
    cols = ["trade_date","client_order_id","ts_code","side","qty","price_type","limit_price",
            "reason","risk_tags","run_id","config_hash","created_at"]
    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT OR REPLACE INTO orders_daily({','.join(cols)}) VALUES({placeholders})"
    cur = conn.cursor()
    for o in orders:
        cur.execute(sql, (
            str(trade_date),
            o.get("client_order_id"),
            o.get("ts_code"),
            o.get("side"),
            int(o.get("qty")),
            o.get("price_type"),
            o.get("limit_price"),
            o.get("reason",""),
            o.get("risk_tags",""),
            run_id,
            config_hash,
            created_at
        ))
    conn.commit()
