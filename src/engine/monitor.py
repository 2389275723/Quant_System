from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict

import pandas as pd

from src.storage.schema import safe_rank_column


def build_factpack(conn: sqlite3.Connection, trade_date: str, config_hash: str) -> Dict[str, Any]:
    rank_col = safe_rank_column(conn)

    df = pd.read_sql_query(
        f"""SELECT trade_date, ts_code, name, industry, score_rule, trend_score, fund_score, flow_score,
                  final_score, {rank_col} AS rank_final
             FROM picks_daily
             WHERE trade_date=? AND config_hash=?
             ORDER BY {rank_col} ASC
        """,
        conn,
        params=(trade_date, config_hash),
    )
    topn = df.head(5).to_dict(orient="records")
    pack: Dict[str, Any] = {
        "trade_date": trade_date,
        "config_hash": config_hash,
        "topn": topn,
        "count": int(df.shape[0]),
        "final_score_mean": float(df["final_score"].mean()) if not df.empty else None,
        "final_score_std": float(df["final_score"].std(ddof=0)) if not df.empty else None,
    }
    # save into system_state as json for UI consumption
    conn.execute(
        "INSERT INTO system_state(k, v, updated_at) VALUES('last_factpack_json', ?, datetime('now')) "
        "ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_at=excluded.updated_at",
        (json.dumps(pack, ensure_ascii=False),),
    )
    return pack
