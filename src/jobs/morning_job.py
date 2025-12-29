from __future__ import annotations


# --- AUTO_PATCH_FILLNA_SCALAR_GUARD_2025_12_29
def _num_scalar(x, fill=0.0):
    """Scalar-safe numeric conversion (no .fillna on scalars)."""
    import pandas as pd
    y = pd.to_numeric(x, errors="coerce")
    try:
        return fill if pd.isna(y) else float(y)
    except Exception:
        return fill


def _num_col(df, col, fill=0.0, default=None):
    """Series-safe numeric column getter.

    If df has the column: returns numeric Series with .fillna(fill)
    If missing: returns constant Series aligned to df.index (default if provided else fill)
    """
    import pandas as pd
    if hasattr(df, "columns") and hasattr(df, "index") and col in getattr(df, "columns"):
        return pd.to_numeric(df[col], errors="coerce").fillna(fill)

    idx = getattr(df, "index", None)
    const = fill if default is None else default
    if idx is None:
        return pd.Series([const], dtype="float64")
    return pd.Series(const, index=idx, dtype="float64")


def _num_any(x, fill=0.0):
    """Generic numeric conversion that works for Series or scalar."""
    import pandas as pd
    y = pd.to_numeric(x, errors="coerce")
    if hasattr(y, "fillna"):
        return y.fillna(fill)
    try:
        return fill if pd.isna(y) else float(y)
    except Exception:
        return fill
# --- END AUTO_PATCH_FILLNA_SCALAR_GUARD_2025_12_29

import sqlite3
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from src.core.config import load_cfg, get
from src.core.paths import resolve_from_cfg
from src.core.hashing import stable_hash_dict, file_hash
from src.core.timeutil import make_run_id, now_cn, today_cn
from src.storage.sqlite import connect
from src.storage.schema import ensure_schema, safe_rank_column
from src.storage.upsert import upsert_df
from src.engine.strength import strength_gate
from src.bridge.gates import kill_switch, fat_finger_check
from src.bridge.orders import export_orders_csv


def _set_state(conn: sqlite3.Connection, k: str, v: str) -> None:
    conn.execute(
        "INSERT INTO system_state(k, v, updated_at) VALUES(?, ?, datetime('now')) "
        "ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_at=excluded.updated_at",
        (k, v),
    )


def _latest_trade_date(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute("SELECT MAX(trade_date) AS d FROM picks_daily").fetchone()
    return row["d"] if row and row["d"] else None


def run_morning_job(cfg_path: str = "config/config.yaml", trade_date: Optional[str] = None) -> Dict[str, Any]:
    cfg = load_cfg(cfg_path)
    db_path = str(resolve_from_cfg(cfg_path, get(cfg, "paths.db_path")))
    outbox_dir = get(cfg, "paths.outbox_dir")
    stop_file = get(cfg, "paths.stop_file")

    run_id = make_run_id("MORN")
    config_hash = stable_hash_dict(cfg)
    code_hash = file_hash("main.py")

    conn = connect(db_path)
    ensure_schema(conn)

    started_at = now_cn().isoformat(timespec="seconds")
    _set_state(conn, "phase", "MORNING_JOB")
    _set_state(conn, "last_run_id", run_id)

    conn.execute(
        "INSERT OR REPLACE INTO execution_log(run_id, job, trade_date, status, error_code, error_msg, started_at, finished_at, config_hash, code_hash) "
        "VALUES(?, 'MORNING', ?, 'RUNNING', NULL, NULL, ?, NULL, ?, ?)",
        (run_id, trade_date or "", started_at, config_hash, code_hash),
    )

    try:
        if trade_date is None:
            # default to latest picks date, else today
            trade_date = _latest_trade_date(conn) or today_cn()

        rank_col = safe_rank_column(conn)

        picks = pd.read_sql_query(
            f"""SELECT ts_code, name, industry, final_score, {rank_col} AS rank_final
                FROM picks_daily
                WHERE trade_date=? AND config_hash=?
                ORDER BY {rank_col} ASC
            """,
            conn,
            params=(trade_date, config_hash),
        )

        if picks.empty:
            # fallback: ignore config_hash (user might have changed config)
            picks = pd.read_sql_query(
                f"""SELECT ts_code, name, industry, final_score, {rank_col} AS rank_final
                    FROM picks_daily
                    WHERE trade_date=?
                    ORDER BY {rank_col} ASC
                """,
                conn,
                params=(trade_date,),
            )

        if picks.empty:
            raise RuntimeError(f"No picks found for trade_date={trade_date}. Run Night Job first.")

        top_n = int(get(cfg, "scoring.top_n", 5))
        picks_n = picks.head(top_n).copy()

        # V1.5 strength gate
        if bool(get(cfg, "v1_5.enable_strength_gate", True)):
            min_score = float(get(cfg, "v1_5.strength_gate_min_final_score", 0.15))
            sd = strength_gate(picks_n, min_final_score=min_score)
            _set_state(conn, "strength_gate", str(sd))
            allow_new = sd.allow_new_positions
            exposure_mult = sd.exposure_multiplier
        else:
            allow_new = True
            exposure_mult = 1.0

        # For a scaffold, generate equal-qty BUY orders.
        # In production you'd read real asset/cash + price and compute target_value.
        orders = []
        if allow_new:
            base_qty = 100  # placeholder
            for _, r in picks_n.iterrows():
                orders.append({
                    "ts_code": r["ts_code"],
                    "side": "BUY",
                    "qty": int(base_qty),
                    "limit_price": "",
                    "notional": "",
                    "reason": "TopN",
                })
        orders_df = pd.DataFrame(orders)

        # Gates
        g1 = kill_switch(stop_file)
        if not g1.ok:
            raise RuntimeError(f"KILL_SWITCH: {g1.details}")

        ff = get(cfg, "sanity.fat_finger", {}) or {}
        g2 = fat_finger_check(
            orders_df,
            max_lines=int(ff.get("max_lines", 30)),
            max_notional_per_order=float(ff.get("max_notional_per_order", 500000)),
        )
        if not g2.ok:
            raise RuntimeError(f"FAT_FINGER: {g2.reason} {g2.details or ''}")

        # Export file (atomic)
        out_path = export_orders_csv(orders_df, outbox_dir=outbox_dir, trade_date=trade_date, run_id=run_id)
        _set_state(conn, "last_orders_path", out_path)

        # Persist orders
        if not orders_df.empty:
            od = orders_df.copy()
            od.insert(0, "trade_date", trade_date)
            od["limit_price"] = pd.to_numeric(od["limit_price"], errors="coerce")
            od["qty"] = _num_any(od["qty"], 0).astype(int)
            od["notional"] = pd.to_numeric(od["notional"], errors="coerce")
            od["status"] = "EXPORTED"
            od["config_hash"] = config_hash
            od["run_id"] = run_id
            od["created_at"] = now_cn().isoformat(timespec="seconds")
            upsert_df(conn, "orders_daily", od, pk_cols=["trade_date","ts_code","side","config_hash"])

        finished_at = now_cn().isoformat(timespec="seconds")
        conn.execute(
            "UPDATE execution_log SET status='OK', trade_date=?, finished_at=? WHERE run_id=?",
            (trade_date, finished_at, run_id),
        )
        _set_state(conn, "phase", "IDLE")
        _set_state(conn, "last_morning_ok", finished_at)

        return {"ok": True, "run_id": run_id, "trade_date": trade_date, "orders_path": out_path}

    except Exception as e:
        finished_at = now_cn().isoformat(timespec="seconds")
        conn.execute(
            "UPDATE execution_log SET status='FAILED', error_code='EXCEPTION', error_msg=?, trade_date=?, finished_at=? WHERE run_id=?",
            (str(e), trade_date or "", finished_at, run_id),
        )
        _set_state(conn, "phase", "IDLE")
        _set_state(conn, "last_error", str(e))
        return {"ok": False, "run_id": run_id, "trade_date": trade_date, "error": str(e)}
    finally:
        conn.close()
