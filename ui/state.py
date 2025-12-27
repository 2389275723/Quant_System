from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Optional

from src.core.config import load_cfg, get
from src.core.paths import resolve_from_cfg
from src.storage.sqlite import connect
from src.storage.schema import ensure_schema


def read_system_state(conn: sqlite3.Connection) -> Dict[str, str]:
    rows = conn.execute("SELECT k, v FROM system_state").fetchall()
    return {r["k"]: r["v"] for r in rows}


def read_last_execution(conn: sqlite3.Connection) -> Dict[str, Any]:
    row = conn.execute(
        """SELECT run_id, job, trade_date, status, error_msg, started_at, finished_at
             FROM execution_log
             ORDER BY started_at DESC
             LIMIT 1"""
    ).fetchone()
    return dict(row) if row else {}


def check_ptrade_heartbeat(inbox_dir: str, stale_sec: int) -> bool:
    hb = Path(inbox_dir) / "ptrade_heartbeat.json"
    if not hb.exists():
        return False
    try:
        mtime = hb.stat().st_mtime
        return (time.time() - mtime) <= stale_sec
    except Exception:
        return False


def get_status(cfg_path: str = "config/config.yaml") -> Dict[str, Any]:
    cfg = load_cfg(cfg_path)
    db_path = str(resolve_from_cfg(cfg_path, get(cfg, "paths.db_path")))
    inbox_dir = get(cfg, "paths.inbox_dir")
    stop_file = get(cfg, "paths.stop_file")
    stale = int(get(cfg, "ui.ptrade_heartbeat_stale_sec", 120))

    status: Dict[str, Any] = {}
    status["kill_switch"] = os.path.exists(stop_file)

    # DB status
    try:
        conn = connect(db_path)
        ensure_schema(conn)
        st = read_system_state(conn)
        status["phase"] = st.get("phase", "IDLE")
        status["last_error"] = st.get("last_error")
        status["regime"] = st.get("regime")
        status["strength_gate"] = st.get("strength_gate")
        status["last_factpack_json"] = st.get("last_factpack_json")
        status["last_orders_path"] = st.get("last_orders_path")
        status["db_ok"] = True

        status["last_exec"] = read_last_execution(conn)
    except Exception as e:
        status["db_ok"] = False
        status["db_error"] = str(e)
        status["phase"] = "IDLE"
    finally:
        try:
            conn.close()
        except Exception:
            pass

    status["ptrade_heartbeat_ok"] = check_ptrade_heartbeat(inbox_dir, stale)
    token = os.getenv("TUSHARE_TOKEN","").strip()
    # Green only when token is provided (and tushare installed).
    if not token:
        status["tushare_ok"] = False
    else:
        try:
            import tushare  # noqa: F401
            status["tushare_ok"] = True
        except Exception:
            status["tushare_ok"] = False

    return status
