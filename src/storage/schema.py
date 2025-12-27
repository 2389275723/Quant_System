from __future__ import annotations

import sqlite3
from typing import Dict, Iterable, List, Tuple


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _columns(conn: sqlite3.Connection, table: str) -> List[str]:
    if not _table_exists(conn, table):
        return []
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]  # name is at index 1


def _ensure_table(conn: sqlite3.Connection, ddl: str) -> None:
    conn.execute(ddl)


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: Dict[str, str]) -> None:
    existing = set(_columns(conn, table))
    for col, col_ddl in columns.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_ddl}")


def ensure_schema(conn: sqlite3.Connection) -> None:
    # ---- meta
    _ensure_table(
        conn,
        """CREATE TABLE IF NOT EXISTS meta (
            k TEXT PRIMARY KEY,
            v TEXT
        );""",
    )

    # ---- system_state (single row by key)
    _ensure_table(
        conn,
        """CREATE TABLE IF NOT EXISTS system_state (
            k TEXT PRIMARY KEY,
            v TEXT,
            updated_at TEXT
        );""",
    )

    # ---- execution_log
    _ensure_table(
        conn,
        """CREATE TABLE IF NOT EXISTS execution_log (
            run_id TEXT PRIMARY KEY,
            job TEXT,
            trade_date TEXT,
            status TEXT,
            error_code TEXT,
            error_msg TEXT,
            started_at TEXT,
            finished_at TEXT,
            config_hash TEXT,
            code_hash TEXT
        );""",
    )


    _ensure_columns(conn, "execution_log", {
        "job": "TEXT",
        "trade_date": "TEXT",
        "status": "TEXT",
        "error_code": "TEXT",
        "error_msg": "TEXT",
        "started_at": "TEXT",
        "finished_at": "TEXT",
        "config_hash": "TEXT",
        "code_hash": "TEXT",
    })

    # ---- picks_daily (this is where your SQL errors came from)
    _ensure_table(
        conn,
        """CREATE TABLE IF NOT EXISTS picks_daily (
            trade_date TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            name TEXT,
            industry TEXT,

            score_rule REAL,
            trend_score REAL,
            fund_score REAL,
            flow_score REAL,

            final_score REAL,

            rank INTEGER,
            rank_rule INTEGER,
            rank_final INTEGER,

            config_hash TEXT NOT NULL,
            run_id TEXT NOT NULL,

            created_at TEXT,

            PRIMARY KEY (trade_date, ts_code, config_hash)
        );""",
    )

    # If your DB is older, add missing columns safely (idempotent)
    _ensure_columns(conn, "picks_daily", {
        "name": "TEXT",
        "industry": "TEXT",
        "score_rule": "REAL",
        "trend_score": "REAL",
        "fund_score": "REAL",
        "flow_score": "REAL",
        "final_score": "REAL",
        "rank": "INTEGER",
        "rank_rule": "INTEGER",
        "rank_final": "INTEGER",
        "final_score_ai": "REAL",
        "rank_ai": "INTEGER",
        "created_at": "TEXT",
        "run_id": "TEXT",
    })

    # Helpful indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_picks_daily_date_hash ON picks_daily(trade_date, config_hash);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_picks_daily_date_rank ON picks_daily(trade_date, rank);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_picks_daily_date_rank_ai ON picks_daily(trade_date, rank_ai);")

    # ---- model_scores_daily (dual-head model outputs; optional)
    _ensure_table(
        conn,
        """CREATE TABLE IF NOT EXISTS model_scores_daily (
            trade_date TEXT NOT NULL,
            ts_code TEXT NOT NULL,

            alpha_ds REAL,
            risk_prob_ds REAL,
            risk_sev_ds INTEGER,
            conf_ds REAL,
            comment_ds TEXT,

            alpha_qw REAL,
            risk_prob_qw REAL,
            risk_sev_qw INTEGER,
            conf_qw REAL,
            comment_qw TEXT,

            alpha_final REAL,
            risk_prob_final REAL,
            risk_sev_final INTEGER,
            disagreement REAL,
            action TEXT,

            config_hash TEXT NOT NULL,
            run_id TEXT NOT NULL,
            created_at TEXT,

            PRIMARY KEY (trade_date, ts_code, config_hash)
        );""",
    )

    _ensure_columns(conn, "model_scores_daily", {
        "alpha_ds": "REAL",
        "risk_prob_ds": "REAL",
        "risk_sev_ds": "INTEGER",
        "conf_ds": "REAL",
        "comment_ds": "TEXT",
        "alpha_qw": "REAL",
        "risk_prob_qw": "REAL",
        "risk_sev_qw": "INTEGER",
        "conf_qw": "REAL",
        "comment_qw": "TEXT",
        "alpha_final": "REAL",
        "risk_prob_final": "REAL",
        "risk_sev_final": "INTEGER",
        "disagreement": "REAL",
        "action": "TEXT",
        "created_at": "TEXT",
    })

    # ---- market_context_daily (news/market snapshot to feed LLM)
    _ensure_table(
        conn,
        """CREATE TABLE IF NOT EXISTS market_context_daily (
            trade_date TEXT NOT NULL,
            run_id TEXT NOT NULL,
            config_hash TEXT NOT NULL,
            ctx_json TEXT,
            created_at TEXT,
            PRIMARY KEY (trade_date, run_id, config_hash)
        );""",
    )

    _ensure_columns(conn, "market_context_daily", {
        "ctx_json": "TEXT",
        "created_at": "TEXT",
    })

    # ---- orders_daily (exported orders)
    _ensure_table(
        conn,
        """CREATE TABLE IF NOT EXISTS orders_daily (
            trade_date TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            side TEXT NOT NULL,          -- BUY/SELL
            qty INTEGER NOT NULL,
            limit_price REAL,
            notional REAL,
            reason TEXT,
            status TEXT,                -- PLANNED/EXPORTED/PROCESSED
            config_hash TEXT NOT NULL,
            run_id TEXT NOT NULL,
            created_at TEXT,
            PRIMARY KEY (trade_date, ts_code, side, config_hash)
        );""",
    )

    # ---- reconciliation_log
    _ensure_table(
        conn,
        """CREATE TABLE IF NOT EXISTS reconciliation_log (
            trade_date TEXT NOT NULL,
            run_id TEXT NOT NULL,
            status TEXT,
            msg TEXT,
            created_at TEXT,
            PRIMARY KEY (trade_date, run_id)
        );""",
    )


def safe_rank_column(conn: sqlite3.Connection) -> str:
    cols = set(_columns(conn, "picks_daily"))
    if "rank_final" in cols:
        return "rank_final"
    if "rank" in cols:
        return "rank"
    if "rank_rule" in cols:
        return "rank_rule"
    # fallback: use rowid
    return "rowid"
