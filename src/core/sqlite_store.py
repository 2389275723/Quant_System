from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable, Tuple


def _table_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    """Return existing columns of a sqlite table."""
    cur = conn.execute(f"PRAGMA table_info({table});")
    rows = cur.fetchall() or []
    # sqlite row format: (cid, name, type, notnull, dflt_value, pk)
    return {str(r[1]) for r in rows}


def ensure_columns(conn: sqlite3.Connection, table: str, columns: Iterable[tuple[str, str]]) -> None:
    """Add missing columns to an existing table.

    This is a minimal V1 'schema migrator' to prevent the UI/monitor pipeline
    from breaking when we evolve the schema.
    """
    existing = _table_cols(conn, table)
    for col, col_type in columns:
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type};")
    conn.commit()

def connect_sqlite(db_path: str, wal: bool = True, busy_timeout_ms: int = 5000) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)};")
    conn.execute("PRAGMA foreign_keys=ON;")
    if wal:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_schema(conn: sqlite3.Connection) -> None:
    """Create V1 schema (append-only snapshot_raw + execution artifacts)."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS snapshot_raw (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            name TEXT,
            industry TEXT,
            open REAL, high REAL, low REAL, close REAL,
            pct_chg REAL,
            vol REAL, amount REAL,
            turnover_rate REAL,
            circ_mv REAL, total_mv REAL,

            universe_flag INTEGER DEFAULT 0,
            filter_flags TEXT,

            -- atomic factors (wide table)
            f_ma20 REAL,
            f_ma20_range REAL,
            f_rsi6 REAL,
            f_ret20 REAL,
            f_near_high REAL,
            f_surge5 REAL,

            -- rule scores
            score_rule REAL,
            trend_score REAL,
            fund_score REAL,
            flow_score REAL,
            rank_rule INTEGER,

            -- audit
            source_name TEXT,
            source_url TEXT,
            ingest_time TEXT,
            run_id TEXT,
            strategy_id TEXT,
            strategy_version TEXT,
            config_hash TEXT,
            payload_hash TEXT,

            UNIQUE(trade_date, ts_code, config_hash)
        );

        CREATE TABLE IF NOT EXISTS label_daily (
            asof_date TEXT NOT NULL,
            ts_code TEXT NOT NULL,

            ret_3d REAL,
            ret_7d REAL,
            excess_3d REAL,
            excess_7d REAL,
            mdd_3d REAL,
            mdd_7d REAL,

            coverage_3d INTEGER DEFAULT 1,
            coverage_7d INTEGER DEFAULT 1,

            run_id TEXT,
            config_hash TEXT,
            PRIMARY KEY(asof_date, ts_code, config_hash)
        );

        CREATE TABLE IF NOT EXISTS picks_daily (
            trade_date TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            rank INTEGER NOT NULL,
            -- rank produced by pure rule_score (for transparency / debugging)
            rank_rule INTEGER,
            final_score REAL,
            score_rule REAL,
            trend_score REAL,
            fund_score REAL,
            flow_score REAL,
            filter_flags TEXT,
            risk_gate_action TEXT,
            risk_prob REAL,
            risk_severity INTEGER,
            disagreement REAL,
            alpha_score REAL,
            confidence REAL,
            run_id TEXT,
            config_hash TEXT,
            PRIMARY KEY(trade_date, ts_code, config_hash)
        );

        CREATE TABLE IF NOT EXISTS model_scores_daily (
            trade_date TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            provider TEXT NOT NULL,
            alpha_score REAL,
            risk_prob REAL,
            risk_severity INTEGER,
            risk_flags TEXT,
            confidence REAL,
            prompt_hash TEXT,
            model_name TEXT,
            degraded_reason TEXT,
            ingest_time TEXT,
            run_id TEXT,
            config_hash TEXT,
            PRIMARY KEY(trade_date, ts_code, provider, config_hash)
        );

        CREATE TABLE IF NOT EXISTS targets_daily (
            trade_date TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            target_weight REAL,
            rank INTEGER,
            final_score REAL,
            reason TEXT,
            run_id TEXT,
            config_hash TEXT,
            PRIMARY KEY(trade_date, ts_code, config_hash)
        );

        CREATE TABLE IF NOT EXISTS orders_daily (
            trade_date TEXT NOT NULL,
            client_order_id TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            side TEXT NOT NULL,
            qty INTEGER NOT NULL,
            price_type TEXT NOT NULL,
            limit_price REAL,
            reason TEXT,
            risk_tags TEXT,
            run_id TEXT,
            config_hash TEXT,
            created_at TEXT,
            PRIMARY KEY(trade_date, client_order_id, config_hash)
        );

        CREATE TABLE IF NOT EXISTS order_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            client_order_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload TEXT,
            event_time TEXT,
            run_id TEXT,
            config_hash TEXT
        );

        CREATE TABLE IF NOT EXISTS reconciliation_log (
            trade_date TEXT NOT NULL,
            run_id TEXT,
            config_hash TEXT,
            expected_total_assets REAL,
            real_total_assets REAL,
            dev_ratio REAL,
            ok INTEGER,
            detail TEXT,
            created_at TEXT,
            PRIMARY KEY(trade_date, config_hash)
        );

        CREATE TABLE IF NOT EXISTS execution_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT,
            stage TEXT,
            run_id TEXT,
            strategy_id TEXT,
            config_hash TEXT,
            code_hash TEXT,
            model_version TEXT,
            prompt_hash TEXT,
            started_at TEXT,
            finished_at TEXT,
            ok INTEGER,
            detail TEXT
        );

        CREATE TABLE IF NOT EXISTS system_status (
            k TEXT PRIMARY KEY,
            v TEXT,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS factpack_daily (
            trade_date TEXT PRIMARY KEY,
            payload_json TEXT,
            created_at TEXT,
            run_id TEXT,
            config_hash TEXT
        );
        """
    )
    conn.commit()

    # --- minimal migrations (V1 safety) ---
    # Older DBs may miss newly added columns. We add them in-place to avoid runtime crashes.
    ensure_columns(conn, "picks_daily", [("rank_rule", "INTEGER")])

    # Backfill: if rank_rule is NULL (old rows), fall back to final rank.
    try:
        conn.execute("UPDATE picks_daily SET rank_rule = rank WHERE rank_rule IS NULL")
        conn.commit()
    except Exception:
        # table might not exist in some edge cases, ignore
        pass

def upsert_status(conn: sqlite3.Connection, k: str, v: str, updated_at: str) -> None:
    conn.execute(
        """INSERT INTO system_status(k, v, updated_at)
              VALUES(?,?,?)
              ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_at=excluded.updated_at
        """,
        (k, v, updated_at),
    )
    conn.commit()

def get_status(conn: sqlite3.Connection, k: str, default: str = "") -> str:
    cur = conn.execute("SELECT v FROM system_status WHERE k=? LIMIT 1", (k,))
    row = cur.fetchone()
    return row["v"] if row else default

def query_df(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> list[dict]:
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    return [dict(r) for r in rows]
