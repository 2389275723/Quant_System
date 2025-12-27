from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional


def connect(db_path: str, timeout_sec: float = 30.0) -> sqlite3.Connection:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), timeout=timeout_sec, isolation_level=None)  # autocommit
    conn.row_factory = sqlite3.Row

    # Concurrency / robustness
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(f"PRAGMA busy_timeout={int(timeout_sec * 1000)};")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn
