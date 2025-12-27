from __future__ import annotations

import sqlite3
from typing import Iterable, List

import pandas as pd


def upsert_df(conn: sqlite3.Connection, table: str, df: pd.DataFrame, pk_cols: List[str]) -> None:
    """SQLite UPSERT (insert or replace) row-by-row.

    This is slower than executemany for huge tables but fine for daily TopM/TopN.
    """
    if df is None or df.empty:
        return

    cols = list(df.columns)
    placeholders = ", ".join(["?"] * len(cols))
    col_sql = ", ".join(cols)

    # Use REPLACE to keep idempotent behaviour with PRIMARY KEY
    sql = f"INSERT OR REPLACE INTO {table} ({col_sql}) VALUES ({placeholders})"
    conn.executemany(sql, df.itertuples(index=False, name=None))
