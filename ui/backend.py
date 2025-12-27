from __future__ import annotations

from typing import Any, Dict, Tuple
import streamlit as st

from src.core.config import load_yaml, config_hash, resolve_path
from src.core.sqlite_store import connect_sqlite, init_schema

@st.cache_resource
def get_cfg(config_path: str) -> Tuple[Dict[str, Any], str]:
    cfg = load_yaml(config_path)
    return cfg, config_hash(cfg)

@st.cache_resource
def get_conn(sqlite_path: str, wal: bool = True, busy_timeout_ms: int = 5000):
    conn = connect_sqlite(sqlite_path, wal=wal, busy_timeout_ms=busy_timeout_ms)
    init_schema(conn)
    return conn
