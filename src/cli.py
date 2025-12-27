from __future__ import annotations

import argparse
import glob
import logging
import os
from pathlib import Path
from typing import Any, Dict

from .core.config import load_yaml, config_hash, resolve_path, project_root
from .core.envutil import load_env_file
from .core.hashutil import sha256_files
from .core.logging import setup_logging
from .core.sqlite_store import connect_sqlite, init_schema
from .jobs.night_job import run_night_job
from .jobs.morning_job import run_morning_job
from .jobs.close_job import run_close_job

def compute_code_hash() -> str:
    root = project_root()
    paths = []
    for pat in ["src/**/*.py", "ui/**/*.py", "ptrade/**/*.py", "config/config.yaml"]:
        paths.extend(glob.glob(str(root / pat), recursive=True))
    # keep stable order
    paths = sorted(set(paths))
    return sha256_files(paths)

def main():
    # Load local secret env first (optional)
    load_env_file(resolve_path('config/secret.env'))

    parser = argparse.ArgumentParser(prog="Quant_System")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config.yaml")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("initdb", help="Create SQLite schema")
    sub.add_parser("night", help="Run Night Job")
    sub.add_parser("morning", help="Run Morning Job")
    sub.add_parser("close", help="Run Close Job (reconciliation)")

    args = parser.parse_args()
    cfg = load_yaml(args.config)
    cfg_hash = config_hash(cfg)

    setup_logging(resolve_path("logs"))
    logging.info("config_hash=%s", cfg_hash)

    db_path = resolve_path((cfg.get("storage", {}) or {}).get("sqlite_path", "data/quant.db"))
    wal = bool((cfg.get("storage", {}) or {}).get("wal", True))
    busy = int((cfg.get("storage", {}) or {}).get("busy_timeout_ms", 5000))
    conn = connect_sqlite(db_path, wal=wal, busy_timeout_ms=busy)
    init_schema(conn)

    code_hash = compute_code_hash()

    if args.cmd == "initdb":
        logging.info("DB initialized: %s", db_path)
        return

    if args.cmd == "night":
        run_night_job(conn, cfg, cfg_hash, code_hash)
        return

    if args.cmd == "morning":
        run_morning_job(conn, cfg, cfg_hash, code_hash)
        return

    if args.cmd == "close":
        run_close_job(conn, cfg, cfg_hash, code_hash)
        return

if __name__ == "__main__":
    main()
