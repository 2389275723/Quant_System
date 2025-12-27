from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

from src.jobs.night_job import run_night_job
from src.jobs.morning_job import run_morning_job
from src.storage.sqlite import connect
from src.storage.schema import ensure_schema
from src.core.config import load_cfg, get
from src.core.paths import resolve_from_cfg
from src.data.tushare_bars import update_daily_bars_csv, health_check


def cmd_repair(cfg_path: str) -> int:
    cfg = load_cfg(cfg_path)
    db_path = str(resolve_from_cfg(cfg_path, get(cfg, "paths.db_path")))
    conn = connect(db_path)
    try:
        ensure_schema(conn)
        print("OK: schema ensured / repaired")
        return 0
    finally:
        conn.close()


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=["night", "morning", "repair", "update-bars"])
    p.add_argument("--cfg", default="config/config.yaml")
    p.add_argument("--trade-date", default=None)
    args = p.parse_args(argv)

    if args.cmd == "repair":
        return cmd_repair(args.cfg)

    if args.cmd == "night":
        res = run_night_job(cfg_path=args.cfg, trade_date=args.trade_date)
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return 0 if res.get("ok") else 2

    if args.cmd == "morning":
        res = run_morning_job(cfg_path=args.cfg, trade_date=args.trade_date)
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return 0 if res.get("ok") else 2


    if args.cmd == "update-bars":
        cfg = load_cfg(args.cfg)
        trade_date = args.trade_date or datetime.now().strftime("%Y%m%d")
        bars_path = resolve_from_cfg(args.cfg, get(cfg, "paths.bars_path"))
        # quick check (raises if token invalid)
        ok = health_check(trade_date)
        if not ok:
            print("WARN: trade_cal returned empty. Is this a trade date?")
        out_csv = update_daily_bars_csv(trade_date=trade_date, out_csv_path=bars_path)
        print(f"OK: bars updated -> {out_csv}")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
