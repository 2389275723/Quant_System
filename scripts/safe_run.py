from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
import subprocess
import csv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bridge.reconciliation import check_reconcile_status, RECONCILE_STATUS_PATH

MAIN = ROOT / "main.py"

def _load_trade_cal(trade_cal_csv: Path) -> dict[str, bool]:
    cal: dict[str, bool] = {}
    with trade_cal_csv.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cd = (row.get("cal_date") or row.get("trade_date") or row.get("date") or "").strip()
            io = (row.get("is_open") or row.get("open") or row.get("is_trade_day") or "").strip()
            if not cd:
                continue
            cd = cd.replace("-", "")
            if len(cd) != 8:
                continue
            key = f"{cd[0:4]}-{cd[4:6]}-{cd[6:8]}"
            cal[key] = io in ("1", "true", "True", "TRUE", "Y", "y")
    return cal

def ensure_trade_day(trade_date: str) -> None:
    # prefer local trade_cal cache if present
    candidates = [
        ROOT / "data" / "manual" / "trade_cal.csv",
        ROOT / "data" / "trade_cal.csv",
    ]
    for p in candidates:
        if p.exists():
            try:
                cal = _load_trade_cal(p)
                if trade_date in cal:
                    if not cal[trade_date]:
                        print("NOT_TRADE_DAY")
                        raise SystemExit(2)
                    return
            except Exception:
                break

    # fallback: weekend
    try:
        dt = datetime.strptime(trade_date, "%Y-%m-%d").date()
    except ValueError:
        print(f"BAD_TRADE_DATE: {trade_date}")
        raise SystemExit(2)

    if dt.weekday() >= 5:
        print("NOT_TRADE_DAY")
        raise SystemExit(2)

def ensure_reconcile_ok(trade_date: str) -> None:
    ok, reason, _ = check_reconcile_status(trade_date, status_path=RECONCILE_STATUS_PATH)
    if ok:
        return
    msg = f"RECONCILE_STATUS_BLOCK: {reason} (path={RECONCILE_STATUS_PATH})"
    print(msg)
    raise SystemExit(3)

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("job", choices=["night", "morning"])
    parser.add_argument("--trade-date", required=True, dest="trade_date")
    parser.add_argument("--cfg", default=None, dest="cfg")
    args, rest = parser.parse_known_args()

    trade_date = args.trade_date
    ensure_trade_day(trade_date)
    if args.job == "morning":
        ensure_reconcile_ok(trade_date)

    cmd = [sys.executable, str(MAIN)]
    if args.cfg:
        cmd += ["--cfg", args.cfg]
    cmd += [args.job, "--trade-date", trade_date]
    cmd += rest

    p = subprocess.run(cmd, cwd=str(ROOT))
    return int(p.returncode)

if __name__ == "__main__":
    raise SystemExit(main())
