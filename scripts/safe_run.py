from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
import subprocess
import csv
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.config import load_cfg
from src.core.trading_calendar import TradingCalendar
from src.utils.trade_date import normalize_trade_date
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

def _exit_payload(payload: dict[str, object], code: int = 2) -> None:
    print(json.dumps(payload, ensure_ascii=False))
    raise SystemExit(code)


def ensure_trade_day(trade_date: str, cfg_path: Optional[str]) -> None:
    td_norm = normalize_trade_date(trade_date)
    cfg_file = cfg_path or "config/config.yaml"

    try:
        cfg = load_cfg(cfg_file)
        cal = TradingCalendar(cfg, cfg_path=cfg_file)
        if not cal.is_trade_day(td_norm):
            _exit_payload({"ok": False, "reason": "NOT_TRADE_DAY", "trade_date": td_norm})
        return
    except Exception:
        # fall through to CSV/weekend guard if config/calendar unavailable
        pass

    candidates = [
        ROOT / "data" / "manual" / "trade_cal.csv",
        ROOT / "data" / "trade_cal.csv",
    ]
    for p in candidates:
        if p.exists():
            try:
                cal = _load_trade_cal(p)
                if td_norm in cal:
                    if not cal[td_norm]:
                        _exit_payload(
                            {"ok": False, "reason": "NOT_TRADE_DAY", "trade_date": td_norm, "source": str(p)}
                        )
                    return
            except Exception:
                break

    # fallback: weekend
    try:
        dt = datetime.strptime(td_norm or trade_date, "%Y-%m-%d").date()
    except ValueError:
        _exit_payload({"ok": False, "reason": "BAD_TRADE_DATE", "trade_date": trade_date})

    if dt.weekday() >= 5:
        _exit_payload({"ok": False, "reason": "NOT_TRADE_DAY", "trade_date": td_norm or trade_date})

def ensure_reconcile_ok(trade_date: str) -> None:
    ok, reason, _ = check_reconcile_status(trade_date, status_path=RECONCILE_STATUS_PATH)
    if ok:
        return
    msg = f"RECONCILE_STATUS_BLOCK: {reason} (path={RECONCILE_STATUS_PATH})"
    _exit_payload(
        {
            "ok": False,
            "reason": "RECONCILE_STATUS_BLOCK",
            "trade_date": trade_date,
            "details": reason,
            "status_path": str(RECONCILE_STATUS_PATH),
            "message": msg,
        },
        code=3,
    )

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("job", choices=["night", "morning"])
    parser.add_argument("--trade-date", required=True, dest="trade_date")
    parser.add_argument("--cfg", default="config/config.yaml", dest="cfg")
    args, rest = parser.parse_known_args()

    cfg_file = args.cfg or "config/config.yaml"
    trade_date = normalize_trade_date(args.trade_date) or args.trade_date

    ensure_trade_day(trade_date, cfg_file)
    if args.job == "morning":
        ensure_reconcile_ok(trade_date)

    cmd = [sys.executable, str(MAIN)]
    if cfg_file:
        cmd += ["--cfg", cfg_file]
    cmd += [args.job, "--trade-date", trade_date]
    cmd += rest

    p = subprocess.run(cmd, cwd=str(ROOT))
    return int(p.returncode)

if __name__ == "__main__":
    raise SystemExit(main())
