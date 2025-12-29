from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from src.utils.trade_date import normalize_trade_date

RECONCILE_STATUS_PATH = Path("data") / "manual" / "reconcile_status.json"
ORDERS_CSV_PATH = Path("bridge") / "outbox" / "orders.csv"


@dataclass
class ReconResult:
    ok: bool
    reason: str
    details: str = ""


def isclose_money(a: float, b: float, rel_tol: float = 1e-6, abs_tol: float = 0.01) -> bool:
    return math.isclose(float(a), float(b), rel_tol=rel_tol, abs_tol=abs_tol)


def load_ptrade_positions_csv(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p, encoding="utf-8", dtype=str)


def _orders_gate(orders_path: Path = ORDERS_CSV_PATH) -> tuple[bool, str, int]:
    """Return ok flag, reason, and data row count for orders.csv presence check."""
    op = Path(orders_path)
    if not op.exists():
        return False, f"orders.csv missing at {op}", 0
    try:
        with op.open("r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f)
            next(reader, None)  # drop header
            rows = sum(1 for row in reader if any((cell or "").strip() for cell in row))
    except Exception as e:
        return False, f"orders.csv unreadable at {op}: {e}", 0
    if rows <= 0:
        return False, f"orders.csv has no data rows at {op}", rows
    return True, f"orders.csv rows={rows}", rows


def build_reconcile_status(trade_date: str, run_id: Optional[str] = None, orders_path: Path = ORDERS_CSV_PATH) -> dict[str, object]:
    td = normalize_trade_date(trade_date)
    ok, reason, _ = _orders_gate(orders_path)
    return {
        "trade_date": td or (trade_date or ""),
        "ok": bool(ok),
        "reason": reason,
        "run_id": run_id or "",
        "ts": datetime.now().isoformat(timespec="seconds"),
    }


def write_reconcile_status(
    trade_date: str,
    run_id: Optional[str] = None,
    status_path: Path = RECONCILE_STATUS_PATH,
    orders_path: Path = ORDERS_CSV_PATH,
) -> dict[str, object]:
    payload = build_reconcile_status(trade_date, run_id=run_id, orders_path=orders_path)
    sp = Path(status_path)
    sp.parent.mkdir(parents=True, exist_ok=True)
    sp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def check_reconcile_status(trade_date: str, status_path: Path = RECONCILE_STATUS_PATH) -> tuple[bool, str, dict]:
    sp = Path(status_path)
    if not sp.exists():
        return False, f"reconcile_status.json missing at {sp}", {}
    try:
        data = json.loads(sp.read_text(encoding="utf-8", errors="replace"))
    except Exception as e:
        return False, f"reconcile_status.json unreadable at {sp}: {e}", {}

    target_td = normalize_trade_date(trade_date)
    stored_td_raw = str(data.get("trade_date") or "").strip()
    stored_td = normalize_trade_date(stored_td_raw) if stored_td_raw else ""
    if not stored_td:
        return False, f"reconcile_status.json missing trade_date for {target_td} at {sp}", data
    if target_td and stored_td and stored_td != target_td:
        return False, f"trade_date mismatch: expected {target_td}, got {stored_td_raw or stored_td}", data

    ok = bool(data.get("ok"))
    if not ok:
        reason = data.get("reason") or "reconcile_status ok=false"
        return False, f"reconcile_status not OK: {reason}", data
    return True, data.get("reason") or "", data
