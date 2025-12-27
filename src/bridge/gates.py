from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class GateResult:
    ok: bool
    reason: str
    details: str | None = None


def kill_switch(stop_file: str) -> GateResult:
    if os.path.exists(stop_file):
        return GateResult(False, "KILL_SWITCH", f"STOP file exists: {stop_file}")
    return GateResult(True, "OK")


def fat_finger_check(orders: pd.DataFrame, max_lines: int, max_notional_per_order: float) -> GateResult:
    if orders is None:
        return GateResult(False, "NO_ORDERS")
    if len(orders) == 0:
        return GateResult(True, "OK", "no orders")
    if len(orders) > int(max_lines):
        return GateResult(False, "TOO_MANY_LINES", f"lines={len(orders)} > {max_lines}")
    if "notional" in orders.columns:
        mx = float(pd.to_numeric(orders["notional"], errors="coerce").fillna(0.0).max())
        if mx > float(max_notional_per_order):
            return GateResult(False, "ORDER_TOO_LARGE", f"max_notional={mx:.2f} > {max_notional_per_order}")
    return GateResult(True, "OK")
