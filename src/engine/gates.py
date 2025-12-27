from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from .portfolio import Order

def kill_switch_active(cfg: Dict[str, Any]) -> bool:
    path = (cfg.get("sanity", {}) or {}).get("kill_switch_file", "bridge/STOP")
    return os.path.exists(path)

def fat_finger_check(orders: List[Order], cfg: Dict[str, Any], total_assets: float) -> Tuple[bool, str]:
    """Return (ok, detail). If not ok -> block trading."""
    s = cfg.get("sanity", {}) or {}
    p = cfg.get("portfolio", {}) or {}
    lim = p.get("limits", {}) or {}

    max_orders = int(s.get("max_orders", 50))
    max_order_value = float(s.get("max_order_value", 500000))
    max_pos = float(lim.get("max_pos_per_stock", 0.2))
    max_turnover = float(lim.get("max_daily_turnover", 0.6))

    if len(orders) > max_orders:
        return False, f"Too many orders: {len(orders)} > {max_orders}"

    buy_value = 0.0
    for o in orders:
        price = o.limit_price if o.limit_price is not None else 0.0
        v = float(o.qty) * float(price)
        if o.side == "BUY":
            buy_value += v
            if v > max_order_value:
                return False, f"Single order value too large: {o.ts_code} value={v:.2f} > {max_order_value}"
            if total_assets > 0 and (v / total_assets) > max_pos + 1e-12:
                return False, f"Single position cap exceeded: {o.ts_code} weight={(v/total_assets):.2%} > {max_pos:.2%}"

    if total_assets > 0 and (buy_value / total_assets) > max_turnover + 1e-12:
        return False, f"Daily turnover cap exceeded: buy_value/total_assets={(buy_value/total_assets):.2%} > {max_turnover:.2%}"

    return True, "OK"

def isclose_money(a: float, b: float, rel_tol: float = 1e-6, abs_tol: float = 0.01) -> bool:
    return math.isclose(float(a), float(b), rel_tol=rel_tol, abs_tol=abs_tol)

def asset_check(expected_total_assets: float, real_total_assets: float, cfg: Dict[str, Any]) -> Tuple[bool, float, str]:
    ac = cfg.get("asset_check", {}) or {}
    enabled = bool(ac.get("enabled", True))
    if not enabled:
        return True, 0.0, "DISABLED"
    max_dev = float(ac.get("max_total_asset_dev", 0.05))
    abs_tol = float(ac.get("abs_tol", 0.01))
    rel_tol = float(ac.get("rel_tol", 1e-6))

    if expected_total_assets is None or not math.isfinite(float(expected_total_assets)):
        return True, 0.0, "NO_EXPECTED_ASSETS"

    dev_ratio = 0.0 if expected_total_assets == 0 else (float(real_total_assets) - float(expected_total_assets)) / float(expected_total_assets)
    ok = abs(dev_ratio) <= max_dev or isclose_money(expected_total_assets, real_total_assets, rel_tol=rel_tol, abs_tol=abs_tol)
    detail = f"expected={expected_total_assets:.2f}, real={real_total_assets:.2f}, dev_ratio={dev_ratio:.4%}"
    return ok, dev_ratio, detail
