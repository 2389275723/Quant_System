from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import math
import pandas as pd
import numpy as np

def _round_lot(qty: float, lot: int = 100) -> int:
    if qty <= 0:
        return 0
    return int(math.floor(qty / lot) * lot)

@dataclass
class Order:
    client_order_id: str
    ts_code: str
    side: str   # BUY/SELL
    qty: int
    price_type: str  # LIMIT/MKT
    limit_price: float | None
    reason: str
    risk_tags: str = ""

class PortfolioManager:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.top_buy = int(((cfg.get("strategy", {}) or {}).get("buffer_zone", {}) or {}).get("top_buy", 5))
        self.top_sell = int(((cfg.get("strategy", {}) or {}).get("buffer_zone", {}) or {}).get("top_sell", 20))

        p = cfg.get("portfolio", {}) or {}
        self.cash_t1 = bool(((p.get("settlement", {}) or {}).get("cash_t_plus_1", True)))
        lim = p.get("limits", {}) or {}
        self.max_pos = float(lim.get("max_pos_per_stock", 0.2))
        self.max_turnover = float(lim.get("max_daily_turnover", 0.6))
        self.min_order_value = float(lim.get("min_order_value", 2000))

    def generate_orders(
        self,
        trade_date: str,
        picks_ranked: pd.DataFrame,
        targets: pd.DataFrame,
        positions: pd.DataFrame,
        cash_available: float,
        total_assets: float,
        price_df: pd.DataFrame,
        run_id: str
    ) -> List[Order]:
        """Create orders to move towards targets with Buffer Zone rules."""
        trade_date = str(trade_date)

        # current holdings
        pos = positions.copy() if positions is not None else pd.DataFrame()
        if not pos.empty:
            if "ts_code" not in pos.columns and "symbol" in pos.columns:
                pos["ts_code"] = pos["symbol"]
            for c in ["amount","market_value","cost_price","last_price"]:
                if c in pos.columns:
                    pos[c] = pd.to_numeric(pos[c], errors="coerce").fillna(0.0)

        held = set(pos["ts_code"].astype(str).tolist()) if not pos.empty else set()

        # buffer retain list: top_sell by final rank
        retain = set()
        if picks_ranked is not None and not picks_ranked.empty:
            pr = picks_ranked.sort_values("rank_final")
            retain = set(pr.head(self.top_sell)["ts_code"].astype(str).tolist())

        # desired list: top_buy for new buys (if not held)
        buy_candidates = []
        if picks_ranked is not None and not picks_ranked.empty:
            pr = picks_ranked.sort_values("rank_final")
            buy_candidates = pr.head(self.top_buy)["ts_code"].astype(str).tolist()

        # prices (from auction/quote)
        px = price_df.copy() if price_df is not None else pd.DataFrame()
        if not px.empty and "ts_code" not in px.columns and "symbol" in px.columns:
            px["ts_code"] = px["symbol"]
        if not px.empty:
            for c in ["ref_price","up_limit","down_limit","last_price"]:
                if c in px.columns:
                    px[c] = pd.to_numeric(px[c], errors="coerce")

        def get_price(code: str) -> float:
            if px.empty:
                return float("nan")
            r = px[px["ts_code"] == code]
            if r.empty:
                return float("nan")
            # prefer ref_price then last_price
            v = r.iloc[0].get("ref_price")
            if pd.isna(v):
                v = r.iloc[0].get("last_price")
            return float(v) if pd.notna(v) else float("nan")

        def buyable(code: str) -> bool:
            if px.empty:
                return True
            r = px[px["ts_code"] == code]
            if r.empty:
                return True
            p = r.iloc[0].get("ref_price")
            up = r.iloc[0].get("up_limit")
            if pd.isna(p) or pd.isna(up):
                return True
            return float(p) < float(up) - 1e-6

        def sellable(code: str) -> bool:
            if px.empty:
                return True
            r = px[px["ts_code"] == code]
            if r.empty:
                return True
            p = r.iloc[0].get("ref_price")
            dn = r.iloc[0].get("down_limit")
            if pd.isna(p) or pd.isna(dn):
                return True
            return float(p) > float(dn) + 1e-6

        # target weights
        t = targets.copy() if targets is not None else pd.DataFrame(columns=["ts_code","target_weight"])
        if not t.empty:
            t["target_weight"] = pd.to_numeric(t["target_weight"], errors="coerce").fillna(0.0)
        target_map = {str(r["ts_code"]): float(r["target_weight"]) for _, r in t.iterrows()} if not t.empty else {}

        orders: List[Order] = []

        # 1) Sell holdings not in retain list (buffer zone sell rule)
        for code in sorted(held):
            if code not in retain:
                if not sellable(code):
                    continue
                amt = int(pos[pos["ts_code"] == code]["amount"].iloc[0]) if not pos.empty else 0
                if amt > 0:
                    orders.append(Order(
                        client_order_id=f"{trade_date}_{run_id}_SELL_{code}",
                        ts_code=code,
                        side="SELL",
                        qty=amt,
                        price_type="MKT",
                        limit_price=None,
                        reason="BUFFER_SELL_NOT_RETAIN",
                    ))

        # 2) Rebalance target holdings (topn)
        # Compute available cash for buys (T+1 means do not add expected sell proceeds)
        cash_for_buys = float(cash_available)

        for code, w in target_map.items():
            if w <= 0:
                continue
            if code not in buy_candidates and code not in held:
                # buffer rule: only open new positions within top_buy
                continue
            p = get_price(code)
            if not math.isfinite(p) or p <= 0:
                continue

            # enforce buyable
            if not buyable(code):
                continue

            desired_value = w * float(total_assets)
            cur_value = float(pos[pos["ts_code"] == code]["market_value"].iloc[0]) if (not pos.empty and code in held) else 0.0
            diff_value = desired_value - cur_value

            # buy
            if diff_value > self.min_order_value:
                # cap by cash_for_buys
                buy_value = min(diff_value, cash_for_buys)
                if buy_value < self.min_order_value:
                    continue
                qty = _round_lot(buy_value / p, lot=100)
                if qty <= 0:
                    continue
                orders.append(Order(
                    client_order_id=f"{trade_date}_{run_id}_BUY_{code}",
                    ts_code=code,
                    side="BUY",
                    qty=qty,
                    price_type="LIMIT",
                    limit_price=p,
                    reason="TARGET_BUY",
                ))
                cash_for_buys -= qty * p

            # sell excess (within retain list)
            elif diff_value < -self.min_order_value and code in held:
                if not sellable(code):
                    continue
                amt = int(pos[pos["ts_code"] == code]["amount"].iloc[0])
                sell_value = min(-diff_value, amt * p)
                qty = _round_lot(sell_value / p, lot=100)
                qty = min(qty, amt)
                if qty <= 0:
                    continue
                orders.append(Order(
                    client_order_id=f"{trade_date}_{run_id}_SELLR_{code}",
                    ts_code=code,
                    side="SELL",
                    qty=qty,
                    price_type="LIMIT",
                    limit_price=p,
                    reason="TARGET_SELL_REBAL",
                ))

        return orders
