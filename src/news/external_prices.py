from __future__ import annotations

"""External (non-A-share) market snapshot.

This module is used by ``src.news.market_ctx`` to build an "外盘映射" block.

Design principles:
1) Free/low-friction: default to Stooq public CSV endpoints.
2) Best-effort: any single symbol failure should not break the whole job.
3) Small payload: store only prices + simple deltas.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


@dataclass
class ExternalPricesConfig:
    enabled: bool = True
    # Stooq daily CSV endpoint (historical). Example:
    # https://stooq.com/q/d/l/?s=^spx&i=d&c=2
    stooq_hist_url: str = "https://stooq.com/q/d/l/"
    timeout_sec: int = 12
    # How many rows to request; 2 is enough for 1D change
    history_rows: int = 3


def _safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if pd.isna(v):
            return None
        return v
    except Exception:
        return None


def fetch_stooq_1d(symbol: str, cfg: Optional[ExternalPricesConfig] = None) -> Dict[str, Any]:
    """Fetch last close + 1D change% for a symbol from Stooq.

    Returns a dict:
      {"symbol":..., "last_close":..., "prev_close":..., "chg_pct":..., "asof":...}
    """

    cfg = cfg or ExternalPricesConfig()
    s = (symbol or "").strip()
    if not s:
        raise ValueError("empty symbol")

    import requests  # local import

    params = {
        "s": s.lower(),
        "i": "d",
        "c": int(cfg.history_rows),
    }
    r = requests.get(cfg.stooq_hist_url, params=params, timeout=cfg.timeout_sec)
    r.raise_for_status()

    # Stooq returns CSV like: Date,Open,High,Low,Close,Volume
    # Newest rows can be first or last depending on endpoint; we sort by Date.
    from io import StringIO

    df = pd.read_csv(StringIO(r.text))
    if df.empty or "Close" not in df.columns:
        return {"symbol": s, "last_close": None, "prev_close": None, "chg_pct": None, "asof": None}

    if "Date" in df.columns:
        df["Date"] = df["Date"].astype(str)
        df = df.sort_values("Date").reset_index(drop=True)

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else None

    last_close = _safe_float(last.get("Close"))
    prev_close = _safe_float(prev.get("Close")) if prev is not None else None
    asof = str(last.get("Date")) if "Date" in df.columns else None

    chg_pct: Optional[float] = None
    if last_close is not None and prev_close not in (None, 0):
        chg_pct = (last_close / float(prev_close) - 1.0) * 100.0
    elif last_close is not None:
        # fallback: use Open
        op = _safe_float(last.get("Open"))
        if op not in (None, 0):
            chg_pct = (last_close / float(op) - 1.0) * 100.0

    return {
        "symbol": s,
        "last_close": None if last_close is None else round(float(last_close), 6),
        "prev_close": None if prev_close is None else round(float(prev_close), 6),
        "chg_pct": None if chg_pct is None else round(float(chg_pct), 4),
        "asof": asof,
    }


def fetch_watchlist(
    watchlist: List[Dict[str, str]],
    cfg: Optional[ExternalPricesConfig] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    """Fetch a list of external instruments.

    watchlist item format:
      {"name": "标普500", "symbol": "^spx"}

    Returns: (rows, errors)
    """
    cfg = cfg or ExternalPricesConfig()
    if not cfg.enabled:
        return [], []

    rows: List[Dict[str, Any]] = []
    errs: List[Dict[str, str]] = []
    for it in watchlist or []:
        name = (it.get("name") or "").strip() or (it.get("symbol") or "").strip()
        sym = (it.get("symbol") or "").strip()
        if not sym:
            continue
        try:
            q = fetch_stooq_1d(sym, cfg=cfg)
            q["name"] = name
            rows.append(q)
        except Exception as e:
            errs.append({"name": name, "symbol": sym, "error": f"{type(e).__name__}: {e}"})

    return rows, errs


def map_prices_to_industries(
    industries: List[str],
    price_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Heuristic mapping: which external moves may matter to the top industries.

    The mapping is deliberately shallow (rule-based) and meant as a *context*.
    Real trading decisions should still rely on your internal scoring + risk controls.
    """
    # default rule-set (you can refine later)
    ind2syms = {
        "有色金属": ["hg.f", "^spx"],
        "煤炭": ["cl.f"],
        "石油石化": ["cl.f"],
        "电力": ["cl.f"],
        "银行": ["^spx"],
        "证券": ["^spx"],
        "房地产": ["^spx"],
        "半导体": ["^ndq", "^spx"],
        "计算机": ["^ndq"],
        "通信": ["^ndq"],
        "医药生物": ["^spx"],
        "汽车": ["^ndq", "^spx"],
        "新能源": ["cl.f", "^ndq"],
    }

    by_symbol = {str(r.get("symbol") or "").lower(): r for r in (price_rows or [])}

    out: List[Dict[str, Any]] = []
    for ind in industries or []:
        syms = ind2syms.get(ind, [])
        drivers = []
        score = 0.0
        for s in syms:
            rr = by_symbol.get(s.lower())
            if not rr:
                continue
            chg = rr.get("chg_pct")
            if chg is None:
                continue
            drivers.append({"symbol": rr.get("symbol"), "name": rr.get("name"), "chg_pct": chg})
            # very light scoring
            score += float(chg) / 3.0
        out.append({"industry": ind, "drivers": drivers, "score": round(score, 4)})

    # sort by absolute signal strength
    out = sorted(out, key=lambda x: abs(float(x.get("score") or 0.0)), reverse=True)
    return out
