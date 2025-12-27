from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

from src.news.gdelt import GDELTConfig, fetch_headlines
from src.news.tushare_ext import build_tushare_market_context
from src.news.external_prices import ExternalPricesConfig, fetch_watchlist, map_prices_to_industries


_INDUSTRY_KW = {
    # common CN -> (EN keywords)
    "有色金属": ["copper", "aluminium", "nickel", "metals"],
    "钢铁": ["steel"],
    "煤炭": ["coal"],
    "石油石化": ["oil", "refinery"],
    "电力": ["power", "electricity"],
    "半导体": ["semiconductor", "chip"],
    "计算机": ["software", "cyber"],
    "通信": ["telecom", "5G"],
    "医药生物": ["pharma", "drug", "biotech"],
    "银行": ["bank"],
    "证券": ["brokerage", "securities"],
    "房地产": ["property", "real estate"],
    "汽车": ["auto", "EV"],
    "新能源": ["renewable", "solar", "wind"],
}


@dataclass
class MarketCtxConfig:
    enabled: bool = True
    enable_tushare_ctx: bool = True
    enable_gdelt: bool = True
    gdelt_max_items: int = 10
    enable_external_prices: bool = True


_DEFAULT_WATCHLIST = [
    {"name": "S&P500", "symbol": "^spx"},
    {"name": "NASDAQ100", "symbol": "^ndq"},
    {"name": "道琼斯", "symbol": "^dji"},
    {"name": "COMEX铜(HG)", "symbol": "hg.f"},
    {"name": "WTI原油(CL)", "symbol": "cl.f"},
    {"name": "黄金(GC)", "symbol": "gc.f"},
]


def _build_gdelt_query(industries: List[str]) -> str:
    # include both Chinese industry names and rough English keywords
    # A few always-on triggers for "外盘映射" (can be refined later)
    kws: List[str] = ["China", "LME", "Goldman Sachs"]
    for ind in industries:
        if ind and ind not in kws:
            kws.append(ind)
        for en in _INDUSTRY_KW.get(ind, []):
            if en not in kws:
                kws.append(en)

    # GDELT query: join with OR
    # NOTE: keep it short to reduce API failures
    kws = kws[:10]
    return " OR ".join([f'"{k}"' if " " in k else k for k in kws])


def build_market_context(
    trade_date: str,
    picks_df: Optional[pd.DataFrame] = None,
    cfg: Optional[MarketCtxConfig] = None,
    raw_cfg: Optional[dict] = None,
) -> Dict[str, object]:
    cfg = cfg or MarketCtxConfig()
    if not cfg.enabled:
        return {"trade_date": trade_date}

    ctx: Dict[str, object] = {"trade_date": trade_date}

    industries: List[str] = []
    if picks_df is not None and not picks_df.empty and "industry" in picks_df.columns:
        industries = [str(x) for x in picks_df["industry"].dropna().astype(str).head(6).tolist() if str(x).strip()]
        # de-dup while keeping order
        seen = set()
        industries = [x for x in industries if not (x in seen or seen.add(x))]

    # 1) optional tushare market context
    if cfg.enable_tushare_ctx:
        try:
            ctx.update(build_tushare_market_context(trade_date=trade_date, topk=8))
        except Exception as e:
            ctx["tushare_ctx_error"] = str(e)

    # 1.5) external prices snapshot (Stooq)
    if cfg.enable_external_prices:
        try:
            watch = _DEFAULT_WATCHLIST
            # optional override from config.yaml:
            # news:
            #   external_prices:
            #     watchlist: {"铜": "hg.f", "标普500": "^spx"}
            if raw_cfg and isinstance(raw_cfg, dict):
                try:
                    wc = (((raw_cfg.get("news") or {}).get("external_prices") or {}).get("watchlist") or {})
                    if isinstance(wc, dict) and wc:
                        watch = {str(k): str(v) for k, v in wc.items()}
                except Exception:
                    pass

            rows, errs = fetch_watchlist(watch, cfg=ExternalPricesConfig())
            ctx["external_prices"] = rows
            if errs:
                ctx["external_prices_errors"] = errs
            # map to top industries in picks
            if industries and rows:
                ctx["external_mapping"] = map_prices_to_industries(industries, rows)[:8]
        except Exception as e:
            ctx["external_prices_error"] = str(e)

    # 2) gdelt headlines (free global)
    if cfg.enable_gdelt:
        try:
            q = _build_gdelt_query(industries)
            hcfg = GDELTConfig(max_items=int(cfg.gdelt_max_items))
            hs = fetch_headlines(q, cfg=hcfg)
            # UI expects `headlines`
            ctx["headlines"] = [h.__dict__ for h in hs]
            ctx["query"] = q
        except Exception as e:
            ctx["headlines_error"] = str(e)

    return ctx
