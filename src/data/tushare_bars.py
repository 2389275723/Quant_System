from __future__ import annotations

import datetime
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from src.utils.trade_date import normalize_trade_date


@dataclass
class TushareBarsConfig:
    # Env names (written by UI "系统设置")
    token_env: str = "TUSHARE_TOKEN"
    http_url_env: str = "TUSHARE_HTTP_URL"  # third-party gateway base url
    probe_code_env: str = "TUSHARE_PROBE_CODE"  # optional, e.g. 000001.SZ
    timeout_sec: int = 30


def _require_token(cfg: TushareBarsConfig) -> str:
    token = os.getenv(cfg.token_env, "").strip()
    if not token:
        raise RuntimeError(f"Missing {cfg.token_env}. Please set it in .env or environment variables.")
    return token


def _require_http_url(cfg: TushareBarsConfig) -> str:
    url = os.getenv(cfg.http_url_env, "").strip()
    if not url:
        raise RuntimeError(f"Missing {cfg.http_url_env}. Please set it in .env or environment variables.")
    return url.rstrip("/")


def _probe_code(cfg: TushareBarsConfig) -> str:
    code = os.getenv(cfg.probe_code_env, "").strip()
    return code or "000001.SZ"


def _assert_tushare_ok(cfg: TushareBarsConfig, trade_date: str) -> None:
    """
    Quick health-check:
    - token is present
    - http url is present
    - gateway responds and returns non-empty daily for a probe code on given trade_date

    trade_date supports both YYYYMMDD and YYYY-MM-DD.
    """
    token = _require_token(cfg)
    url = _require_http_url(cfg)

    # normalize to YYYYMMDD for API
    td_api = normalize_trade_date(trade_date, sep="")
    if len(td_api) != 8 or not td_api.isdigit():
        raise ValueError(f"trade_date 格式错误，应为 YYYYMMDD 或 YYYY-MM-DD, got={trade_date!r}")

    # do a tiny probe request
    code = _probe_code(cfg)
    df = gateway_query(
        "daily",
        params={"trade_date": td_api, "ts_code": code},
        csv_path=Path("bridge/outbox") / f"probe_daily_{code.replace('.', '_')}_{td_api}.csv",
    )
    if df.empty:
        raise RuntimeError(f"Tushare gateway probe returned empty for ts_code={code}, trade_date={td_api}. "
                           f"Please check token/url or the date is a valid trade day. token={token[:6]}*** url={url}")


def build_market_daily_bars_csv(trade_date: str, out_csv_path: str | Path, cfg: Optional[TushareBarsConfig] = None) -> Path:
    """
    Build market-wide daily bars snapshot for a trade day.

    This is a convenience wrapper that calls update_daily_bars_csv().
    """
    return update_daily_bars_csv(trade_date=trade_date, out_csv_path=out_csv_path, cfg=cfg)


def update_daily_bars_csv(
    trade_date: str,
    out_csv_path: str | Path,
    cfg: Optional[TushareBarsConfig] = None,
) -> Path:
    """
    拉取某个交易日的日线数据，并写入/更新 bars CSV。

    - trade_date 支持两种格式：YYYYMMDD / YYYY-MM-DD
    - 调用 tushare 接口时使用 YYYYMMDD（无分隔符）
    - 写出的 CSV 统一使用 YYYY-MM-DD（方便下游稳定处理）
    """
    if cfg is None:
        cfg = TushareBarsConfig()

    # Normalize trade_date: API 用 YYYYMMDD；输出/落盘用 YYYY-MM-DD
    trade_date_api = normalize_trade_date(trade_date, sep="")
    trade_date_norm = normalize_trade_date(trade_date, sep="-")

    out_csv_path = Path(out_csv_path)
    out_csv_path.parent.mkdir(parents=True, exist_ok=True)

    # download to outbox first
    outbox = Path("bridge/outbox")
    outbox.mkdir(parents=True, exist_ok=True)

    # 1) daily（日线 OHLCV + amount 等）
    tmp_daily = outbox / f"tushare_daily_{trade_date_api}.csv"
    params_daily = {"trade_date": trade_date_api}
    df_daily = gateway_query("daily", params=params_daily, csv_path=tmp_daily)

    # 2) daily_basic（流通市值/换手等）
    tmp_basic = outbox / f"tushare_daily_basic_{trade_date_api}.csv"
    params_basic = {"trade_date": trade_date_api}
    df_basic = gateway_query("daily_basic", params=params_basic, csv_path=tmp_basic)

    if df_daily.empty:
        raise RuntimeError(f"tushare daily returned empty for trade_date={trade_date_api}")

    # Normalize trade_date columns returned by API (通常是 int/str 的 YYYYMMDD)
    def _norm_td(v: Any) -> str:
        return normalize_trade_date(str(v), sep="-")

    if "trade_date" in df_daily.columns:
        df_daily["trade_date"] = df_daily["trade_date"].apply(_norm_td)
    if "trade_date" in df_basic.columns:
        df_basic["trade_date"] = df_basic["trade_date"].apply(_norm_td)

    # Keep only the target day (defensive)
    df_daily = df_daily[df_daily["trade_date"] == trade_date_norm].copy()
    df_basic = df_basic[df_basic["trade_date"] == trade_date_norm].copy()

    # columns keep
    cols_daily_keep = [
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
    ]
    cols_basic_keep = [
        "ts_code",
        "trade_date",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "pe",
        "pe_ttm",
        "pb",
        "ps",
        "ps_ttm",
        "dv_ratio",
        "dv_ttm",
        "total_share",
        "float_share",
        "free_share",
        "total_mv",
        "circ_mv",
    ]

    # Some gateways may not return all columns; keep intersection
    cols_daily_keep = [c for c in cols_daily_keep if c in df_daily.columns]
    cols_basic_keep = [c for c in cols_basic_keep if c in df_basic.columns]

    df_daily = df_daily[cols_daily_keep].copy()
    df_basic = df_basic[cols_basic_keep].copy()

    # Merge daily + basic
    if not df_basic.empty:
        df = pd.merge(df_daily, df_basic, on=["ts_code", "trade_date"], how="left")
    else:
        df = df_daily

    # Ensure trade_date is consistent
    df["trade_date"] = trade_date_norm

    # If out_csv already exists, update the specific day; else write new
    if out_csv_path.exists():
        old = pd.read_csv(out_csv_path, dtype={"ts_code": str})
        if "trade_date" not in old.columns:
            raise RuntimeError(f"existing bars file missing trade_date: {out_csv_path}")

        # normalize old trade_date
        old["trade_date"] = old["trade_date"].astype(str).apply(_norm_td)
        old = old[old["trade_date"] != trade_date_norm]

        new_all = pd.concat([old, df], ignore_index=True)
    else:
        new_all = df

    # Sort for stable diffs
    sort_cols = [c for c in ["trade_date", "ts_code"] if c in new_all.columns]
    if sort_cols:
        new_all = new_all.sort_values(sort_cols).reset_index(drop=True)

    new_all.to_csv(out_csv_path, index=False, encoding="utf-8")

    return out_csv_path


def gateway_query(api_name: str, params: dict, csv_path: Path) -> pd.DataFrame:
    """
    Query tushare (through a third-party gateway) and cache to csv_path.

    Environment variables:
    - TUSHARE_TOKEN: token
    - TUSHARE_HTTP_URL: base url like http://xxxx/dataapi
    """
    token = _require_token(TushareBarsConfig())
    base_url = _require_http_url(TushareBarsConfig())
    url = f"{base_url}/{api_name}"

    payload = {"api_name": api_name, "token": token, "params": params}

    # ensure parent dir
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    # if already exists and non-empty, load it
    if csv_path.exists() and csv_path.stat().st_size > 0:
        try:
            return pd.read_csv(csv_path, dtype={"ts_code": str})
        except Exception:
            pass

    import requests

    r = requests.post(url, json=payload, timeout=TushareBarsConfig().timeout_sec)
    r.raise_for_status()

    data = r.json()
    if isinstance(data, dict) and "data" in data:
        raw = data["data"]
    else:
        raw = data

    if raw is None:
        df = pd.DataFrame()
    elif isinstance(raw, dict) and "items" in raw and "fields" in raw:
        df = pd.DataFrame(raw["items"], columns=raw["fields"])
    elif isinstance(raw, list):
        df = pd.DataFrame(raw)
    else:
        # fall back: try parse as list-of-dict or dict-of-list
        df = pd.DataFrame(raw)

    # persist
    df.to_csv(csv_path, index=False, encoding="utf-8")
    return df
