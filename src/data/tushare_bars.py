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


def _to_cols(fields: Any) -> list[str]:
    if fields is None:
        return []
    if isinstance(fields, str):
        s = fields.strip()
        if not s:
            return []
        return [c.strip() for c in s.split(",") if c.strip()]
    if isinstance(fields, (list, tuple)):
        return [str(x).strip() for x in fields if str(x).strip()]
    return []


def _parse_tushare_json(obj: Dict[str, Any], *, api_name: str, used_url: str) -> pd.DataFrame:
    """
    Parse Tushare-like json:
      {"code":0,"msg":"","data":{"fields":[...]/"a,b,c","items":[[...],...]}}
    """
    code = obj.get("code", 0)
    msg = obj.get("msg", "")

    if isinstance(code, str) and code.isdigit():
        code = int(code)

    if code not in (0, None):
        raise RuntimeError(f"Tushare网关返回错误：api={api_name} code={code} msg={msg} url={used_url}")

    data = obj.get("data") or {}
    fields = _to_cols(data.get("fields"))
    items = data.get("items") or []

    if not fields and items:
        # Sometimes gateways return list[dict]
        if isinstance(items[0], dict):
            return pd.DataFrame(items)

    if not fields:
        # no fields -> empty
        return pd.DataFrame()

    if not isinstance(items, list):
        return pd.DataFrame()

    # items: list[list]
    try:
        return pd.DataFrame(items, columns=fields)
    except Exception:
        # last resort: let pandas infer
        return pd.DataFrame(items)


def _http_post_json(url: str, payload: Dict[str, Any], timeout_sec: int) -> Tuple[int, str, str]:
    """
    Return (status_code, content_type, text).
    """
    import requests  # local import to keep deps minimal

    r = requests.post(url, json=payload, timeout=timeout_sec)
    return r.status_code, (r.headers.get("content-type") or ""), r.text


def _gateway_query(
    http_url: str,
    token: str,
    api_name: str,
    params: Dict[str, Any],
    fields: str = "",
    timeout_sec: int = 30,
) -> Tuple[pd.DataFrame, str]:
    """
    Support two common third-party gateway styles:

    A) Per-api endpoint (tushare>=some versions):
       POST {http_url.rstrip('/')}/{api_name}

    B) Single endpoint (many private gateways / old tutorials):
       POST {http_url.rstrip('/')} with {"api_name": "...", ...} in json

    We try A first; if 404, fallback to B.
    Return (df, used_url).
    """
    base = http_url.rstrip("/")
    payload = {"api_name": api_name, "token": token, "params": params, "fields": fields}

    # Try A: /{api_name}
    url_a = f"{base}/{api_name}"
    status, ctype, text = _http_post_json(url_a, payload, timeout_sec)

    if status == 404:
        # Try B: single endpoint
        url_b = base
        status, ctype, text = _http_post_json(url_b, payload, timeout_sec)
        used_url = url_b
    else:
        used_url = url_a

    if status != 200:
        snippet = (text or "")[:500].replace("\n", " ")
        raise RuntimeError(f"Tushare网关HTTP异常：status={status} url={used_url} body={snippet}")

    # Expect JSON
    try:
        obj = json.loads(text)
    except Exception:
        snippet = (text or "")[:500].replace("\n", " ")
        raise RuntimeError(f"Tushare网关返回非JSON：url={used_url} content_type={ctype} body={snippet}")

    df = _parse_tushare_json(obj, api_name=api_name, used_url=used_url)
    return df, used_url


def health_check(trade_date: str, cfg: Optional[TushareBarsConfig] = None) -> bool:
    """
    Online check for third-party Tushare gateways.

    We check `daily(trade_date=...)` because it's required for building daily_bars.csv.

    It raises RuntimeError with a readable message when the check fails.
    """
    cfg = cfg or TushareBarsConfig()
    token = _require_token(cfg)
    http_url = os.getenv(cfg.http_url_env, "").strip()
    if not http_url:
        raise RuntimeError(f"Missing {cfg.http_url_env}. Please set it in .env or in UI settings.")
    trade_date_norm = normalize_trade_date(trade_date, sep="")

    # Probe code is optional for daily (trade_date). Keep for future usage.
    _ = os.getenv(cfg.probe_code_env, "").strip() or "000001.SZ"

    # Some users may input non-trading-day; we try backward a few days.
    try:
        dt = datetime.datetime.strptime(normalize_trade_date(trade_date, sep=""), "%Y%m%d")
    except Exception:
        raise RuntimeError("trade_date 格式错误，应为 YYYYMMDD，例如 20251225")

    last_err: Optional[Exception] = None
    for back in [0, 1, 2, 3, 4, 5, 7, 10]:
        td = (dt - datetime.timedelta(days=back)).strftime("%Y%m%d")
        try:
            df, used = _gateway_query(
                http_url=http_url,
                token=token,
                api_name="daily",
                params={"trade_date": td},
                fields="ts_code,trade_date,close",
                timeout_sec=cfg.timeout_sec,
            )
            if df is not None and len(df) > 0:
                return True
        except Exception as e:
            last_err = e

    raise RuntimeError(
        "Tushare 连接失败：daily(trade_date=...) 仍然无法取到数据。\n"
        f"最后一次错误：{last_err}\n"
        "请重点检查：\n"
        "1) 第三方网关地址是否正确（常见：要不要带 /dataapi）；\n"
        "2) 网关是否支持 /{api_name} 这种路径；若不支持，本模块会自动退回单端点模式；\n"
        "3) Token 是否有效/是否被封；\n"
        "4) trade_date 是否为交易日 / 网关是否更新到该日期。"
    )


def update_daily_bars_csv(
    trade_date: str,
    out_csv_path: str | Path,
    cfg: Optional[TushareBarsConfig] = None,
) -> Path:
    """
    Build daily_bars.csv used by V1.5 from Tushare.

    - Works with third-party gateways (per-api endpoint or single endpoint).
    - Tolerates missing daily_basic (fills NaN).
    """
    cfg = cfg or TushareBarsConfig()
    token = _require_token(cfg)
    http_url = os.getenv(cfg.http_url_env, "").strip()
    if not http_url:
        raise RuntimeError(f"Missing {cfg.http_url_env}. Please set it in .env or in UI settings.")

    # 1) base universe info
    basic, _ = _gateway_query(
        http_url=http_url,
        token=token,
        api_name="stock_basic",
        params={"exchange": "", "list_status": "L"},
        fields="ts_code,name,industry,market",
        timeout_sec=cfg.timeout_sec,
    )
    if basic is None or len(basic) == 0:
        raise RuntimeError("Tushare 返回空数据：stock_basic 为空（请检查 Token / HTTP URL / 网关可用性）")
    basic["ts_code"] = basic["ts_code"].astype(str)

    # 2) daily bars
    daily, _ = _gateway_query(
        http_url=http_url,
        token=token,
        api_name="daily",
        params={"trade_date": trade_date_norm},
        fields="ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        timeout_sec=cfg.timeout_sec,
    )
    if daily is None or len(daily) == 0:
        raise RuntimeError(
            "Tushare 返回空数据：daily 为空。\n"
            "可能原因：trade_date 非交易日 / 网关数据缺失 / 权限不足 / 路由不兼容。"
        )
    daily["ts_code"] = daily["ts_code"].astype(str)
    if "trade_date" in daily.columns:
        daily["trade_date"] = daily["trade_date"].apply(normalize_trade_date)

    # 3) daily basic (optional)
    # NOTE: mv fields in Tushare daily_basic are often in "万元".
    # We will auto-normalize to RMB in step (5) below.
    daily_basic_cols = [
        "ts_code",
        "trade_date",
        "turnover_rate",
        "circ_mv",
        "total_mv",
        "volume_ratio",
        "pe_ttm",
        "pb",
    ]
    try:
        daily_basic, _ = _gateway_query(
            http_url=http_url,
            token=token,
            api_name="daily_basic",
            params={"trade_date": trade_date_norm},
            fields=",".join(daily_basic_cols),
            timeout_sec=cfg.timeout_sec,
        )
        if daily_basic is None or len(daily_basic) == 0:
            daily_basic = pd.DataFrame(columns=daily_basic_cols)
    except Exception:
        # Some gateways don't expose daily_basic; that's ok
        daily_basic = pd.DataFrame(columns=daily_basic_cols)

    if "ts_code" in daily_basic.columns:
        daily_basic["ts_code"] = daily_basic["ts_code"].astype(str)
    if "trade_date" in daily_basic.columns:
        daily_basic["trade_date"] = daily_basic["trade_date"].apply(normalize_trade_date)

    # 4) merge
    df = daily.merge(basic, on="ts_code", how="left")
    if len(daily_basic) > 0:
        df = df.merge(daily_basic, on=["ts_code", "trade_date"], how="left")
    else:
        for col in daily_basic_cols:
            if col not in df.columns:
                df[col] = pd.NA

    # 5) normalize numeric
    for col in [
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
        "turnover_rate",
        "circ_mv",
        "total_mv",
        "volume_ratio",
        "pe_ttm",
        "pb",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 6) Normalize market cap units.
    # Many Tushare endpoints return mv in "万元" (1e4 RMB).
    # We auto-detect by median magnitude to avoid double scaling on custom gateways.
    def _norm_mv(s: pd.Series) -> pd.Series:
        try:
            med = float(pd.to_numeric(s, errors="coerce").dropna().median())
        except Exception:
            med = 0.0
        # If median < 1e9, it's very likely in 万元; convert to RMB.
        if med and med < 1e9:
            return pd.to_numeric(s, errors="coerce") * 1e4
        return pd.to_numeric(s, errors="coerce")

    if "circ_mv" in df.columns:
        df["circ_mv"] = _norm_mv(df["circ_mv"])
    if "total_mv" in df.columns:
        df["total_mv"] = _norm_mv(df["total_mv"])

    out = Path(out_csv_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8-sig")  # Excel friendly
    return out


def gateway_query(
    api_name: str,
    params: Dict[str, Any],
    fields: str = "",
    cfg: Optional[TushareBarsConfig] = None,
) -> pd.DataFrame:
    """Generic query through third-party Tushare gateway.

    This is used by higher-level modules (news, extra factors...) and shares the same
    routing fallback logic as `update_daily_bars_csv`.
    """
    cfg = cfg or TushareBarsConfig()
    token = _require_token(cfg)
    http_url = os.getenv(cfg.http_url_env, "").strip()
    if not http_url:
        raise RuntimeError(f"Missing {cfg.http_url_env}. Please set it in .env or in UI settings.")

    df, _ = _gateway_query(
        http_url=http_url,
        token=token,
        api_name=api_name,
        params=params,
        fields=fields,
        timeout_sec=cfg.timeout_sec,
    )
    return df
