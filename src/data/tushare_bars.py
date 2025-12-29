from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import requests

try:
    # repo 内建议放在：src/utils/trade_date.py
    from src.utils.trade_date import normalize_trade_date  # type: ignore
except Exception:  # pragma: no cover
    def normalize_trade_date(trade_date: Union[str, int], sep: str = "") -> str:
        """
        Normalize trade_date to 'YYYYMMDD' (default) or 'YYYY-MM-DD' (sep='-').
        Accepts: '2025-12-26', '20251226', 20251226.
        """
        s = str(trade_date).strip()
        digits = re.sub(r"\D", "", s)
        if len(digits) != 8:
            raise ValueError(f"Invalid trade_date: {trade_date!r}")
        if sep:
            return f"{digits[:4]}{sep}{digits[4:6]}{sep}{digits[6:]}"
        return digits


_DOTENV_LOADED = False


def _load_dotenv_once() -> None:
    """让 `python -c ...` 也能读取 .env（即使主程序没先 load）"""
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(override=False)
    except Exception:
        pass
    _DOTENV_LOADED = True


@dataclass
class TushareBarsConfig:
    token_env: str = "TUSHARE_TOKEN"
    http_url_env: str = "TUSHARE_HTTP_URL"  # e.g. http://host:5000/dataapi
    timeout_sec: int = 30


def _require_token(cfg: TushareBarsConfig) -> str:
    _load_dotenv_once()
    token = os.getenv(cfg.token_env, "").strip()
    if not token:
        raise RuntimeError(f"Missing {cfg.token_env}. Please set it in .env or environment variables.")
    return token


def _require_http_url(cfg: TushareBarsConfig) -> str:
    _load_dotenv_once()
    http_url = os.getenv(cfg.http_url_env, "").strip()
    if not http_url:
        raise RuntimeError(f"Missing {cfg.http_url_env}. Please set it in .env or environment variables.")
    return http_url.rstrip("/")


def _to_cols(fields: Any) -> List[str]:
    if fields is None:
        return []
    if isinstance(fields, str):
        return [c.strip() for c in fields.split(",") if c.strip()]
    if isinstance(fields, (list, tuple)):
        return [str(x) for x in fields]
    return []


def _parse_tushare_json(obj: Dict[str, Any], api_name: str, used_url: str) -> pd.DataFrame:
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
    # 有些网关把 fields/items 放在顶层
    if "fields" in obj and "items" in obj and not data:
        data = obj

    fields = _to_cols(data.get("fields"))
    items = data.get("items") or []

    if not fields:
        return pd.DataFrame()
    return pd.DataFrame(items, columns=fields)


def _http_post_json(url: str, payload: Dict[str, Any], timeout_sec: int) -> Tuple[int, Dict[str, Any]]:
    """POST json；返回 (status_code, json_obj)。不主动 raise_for_status。"""
    try:
        resp = requests.post(url, json=payload, timeout=timeout_sec)
    except Exception as e:
        raise RuntimeError(f"请求失败：url={url} err={e}") from e

    status = resp.status_code
    try:
        obj = resp.json()
    except Exception:
        obj = {"_raw_text": resp.text[:5000]}
    return status, obj


def _gateway_query(
    http_url: str,
    token: str,
    api_name: str,
    params: Dict[str, Any],
    fields: str = "",
    timeout_sec: int = 30,
    csv_path: Optional[Union[str, Path]] = None,
) -> pd.DataFrame:
    """
    双模式兼容第三方网关/官方 Tushare：

    Mode B（第三方常见）：POST {http_url} ，payload 里带 api_name
    Mode A（少数网关）：  POST {http_url}/{api_name}

    关键：你这个第三方是 Mode B，所以我们 **优先 base_url**，失败再试 /api_name。
    """
    http_url = http_url.rstrip("/")
    payload = {
        "api_name": api_name,
        "token": token,
        "params": params or {},
        "fields": fields or "",
    }

    candidates: List[str] = [http_url, f"{http_url}/{api_name}"]

    last_err: Optional[Exception] = None
    last_debug: str = ""

    for url in candidates:
        status, obj = _http_post_json(url, payload, timeout_sec=timeout_sec)

        # 404/405 或者返回了 HTML，说明 endpoint 不匹配 -> 试下一个
        if status in (404, 405) or ("_raw_text" in obj and status >= 400):
            last_debug = f"HTTP {status} at {url}"
            continue

        try:
            df = _parse_tushare_json(obj, api_name=api_name, used_url=url)
        except Exception as e:
            last_err = e
            last_debug = f"parse failed at {url}: {e}"
            continue

        if csv_path is not None:
            out = Path(csv_path)
            out.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(out, index=False, encoding="utf-8-sig")
        return df

    if last_err:
        raise last_err
    raise RuntimeError(f"Tushare网关请求失败：api={api_name} http_url={http_url}. {last_debug}".strip())


def update_daily_bars_csv(
    trade_date: Union[str, int],
    out_csv_path: Union[str, Path],
    *,
    fields: str = "ts_code,trade_date,open,high,low,close,vol,amount",
    cfg: Optional[TushareBarsConfig] = None,
) -> str:
    """拉取某交易日 daily bars -> 写出 CSV，返回路径"""
    cfg = cfg or TushareBarsConfig()
    http_url = _require_http_url(cfg)
    token = _require_token(cfg)

    td_norm = normalize_trade_date(trade_date, sep="")
    params = {"trade_date": td_norm}

    out_csv_path = Path(out_csv_path)
    _gateway_query(
        http_url=http_url,
        token=token,
        api_name="daily",
        params=params,
        fields=fields,
        timeout_sec=cfg.timeout_sec,
        csv_path=out_csv_path,
    )
    return str(out_csv_path)


def health_check(
    trade_date: Optional[Union[str, int]] = None,
    cfg: Optional[TushareBarsConfig] = None,
) -> Dict[str, Any]:
    """
    不抛异常，直接返回 dict，方便你 `python -c ...` 看结果
    """
    cfg = cfg or TushareBarsConfig()
    info: Dict[str, Any] = {
        "ok": False,
        "trade_date": None,
        "http_url": None,
        "token_env": cfg.token_env,
        "http_url_env": cfg.http_url_env,
        "err": "",
    }

    try:
        http_url = _require_http_url(cfg)
        token = _require_token(cfg)
        info["http_url"] = http_url

        params: Dict[str, Any] = {}
        if trade_date is not None:
            td_norm = normalize_trade_date(trade_date, sep="")
            info["trade_date"] = td_norm
            params = {"trade_date": td_norm, "limit": 1}

        _gateway_query(
            http_url=http_url,
            token=token,
            api_name="daily",
            params=params,
            fields="ts_code,trade_date,close",
            timeout_sec=cfg.timeout_sec,
            csv_path=None,
        )
        info["ok"] = True
        return info
    except Exception as e:
        info["err"] = str(e)
        return info


def gateway_query(
    api_name: str,
    params: Dict[str, Any],
    csv_path: Optional[Union[str, Path]],
    *,
    fields: str = "",
    cfg: Optional[TushareBarsConfig] = None,
) -> pd.DataFrame:
    """通用查询（其它模块可复用）"""
    cfg = cfg or TushareBarsConfig()
    http_url = _require_http_url(cfg)
    token = _require_token(cfg)
    return _gateway_query(
        http_url=http_url,
        token=token,
        api_name=api_name,
        params=params,
        fields=fields,
        timeout_sec=cfg.timeout_sec,
        csv_path=csv_path,
    )
