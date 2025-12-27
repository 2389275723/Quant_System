from __future__ import annotations

import os
import time
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

import pandas as pd
import requests

from ..core.config import resolve_path, get_env

class CircuitBreaker:
    def __init__(self, fail_threshold: int, open_cooldown_sec: int):
        self.fail_threshold = int(fail_threshold)
        self.open_cooldown_sec = int(open_cooldown_sec)
        self.fail_count = 0
        self.open_until_ts = 0.0

    def allow(self) -> bool:
        return time.time() >= self.open_until_ts

    def record_ok(self) -> None:
        self.fail_count = 0

    def record_fail(self) -> None:
        self.fail_count += 1
        if self.fail_count >= self.fail_threshold:
            self.open_until_ts = time.time() + self.open_cooldown_sec

class RateLimiter:
    def __init__(self, rps: float):
        self.rps = float(rps)
        self.min_interval = 1.0 / self.rps if self.rps > 0 else 0.0
        self.last_ts = 0.0

    def wait(self):
        if self.min_interval <= 0:
            return
        now = time.time()
        sleep = self.min_interval - (now - self.last_ts)
        if sleep > 0:
            time.sleep(sleep)
        self.last_ts = time.time()

class TushareProxySource:
    """最小可运行的 Tushare Proxy/Official 适配器。

    说明：
    - 你需要提供 DataApi 代理 URL（cfg.data_source.tushare.http_url）或使用官方 tushare SDK（此处未内置）。
    - 为保证工程“可跑”，当 URL/Token 缺失时会直接抛出可读错误，UI 会提示你切回 manual_csv。
    """
    def __init__(self, cfg: Dict[str, Any], official: bool = False):
        self.cfg = cfg
        self.official = official
        tc = (cfg.get("data_source", {}) or {}).get("tushare", {}) or {}
        self.http_url = tc.get("http_url", "").strip()
        self.token = get_env(tc.get("token_env", "TUSHARE_TOKEN"), "")
        self.timeout = int(tc.get("timeout_sec", 15))
        self.max_retries = int(tc.get("max_retries", 3))
        self.backoff = float(tc.get("backoff_sec", 1.5))
        cb = tc.get("circuit_breaker", {}) or {}
        self.cb = CircuitBreaker(cb.get("fail_threshold", 5), cb.get("open_cooldown_sec", 600))
        self.rl = RateLimiter(tc.get("global_rps", 4))

    def _post(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.cb.allow():
            raise RuntimeError("CircuitBreaker OPEN: remote disabled, please use cache_only/manual_csv.")
        if not self.http_url:
            raise RuntimeError("tushare.http_url is empty; please configure proxy URL or switch to manual_csv.")
        if not self.token:
            raise RuntimeError("Tushare token is missing; set env var TUSHARE_TOKEN or switch to manual_csv.")

        url = self.http_url.rstrip("/") + "/" + endpoint.lstrip("/")
        headers = {"Content-Type": "application/json"}
        payload = dict(payload)
        payload["token"] = self.token

        for i in range(self.max_retries):
            try:
                self.rl.wait()
                r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=self.timeout)
                if r.status_code != 200:
                    raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
                obj = r.json()
                self.cb.record_ok()
                return obj
            except Exception as e:
                self.cb.record_fail()
                if i == self.max_retries - 1:
                    raise
                time.sleep(self.backoff * (i + 1))
        raise RuntimeError("unreachable")

    def get_trade_cal(self) -> pd.DataFrame:
        # Expected proxy endpoint: trade_cal
        obj = self._post("trade_cal", {"params": {}})
        return pd.DataFrame(obj.get("data", []))

    def get_daily_bars(self, end_trade_date: str, lookback_days: int = 60) -> pd.DataFrame:
        obj = self._post("daily", {"params": {"end_date": str(end_trade_date), "lookback": int(lookback_days)}})
        return pd.DataFrame(obj.get("data", []))

    def get_daily_basic(self, trade_date: str) -> pd.DataFrame:
        obj = self._post("daily_basic", {"params": {"trade_date": str(trade_date)}})
        return pd.DataFrame(obj.get("data", []))

    def get_auction_quotes(self, trade_date: str) -> pd.DataFrame:
        obj = self._post("auction", {"params": {"trade_date": str(trade_date)}})
        return pd.DataFrame(obj.get("data", []))

    def get_ptrade_exports(self) -> Dict[str, str]:
        # Remote source does not provide PTrade exports. Use config manual paths.
        mc = (self.cfg.get("data_source", {}) or {}).get("manual_csv", {}) or {}
        return {
            "positions": resolve_path(mc.get("ptrade_positions_path", "")),
            "asset": resolve_path(mc.get("ptrade_asset_path", "")),
            "exec_report": resolve_path(mc.get("exec_report_path", "")),
        }
