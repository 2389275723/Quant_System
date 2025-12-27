from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd



# requests is only used for raw HTTP fallback when OpenAI SDK fails
try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore
@dataclass
class ModelConfig:
    enabled: bool
    budget_sec: int
    batch_size: int
    max_items: int
    deepseek: Dict[str, Any]
    qwen: Dict[str, Any]


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def _to_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _first_json_obj(text: str) -> Optional[dict]:
    """Extract and parse the first {...} JSON object from a possibly noisy text."""
    text = text.strip()
    try:
        if text.startswith("{") and text.endswith("}"):
            return json.loads(text)
    except Exception:
        pass

    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None


def _http_post_json(url: str, api_key: str, payload: dict, timeout_sec: int = 60) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def _sdk_chat_call(
    base_url: str,
    api_key: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    timeout_sec: int = 60,
) -> Tuple[Optional[str], Optional[str]]:
    """Call via openai SDK (OpenAI-compatible). Return (content, error)."""
    try:
        from openai import OpenAI  # type: ignore
    except Exception:
        return None, "openai sdk not installed"
    try:
        client = OpenAI(api_key=api_key, base_url=base_url)
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            stream=False,
        )
        content = getattr(resp.choices[0].message, "content", None) if resp.choices else None
        return (content or ""), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"



def _ensure_v1(base_url: str) -> str:
    """Normalize base_url for OpenAI-compatible endpoints.

    - DeepSeek official host typically needs trailing /v1.
    - DashScope compatible-mode already contains /v1 in URL.
    """
    base = (base_url or "").rstrip("/")
    if not base:
        return base
    if "api.deepseek.com" in base and not re.search(r"/v\d+$", base):
        return base + "/v1"
    return base


def _openai_chat_call(base_url: str, api_key: str, model: str, system_prompt: str, user_prompt: str, timeout_sec: int = 30) -> Tuple[Optional[str], Optional[str]]:
    """Call OpenAI-compatible Chat Completions. Returns (content, error)."""
    base = _ensure_v1(base_url)

    # 1) Prefer OpenAI SDK (works for DashScope compatible-mode)
    content, err = _sdk_chat_call(base, api_key, model, system_prompt, user_prompt, timeout_sec=timeout_sec)
    if err is None and content is not None:
        return content, None

    # 2) Fallback raw HTTP
    url = f"{base}/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        if requests is None:
            return None, 'requests not installed (pip install requests)'
        r = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
        if r.status_code >= 400:
            return None, f"HTTP {r.status_code}: {r.text[:200]}"
        data = r.json()
        content = data.get("choices", [{}])[0].get("message", {}).get("content")
        if not content:
            return None, "empty content"
        return str(content), None
    except Exception as e:
        return None, str(e)

def _mk_feature_blob(row: pd.Series, trade_date: Optional[str], market_context: Optional[Dict[str, Any]] = None) -> dict:
    cols = [
        "ts_code",
        "name",
        "industry",
        "market",
        "score_final",
        "score_base",
        "pct_chg",
        "vol",
        "amount",
        "turnover_rate",
        "circ_mv",
        "cap_bucket",
        "vol_proxy",
        "strength_proxy",
    ]
    feat = {}
    for c in cols:
        if c in row.index:
            v = row[c]
            if isinstance(v, (np.generic,)):
                v = v.item()
            if isinstance(v, float):
                if np.isnan(v):
                    v = None
                else:
                    v = round(v, 6)
            feat[c] = v
    if trade_date:
        feat["trade_date"] = trade_date
    if market_context:
        feat["market_context"] = market_context
    return feat


class DualHeadModelEngine:
    """Dual-head LLM scorer.

    - DeepSeek: primary scorer (attack/alpha)
    - Qwen: auditor (risk)

    Both providers are called via **OpenAI-compatible** /chat/completions.

    To enable:
    - set model.enabled=true in config.yaml
    - set DEEPSEEK_API_KEY and DASHSCOPE_API_KEY in .env (or env vars)
    """

    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.enabled = bool(cfg.get("enabled", False))
        self.budget_sec = int(cfg.get("budget_sec", 1200))
        self.batch_size = int(cfg.get("batch_size", 10))
        self.max_items = int(cfg.get("max_items", 20))

        self.deepseek = cfg.get("deepseek", {}) or {}
        self.qwen = cfg.get("qwen", {}) or {}

    def _get_provider_cfg(self, which: str) -> Tuple[str, str, str]:
        if which == "deepseek":
            api_key_env = self.deepseek.get("api_key_env", "DEEPSEEK_API_KEY")
            api_key = os.getenv(api_key_env, "").strip()
            base_url = (self.deepseek.get("base_url") or os.getenv("DEEPSEEK_BASE_URL") or "https://api.deepseek.com").strip()
            model = (self.deepseek.get("model") or "deepseek-chat").strip()
            return api_key, base_url, model

        api_key_env = self.qwen.get("api_key_env", "DASHSCOPE_API_KEY")
        api_key = os.getenv(api_key_env, "").strip()
        base_url = (self.qwen.get("base_url") or os.getenv("DASHSCOPE_BASE_URL") or "https://dashscope.aliyuncs.com/compatible-mode/v1").strip()
        model = (self.qwen.get("model") or "qwen3-max").strip()
        return api_key, base_url, model

    def score(self, df: pd.DataFrame, trade_date: Optional[str] = None, market_context: Optional[Dict[str, Any]] = None, **kwargs) -> pd.DataFrame:
        out = df.copy()

        # default placeholders (so UI always has columns)
        out["alpha_ds"] = 0.0
        out["alpha_qw"] = 0.0
        out["risk_prob_ds"] = _clamp(float((out.get("vol_proxy", 0.0).astype(float).fillna(0.0).mean()) / 100.0), 0.0, 1.0)
        out["risk_prob_qw"] = out["risk_prob_ds"]
        out["risk_sev_ds"] = 2
        out["risk_sev_qw"] = 2
        out["conf_ds"] = 0.5
        out["conf_qw"] = 0.5
        out["comment_ds"] = "shadow disabled"
        out["comment_qw"] = "shadow disabled"

        if not self.enabled:
            out["alpha_final"] = out[["alpha_ds", "alpha_qw"]].median(axis=1).clip(-3, 3)
            out["risk_prob_final"] = out[["risk_prob_ds", "risk_prob_qw"]].max(axis=1).clip(0, 1)
            out["risk_sev_final"] = out[["risk_sev_ds", "risk_sev_qw"]].max(axis=1).astype(int)
            out["disagreement"] = (out["alpha_ds"] - out["alpha_qw"]).abs() / 6.0
            out["action"] = "OK"
            return out

        ds_key, ds_base, ds_model = self._get_provider_cfg("deepseek")
        qw_key, qw_base, qw_model = self._get_provider_cfg("qwen")

        if not ds_key or not qw_key:
            # enabled but keys missing
            out["comment_ds"] = "missing api key"
            out["comment_qw"] = "missing api key"
            out["action"] = "BLOCK"
            return out

        t0 = time.time()
        max_items = max(1, min(int(self.max_items), len(out)))
        work_idx = list(out.index[:max_items])

        sys_ds = (
            "你是A股短线进攻评委（偏进攻）。"
            "你只输出JSON对象："
            '{"alpha":-3..3,"risk_prob":0..1,"risk_sev":0..3,"conf":0..1,"comment":"<=40字"}'
            "。不要输出任何多余文本。"
        )
        sys_qw = (
            "你是A股风控审计员（偏保守）。"
            "你只输出JSON对象："
            '{"alpha":-3..3,"risk_prob":0..1,"risk_sev":0..3,"conf":0..1,"comment":"<=40字"}'
            "。不要输出任何多余文本。"
        )

        for idx in work_idx:
            if (time.time() - t0) > self.budget_sec:
                break

            row = out.loc[idx]
            feat = _mk_feature_blob(row, trade_date=trade_date, market_context=market_context)
            user = "基于以下特征打分：\n" + json.dumps(feat, ensure_ascii=False)

            # DeepSeek
            content, err = _openai_chat_call(ds_base, ds_key, ds_model, sys_ds, user, timeout_sec=60)
            if err:
                out.at[idx, "comment_ds"] = f"deepseek err: {err[:80]}"
            else:
                obj = _first_json_obj(content or "")
                if obj:
                    out.at[idx, "alpha_ds"] = _clamp(float(obj.get("alpha", 0.0)), -3, 3)
                    out.at[idx, "risk_prob_ds"] = _clamp(float(obj.get("risk_prob", out.at[idx, "risk_prob_ds"])), 0, 1)
                    out.at[idx, "risk_sev_ds"] = _to_int(obj.get("risk_sev", 2), 2)
                    out.at[idx, "conf_ds"] = _clamp(float(obj.get("conf", 0.5)), 0, 1)
                    out.at[idx, "comment_ds"] = str(obj.get("comment", "")).strip()[:120]
                else:
                    out.at[idx, "comment_ds"] = "deepseek: bad json"

            # Qwen
            content, err = _openai_chat_call(qw_base, qw_key, qw_model, sys_qw, user, timeout_sec=60)
            if err:
                out.at[idx, "comment_qw"] = f"qwen err: {err[:80]}"
            else:
                obj = _first_json_obj(content or "")
                if obj:
                    out.at[idx, "alpha_qw"] = _clamp(float(obj.get("alpha", 0.0)), -3, 3)
                    out.at[idx, "risk_prob_qw"] = _clamp(float(obj.get("risk_prob", out.at[idx, "risk_prob_qw"])), 0, 1)
                    out.at[idx, "risk_sev_qw"] = _to_int(obj.get("risk_sev", 2), 2)
                    out.at[idx, "conf_qw"] = _clamp(float(obj.get("conf", 0.5)), 0, 1)
                    out.at[idx, "comment_qw"] = str(obj.get("comment", "")).strip()[:120]
                else:
                    out.at[idx, "comment_qw"] = "qwen: bad json"

        out["alpha_final"] = out[["alpha_ds", "alpha_qw"]].median(axis=1).clip(-3, 3)
        out["risk_prob_final"] = out[["risk_prob_ds", "risk_prob_qw"]].max(axis=1).clip(0, 1)
        out["risk_sev_final"] = out[["risk_sev_ds", "risk_sev_qw"]].max(axis=1).astype(int)

        out["disagreement"] = (out["alpha_ds"] - out["alpha_qw"]).abs() / 6.0  # normalize ~[0,1]
        out["action"] = "OK"
        # Hard veto if too risky
        # (the downstream execution gate will use risk_gate in config)
        return out