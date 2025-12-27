from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from ..core.hashutil import sha256_bytes
from ..core.timeutil import fmt_ts, now_cn

REQUIRED_KEYS = ["alpha_score","risk_prob","risk_severity","risk_flags","confidence"]

def _safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def _safe_int(x, default=1):
    try:
        return int(x)
    except Exception:
        return default

def _neutral() -> Dict[str, Any]:
    return {
        "alpha_score": 0.0,
        "risk_prob": 0.0,
        "risk_severity": 1,
        "risk_flags": [],
        "confidence": 0.0,
    }

def _disagreement(a1: Dict[str, Any], a2: Dict[str, Any]) -> float:
    # Simple disagreement proxy in [0,1]
    da = abs(_safe_float(a1.get("alpha_score")) - _safe_float(a2.get("alpha_score"))) / 6.0  # alpha in [-3,3]
    dp = abs(_safe_float(a1.get("risk_prob")) - _safe_float(a2.get("risk_prob")))
    ds = abs(_safe_int(a1.get("risk_severity")) - _safe_int(a2.get("risk_severity"))) / 4.0
    return max(0.0, min(1.0, 0.5*da + 0.3*dp + 0.2*ds))

class DualHeadModelEngine:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.model_cfg = (cfg.get("model", {}) or {})
        self.enabled = bool(self.model_cfg.get("enabled", True))
        self.mode = self.model_cfg.get("mode", "shadow")
        self.batch_size = int(self.model_cfg.get("batch_size", 10))
        self.budget_sec = int(self.model_cfg.get("night_model_budget_sec", 1200))
        self.providers = (self.model_cfg.get("providers", {}) or {})

    def _provider_call(self, provider_key: str, items: List[Dict[str, Any]], market_ctx: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], str]:
        """Return list of per-item outputs (same order) and degraded_reason (empty if ok)."""
        pcfg = self.providers.get(provider_key, {}) or {}
        base_url = (pcfg.get("base_url") or "").strip()
        api_key = ""
        api_key_env = pcfg.get("api_key_env", "")
        if api_key_env:
            import os
            api_key = os.environ.get(api_key_env, "")

        if not base_url or not api_key:
            return [_neutral() for _ in items], "no_api_key_or_url"

        timeout = int(pcfg.get("timeout_sec", 25))
        model_name = pcfg.get("model", provider_key)

        # We send a JSON prompt to reduce hallucination; provider should echo valid JSON list.
        prompt_obj = {
            "task": "stock_dual_head_scoring",
            "constraints": {
                "no_fabrication": True,
                "output_json_only": True,
                "alpha_range": [-3, 3],
                "risk_prob_range": [0, 1],
                "risk_severity_range": [1, 5],
            },
            "market_context_snapshot": market_ctx,
            "items": items,
            "output_schema": {
                "alpha_score": "float",
                "risk_prob": "float",
                "risk_severity": "int",
                "risk_flags": "list[str]",
                "confidence": "float",
            },
        }
        prompt_text = json.dumps(prompt_obj, ensure_ascii=False)
        prompt_hash = sha256_bytes(prompt_text.encode("utf-8"))

        # Generic HTTP: POST {model, input}. You may need to adapt to your actual provider format.
        payload = {"model": model_name, "input": prompt_obj}
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

        try:
            r = requests.post(base_url, headers=headers, data=json.dumps(payload), timeout=timeout)
            r.raise_for_status()
            obj = r.json()
            # Accept either obj['output'] or obj['data']
            out = obj.get("output") or obj.get("data") or obj
            # out should be list aligned with items
            if isinstance(out, str):
                out = json.loads(out)
            if not isinstance(out, list) or len(out) != len(items):
                return [_neutral() for _ in items], "bad_response_shape"
            fixed = []
            for it in out:
                if not isinstance(it, dict):
                    fixed.append(_neutral())
                    continue
                # fill defaults
                for k in REQUIRED_KEYS:
                    if k not in it:
                        it[k] = _neutral()[k]
                # clamp
                it["alpha_score"] = max(-3.0, min(3.0, _safe_float(it.get("alpha_score"), 0.0)))
                it["risk_prob"] = max(0.0, min(1.0, _safe_float(it.get("risk_prob"), 0.0)))
                it["risk_severity"] = max(1, min(5, _safe_int(it.get("risk_severity"), 1)))
                it["confidence"] = max(0.0, min(1.0, _safe_float(it.get("confidence"), 0.0)))
                if not isinstance(it.get("risk_flags"), list):
                    it["risk_flags"] = []
                fixed.append(it)
            return fixed, ""
        except Exception:
            return [_neutral() for _ in items], "http_error"

    def score(self, df_candidates: pd.DataFrame, market_ctx: Dict[str, Any]) -> Dict[str, Any]:
        """Score TopM candidates. Returns dict with:
        - per_provider: {provider: df}
        - ensemble: df
        - prompt_hash: str
        - degraded: bool
        """
        if (not self.enabled) or df_candidates is None or df_candidates.empty:
            return {"per_provider": {}, "ensemble": pd.DataFrame(), "prompt_hash": "", "degraded": True}

        start = time.time()
        items = []
        for _, r in df_candidates.iterrows():
            items.append({
                "ts_code": r.get("ts_code"),
                "name": r.get("name"),
                "industry": r.get("industry"),
                "rule_score": float(r.get("score_rule")) if pd.notna(r.get("score_rule")) else None,
                "f_ret20": float(r.get("f_ret20")) if pd.notna(r.get("f_ret20")) else None,
                "f_rsi6": float(r.get("f_rsi6")) if pd.notna(r.get("f_rsi6")) else None,
                "f_near_high": float(r.get("f_near_high")) if pd.notna(r.get("f_near_high")) else None,
                "f_ma20_range": float(r.get("f_ma20_range")) if pd.notna(r.get("f_ma20_range")) else None,
            })

        # batching
        out_ds = []
        out_qw = []
        degraded_any = False
        prompt_hash = ""

        for i in range(0, len(items), self.batch_size):
            if time.time() - start > self.budget_sec:
                degraded_any = True
                break
            batch = items[i:i+self.batch_size]
            ds, d_reason = self._provider_call("deepseek", batch, market_ctx)
            qw, q_reason = self._provider_call("qwen", batch, market_ctx)

            if d_reason or q_reason:
                degraded_any = True

            out_ds.extend(ds)
            out_qw.extend(qw)

        # if truncated due to budget, pad neutrals
        if len(out_ds) < len(items):
            out_ds.extend([_neutral()] * (len(items) - len(out_ds)))
        if len(out_qw) < len(items):
            out_qw.extend([_neutral()] * (len(items) - len(out_qw)))

        # ensemble
        ens = []
        for a, b in zip(out_ds, out_qw):
            ens.append({
                "alpha_score": float(pd.Series([_safe_float(a["alpha_score"]), _safe_float(b["alpha_score"])]).median()),
                "risk_prob": max(_safe_float(a["risk_prob"]), _safe_float(b["risk_prob"])),
                "risk_severity": max(_safe_int(a["risk_severity"]), _safe_int(b["risk_severity"])),
                "risk_flags": list(set((a.get("risk_flags") or []) + (b.get("risk_flags") or []))),
                "confidence": float(pd.Series([_safe_float(a["confidence"]), _safe_float(b["confidence"])]).median()),
                "disagreement": _disagreement(a, b),
            })

        # prompt hash for audit (market_ctx dominates)
        prompt_hash = sha256_bytes(json.dumps({"market_ctx": market_ctx}, ensure_ascii=False, sort_keys=True).encode("utf-8"))
        df_ds = pd.DataFrame(out_ds)
        df_qw = pd.DataFrame(out_qw)
        df_ens = pd.DataFrame(ens)

        return {"per_provider": {"deepseek": df_ds, "qwen": df_qw}, "ensemble": df_ens, "prompt_hash": prompt_hash, "degraded": degraded_any}
