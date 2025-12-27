from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple, List

import pandas as pd
import numpy as np

def _code_prefix(ts_code: str) -> str:
    # supports 000001.SZ / 600000.SH etc
    core = str(ts_code).split(".")[0]
    return core[:3]

def apply_hard_filters(df_snapshot: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """Hard filter：任何票不满足即 universe_flag=0，且不能进入 TopN。"""
    if df_snapshot is None or df_snapshot.empty:
        return pd.DataFrame()

    uni_cfg = (cfg.get("strategy", {}) or {}).get("universe_policy", {}) or {}
    exclude_prefixes = set(uni_cfg.get("exclude_prefixes", ["300","301","688","689"]))
    exclude_suffixes = set(uni_cfg.get("exclude_suffixes", [".BJ"]))
    exclude_st = bool(uni_cfg.get("exclude_st", True))

    d = df_snapshot.copy()
    flags = []

    def build_flags(row) -> str:
        f = []
        ts = str(row.get("ts_code", ""))
        name = str(row.get("name", "") or "")
        # suffix
        for suf in exclude_suffixes:
            if ts.endswith(suf):
                f.append(f"EXCL_SUFFIX:{suf}")
                break
        # prefix
        p3 = _code_prefix(ts)
        if p3 in exclude_prefixes:
            f.append(f"EXCL_PREFIX:{p3}")
        if exclude_st and ("ST" in name.upper() or "退" in name):
            f.append("EXCL_ST")
        try:
            cl = float(row.get("close", 0) or 0)
            if cl <= 0:
                f.append("BAD_PRICE")
        except Exception:
            f.append("BAD_PRICE")
        # factor completeness
        if pd.isna(row.get("f_ma20")) or pd.isna(row.get("f_ret20")):
            f.append("NO_HISTORY")
        return "|".join(f)

    d["filter_flags"] = d.apply(build_flags, axis=1)
    d["universe_flag"] = (d["filter_flags"] == "").astype(int)
    return d
