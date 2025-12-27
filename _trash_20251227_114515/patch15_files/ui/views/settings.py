from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import streamlit as st
import yaml

from src.core.config import load_cfg, get
from src.core.paths import resolve_from_cfg, project_root_from_cfg
from src.data.tushare_bars import health_check, update_daily_bars_csv


def _read_env(env_path: Path) -> Dict[str, str]:
    if not env_path.exists():
        return {}
    out: Dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _write_env(env_path: Path, updates: Dict[str, str]) -> None:
    env_path.parent.mkdir(parents=True, exist_ok=True)
    cur = _read_env(env_path)
    cur.update({k: (v or "").strip() for k, v in updates.items()})
    # keep stable ordering
    keys = sorted(cur.keys())
    lines = []
    for k in keys:
        v = cur[k]
        # don't write empty keys
        if v == "":
            continue
        lines.append(f"{k}={v}")
    env_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _save_yaml(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")


def render(cfg_path: str) -> None:
    st.header("🧰 系统设置")

    cfg = load_cfg(cfg_path)
    root = project_root_from_cfg(cfg_path)
    env_path = (root / ".env").resolve()
    env = _read_env(env_path)

    # ---- UI params
    st.subheader("小白设置")
    top_n = st.number_input("我想要买入只数 (TopN)", min_value=1, max_value=50, value=int(get(cfg, "strategy.top_n", 5) or 5), step=1)
    max_pos = st.number_input("单票仓位上限 (0-1)", min_value=0.01, max_value=1.0, value=float(get(cfg, "portfolio.max_position_per_stock", 0.2) or 0.2), step=0.01, format="%.2f")
    exclude_prefixes_cur = get(cfg, "universe.exclude_prefixes", [])
    exclude_markets_cur = get(cfg, "universe.exclude_markets", [])
    allow_growth_default = not (bool(exclude_prefixes_cur) or bool(exclude_markets_cur))
    allow_growth_boards = st.checkbox(
        "允许 300/301/688/689（不再剔除高波动板块）",
        value=allow_growth_default,
        help="取消勾选=剔除创业板/科创板；勾选=允许进入候选池",
    )
    exclude_bj = st.checkbox("排除北交所（.BJ）", value=bool(get(cfg, "universe.exclude_bj", True)))
    max_total_mv_yi = st.number_input("市值上限（亿，<=0 表示不限制）", min_value=0.0, value=float(get(cfg, "universe.max_total_mv_yi", 500.0)), step=10.0)

    st.markdown("---")
    st.subheader("API 账号管理（写入本地 .env）")

    col1, col2 = st.columns(2)
    with col1:
        tushare_token = st.text_input("Tushare Token", value=env.get("TUSHARE_TOKEN", ""), type="password")
        tushare_http = st.text_input("Tushare HTTP URL（第三方网关，可选）", value=env.get("TUSHARE_HTTP_URL", ""))
        deepseek_key = st.text_input("DeepSeek Key", value=env.get("DEEPSEEK_API_KEY", ""), type="password")
        deepseek_base = st.text_input("DeepSeek Base URL（可选）", value=env.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com"))
    with col2:
        dashscope_key = st.text_input("DashScope(Qwen) Key", value=env.get("DASHSCOPE_API_KEY", ""), type="password")
        dashscope_base = st.text_input("DashScope Base URL（可选）", value=env.get("DASHSCOPE_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"))
        enable_ai = st.checkbox("启用 AI 辩论（会调用 DeepSeek/Qwen，可随时生成费用）", value=bool(get(cfg, "model.enabled", False)))
        ai_topk = st.number_input("AI 评估数量上限 (TopK)", min_value=1, max_value=200, value=int(get(cfg, "model.max_items", 20) or 20), step=1)

    st.markdown("---")
    st.subheader("数据更新（bars.csv）")
    trade_date = st.text_input("更新 bars 的交易日（YYYYMMDD）", value=str(get(cfg, "ui.trade_date", "")) or "")

    # persistent output area
    if "tushare_test" not in st.session_state:
        st.session_state["tushare_test"] = None
    if "bars_update" not in st.session_state:
        st.session_state["bars_update"] = None

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("🔎 测试 Tushare 连通", width='stretch'):
            try:
                if not trade_date:
                    raise RuntimeError("请先填写交易日 YYYYMMDD（例如 20251224）")
                ok = health_check(trade_date)
                st.session_state["tushare_test"] = {"ok": bool(ok), "trade_date": trade_date}
            except Exception as e:
                st.session_state["tushare_test"] = {"ok": False, "trade_date": trade_date, "error": str(e)}
    with c2:
        if st.button("📥 更新/生成 daily_bars.csv", width='stretch'):
            try:
                if not trade_date:
                    raise RuntimeError("请先填写交易日 YYYYMMDD（例如 20251224）")
                out_csv = resolve_from_cfg(cfg_path, get(cfg, "paths.bars_path"))
                p = update_daily_bars_csv(trade_date=trade_date, out_csv_path=out_csv)
                # show basic stats
                import pandas as pd
                df = pd.read_csv(p)
                st.session_state["bars_update"] = {"ok": True, "path": str(p), "rows": int(df.shape[0]), "cols": int(df.shape[1])}
            except Exception as e:
                st.session_state["bars_update"] = {"ok": False, "error": str(e)}

    # show outputs
    if st.session_state.get("tushare_test") is not None:
        r = st.session_state["tushare_test"]
        if r.get("ok"):
            st.success(f"Tushare OK ✅  trade_date={r.get('trade_date')}")
        else:
            st.error(f"Tushare FAIL ❌  {r.get('error', 'unknown error')}")
    if st.session_state.get("bars_update") is not None:
        r = st.session_state["bars_update"]
        if r.get("ok"):
            st.success(f"bars 更新成功 ✅  rows={r.get('rows')} cols={r.get('cols')}  path={r.get('path')}")
        else:
            st.error(f"bars 更新失败 ❌  {r.get('error', 'unknown error')}")

    st.markdown("---")
    if st.button("💾 保存设置", width='stretch'):
        # 1) update .env
        _write_env(env_path, {
            "TUSHARE_TOKEN": tushare_token,
            "TUSHARE_HTTP_URL": tushare_http,
            "DEEPSEEK_API_KEY": deepseek_key,
            "DEEPSEEK_BASE_URL": deepseek_base,
            "DASHSCOPE_API_KEY": dashscope_key,
            "DASHSCOPE_BASE_URL": dashscope_base,
        })

        # 2) update config.yaml
        cfg2 = cfg.copy()
        cfg2.setdefault("strategy", {})
        cfg2["strategy"]["top_n"] = int(top_n)

        cfg2.setdefault("portfolio", {})
        cfg2["portfolio"]["max_position_per_stock"] = float(max_pos)

        cfg2.setdefault("model", {})
        cfg2["model"]["enabled"] = bool(enable_ai)
        cfg2["model"]["max_items"] = int(ai_topk)

        cfg2.setdefault("ui", {})
        if trade_date:
            cfg2["ui"]["trade_date"] = trade_date

        if allow_300:
            cfg2.setdefault("universe", {})
            cfg2["universe"]["exclude_prefixes"] = []
        else:
            cfg2.setdefault("universe", {})
            cfg2["universe"]["exclude_prefixes"] = ["300", "301", "688", "689"]
        cfg2["universe"]["exclude_bj"] = bool(exclude_bj)
        cfg2["universe"]["max_total_mv_yi"] = float(max_total_mv_yi) if float(max_total_mv_yi) > 0 else 0


        _save_yaml(Path(cfg_path), cfg2)

        st.success(f"已保存：config.yaml + {env_path.name}。请重新运行 Night Job 让新配置生效。")
        st.rerun()
