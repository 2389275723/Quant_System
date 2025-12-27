from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st

from src.core.config import load_cfg, get
from src.core.paths import resolve_from_cfg
from src.storage.sqlite import connect
from src.storage.schema import ensure_schema, safe_rank_column
from ui import components


def _load_latest(conn: sqlite3.Connection, cfg_hash: Optional[str]) -> pd.DataFrame:
    # latest trade_date with picks
    row = conn.execute("SELECT MAX(trade_date) AS d FROM picks_daily").fetchone()
    if not row or not row["d"]:
        return pd.DataFrame()
    trade_date = row["d"]

    rank_col = safe_rank_column(conn)

    # picks
    if cfg_hash:
        picks = pd.read_sql_query(
            f"""SELECT p.trade_date, p.ts_code, p.name, p.industry,
                      p.score_rule, p.final_score, {rank_col} AS rank_final
                 FROM picks_daily p
                 WHERE p.trade_date=? AND p.config_hash=?
                 ORDER BY {rank_col} ASC
            """,
            conn,
            params=(trade_date, cfg_hash),
        )
    else:
        picks = pd.read_sql_query(
            f"""SELECT p.trade_date, p.ts_code, p.name, p.industry,
                      p.score_rule, p.final_score, {rank_col} AS rank_final, p.config_hash
                 FROM picks_daily p
                 WHERE p.trade_date=?
                 ORDER BY {rank_col} ASC
            """,
            conn,
            params=(trade_date,),
        )
        if "config_hash" in picks.columns:
            cfg_hash = str(picks["config_hash"].iloc[0])

    if picks.empty:
        return pd.DataFrame()

    # model scores (may be empty if disabled)
    ms = pd.read_sql_query(
        """SELECT trade_date, ts_code,
                  alpha_ds, risk_prob_ds, risk_sev_ds, conf_ds, comment_ds,
                  alpha_qw, risk_prob_qw, risk_sev_qw, conf_qw, comment_qw,
                  alpha_final, risk_prob_final, risk_sev_final, disagreement, action
             FROM model_scores_daily
             WHERE trade_date=? AND config_hash=?
        """,
        conn,
        params=(trade_date, cfg_hash),
    )

    if ms.empty:
        # keep columns for UI
        for c in ["alpha_ds","risk_prob_ds","risk_sev_ds","conf_ds","comment_ds",
                  "alpha_qw","risk_prob_qw","risk_sev_qw","conf_qw","comment_qw",
                  "alpha_final","risk_prob_final","risk_sev_final","disagreement","action"]:
            picks[c] = None
        return picks

    out = picks.merge(ms, on=["trade_date","ts_code"], how="left")
    return out


def _score_to_0_100(alpha: Any) -> int:
    try:
        a = float(alpha)
    except Exception:
        return 50
    # alpha in [-3,3]
    return int(round((a + 3.0) / 6.0 * 100.0))


def render(cfg_path: str = "config/config.yaml") -> None:
    st.subheader("🧠 AI 辩论庭")
    st.caption("DeepSeek（进攻方） vs Qwen（风控方）——展示分歧与最终结论（Glass Box）")

    cfg = load_cfg(cfg_path)
    cfg_hash = None  # show latest regardless of hash

    conn = connect(str(resolve_from_cfg(cfg_path, get(cfg, "paths.db_path"))))
    try:
        ensure_schema(conn)
        df = _load_latest(conn, cfg_hash)
    finally:
        conn.close()

    if df.empty:
        st.info("暂无数据：请先运行 Night Job 生成 picks_daily。")
        return

    top_n = int(get(cfg, "scoring.top_n", 5))
    veto_sev = int(get(cfg, "risk_gate.veto.severity_gte", 3))
    veto_prob = float(get(cfg, "risk_gate.veto.prob_gt", 0.30))

    # split
    gold = df.head(1)
    rest = df.iloc[1:top_n].copy()
    others = df.iloc[top_n: top_n + 10].copy()

    def render_card(r: pd.Series, prefix: str = "") -> None:
        name = r.get("name") or ""
        code = r.get("ts_code") or ""
        ds_score = _score_to_0_100(r.get("alpha_ds"))
        qw_score = _score_to_0_100(r.get("alpha_qw"))
        ds_comment = r.get("comment_ds")
        qw_comment = r.get("comment_qw")
        if ds_comment is None or (isinstance(ds_comment, float) and pd.isna(ds_comment)) or str(ds_comment).strip() in ["", "nan", "None"]:
            ds_comment = "（未启用模型：当前走纯规则 / 或 API Key 未生效）"
        if qw_comment is None or (isinstance(qw_comment, float) and pd.isna(qw_comment)) or str(qw_comment).strip() in ["", "nan", "None"]:
            qw_comment = "（未启用模型：当前走纯规则 / 或 API Key 未生效）"

        disagreement = float(r.get("disagreement") or 0.0)
        risk_prob = float(r.get("risk_prob_final") or 0.0)
        risk_sev = int(r.get("risk_sev_final") or 1)

        veto = (risk_sev >= veto_sev and risk_prob > veto_prob) or (disagreement > 0.3)

        with st.container(border=True):
            c1, c2, c3 = st.columns([2, 3, 2])
            c1.markdown(f"### {prefix}{name}")
            c1.caption(str(code))

            if veto:
                c3.error("🚫 分歧/高危剔除")
            else:
                c3.success("✅ 买入（通过）")

            st.markdown("---")

            col_ds, col_qw = st.columns(2)
            with col_ds:
                st.markdown("**🤖 DeepSeek（主策略）**")
                st.progress(int(ds_score), text=f"评分: {ds_score}")
                st.info(f"🗣️ “{ds_comment}”")

            with col_qw:
                st.markdown("**🛡️ Qwen（风控）**")
                st.progress(int(qw_score), text=f"评分: {qw_score}")
                if abs(ds_score - qw_score) > 30:
                    st.warning(f"🗣️ “{qw_comment}”")
                else:
                    st.success(f"🗣️ “{qw_comment}”")

            st.markdown("---")
            components.render_model_confidence(
                conf_ds=float(r.get("conf_ds") or 0.0),
                conf_qw=float(r.get("conf_qw") or 0.0),
                disagreement=disagreement,
            )

            with st.expander("查看技术细节（因子/分数）", expanded=False):
                dd = pd.DataFrame([{
                    "score_rule": r.get("score_rule"),
                    "final_score": r.get("final_score"),
                    "rank_final": r.get("rank_final"),
                    "risk_prob_final": r.get("risk_prob_final"),
                    "risk_sev_final": r.get("risk_sev_final"),
                    "disagreement": r.get("disagreement"),
                }])
                st.dataframe(dd, width="stretch", hide_index=True)

    st.markdown("#### 🏆 今日金股")
    render_card(gold.iloc[0], prefix="🏆 ")

    st.markdown("#### 📌 今日 TopN（其余）")
    for _, r in rest.iterrows():
        render_card(r)

    with st.expander("更多候选（TopN+）", expanded=False):
        for _, r in others.iterrows():
            render_card(r)
