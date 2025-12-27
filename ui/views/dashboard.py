from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict

import streamlit as st

from src.core.config import load_cfg, get
from src.core.paths import resolve_from_cfg
from src.storage.sqlite import connect
from src.storage.schema import ensure_schema
from src.jobs.night_job import run_night_job
from src.jobs.morning_job import run_morning_job
from ui import components
from ui.state import get_status


def _repair_db(cfg_path: str) -> str:
    cfg = load_cfg(cfg_path)
    db_path = str(resolve_from_cfg(cfg_path, get(cfg, "paths.db_path")))
    conn = connect(db_path)
    try:
        ensure_schema(conn)
        return "OK：已尝试修复数据库结构（补齐缺失字段）"
    finally:
        conn.close()


def _stage_card(title: str, done: bool = False, active: bool = False) -> None:
    with st.container(border=active):
        if done:
            st.markdown(f"### ✅ {title}")
            st.caption("已完成")
        elif active:
            st.markdown(f"### 🟡 {title}")
            st.caption("🔥 正在进行中…")
        else:
            st.markdown(f"### ⚪ {title}")
            st.caption("等待中")


def render(cfg_path: str = "config/config.yaml") -> None:
    st.subheader("🌞 今日任务向导")

    status = get_status(cfg_path)

    # 1) 关键错误：翻译成“人话”展示
    last_exec = status.get("last_exec") or {}
    last_failed = (last_exec.get("status") == "FAILED")
    if last_failed:
        tech = last_exec.get("error_msg") or ""
        title, human = components.translate_exception(Exception(tech))
        components.render_human_error("系统遇到了一点小问题", human, tech)
        c1, c2 = st.columns([1, 2])
        with c1:
            if st.button("🛠️ 一键尝试修复", type="primary"):
                msg = _repair_db(cfg_path)
                st.toast(msg, icon="🛠️")
                st.rerun()

    st.markdown("---")

    # 2) 傻瓜式进度条（根据 phase/日志推断）
    phase = status.get("phase", "IDLE")
    # naive mapping
    night_done = bool(status.get("last_factpack_json"))
    morning_done = bool(status.get("last_orders_path"))
    morning_active = (phase == "MORNING_JOB")
    night_active = (phase == "NIGHT_JOB")

    st.write("**当前进度：**")
    col1, col2, col3, col4 = st.columns(4)
    with col1: _stage_card("夜间选股", done=night_done and not night_active, active=night_active)
    with col2: _stage_card("晨间定价", done=morning_done and not morning_active, active=morning_active)
    with col3: _stage_card("实盘交易", done=False, active=False)
    with col4: _stage_card("收盘对账", done=False, active=False)

    # 3) 行动建议
    if phase == "NIGHT_JOB":
        st.info("💡 **当前建议：** 夜间作业运行中，请等待完成（或查看日志/卡片）。")
    elif phase == "MORNING_JOB":
        st.info("💡 **当前建议：** 晨间定价运行中，请勿重复点击出单。")
    else:
        if not night_done:
            st.info("💡 **当前建议：** 先运行【夜间选股】生成 picks_daily。")
        elif night_done and not morning_done:
            st.info("💡 **当前建议：** 夜间已完成，可运行【晨间定价】生成 orders.csv。")
        else:
            st.success("💡 **当前建议：** orders.csv 已生成。等待 PTrade 读取并回传 processed。")

    st.markdown("---")

    # 4) 一键运行按钮（可选）
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        if st.button("🌙 运行 Night Job", width='stretch'):
            res = run_night_job(cfg_path=cfg_path, trade_date=None)
            if res.get("ok"):
                st.success(f"Night Job OK: {res.get('trade_date')}  run_id={res.get('run_id')}")
            else:
                title, human = components.translate_exception(Exception(res.get("error", "")))
                components.render_human_error("Night Job 失败", human, res.get("error", ""))
            st.rerun()
    with c2:
        if st.button("☀️ 运行 Morning Job (09:26)", width='stretch'):
            res = run_morning_job(cfg_path=cfg_path, trade_date=None)
            if res.get("ok"):
                st.success(f"Morning Job OK: {res.get('trade_date')}  orders={res.get('orders_path')}")
            else:
                title, human = components.translate_exception(Exception(res.get("error", "")))
                components.render_human_error("Morning Job 失败", human, res.get("error", ""))
            st.rerun()
    with c3:
        if st.button("🔄 刷新状态", width='stretch'):
            st.rerun()
    # 5) 夜间选股结果（TopN）
    st.markdown("### 🌙 夜间选股结果（TopN）")
    cfg = load_cfg(cfg_path)
    db_path = str(resolve_from_cfg(cfg_path, get(cfg, "paths.db_path")))
    try:
        conn = connect(db_path)
        ensure_schema(conn)
        row = conn.execute(
            "SELECT trade_date, run_id, config_hash, created_at FROM picks_daily ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            st.info("暂无 picks_daily 记录。请先点击上方【🌙 运行 Night Job】。")
        else:
            trade_date = row["trade_date"]
            run_id = row["run_id"]
            st.caption(f"最新 picks_daily：trade_date={trade_date}  run_id={run_id}")

            # --- (B) Market & news context
            try:
                ctx_row = conn.execute(
                    "SELECT ctx_json FROM market_context_daily WHERE trade_date=? AND run_id=? ORDER BY created_at DESC LIMIT 1",
                    (trade_date, run_id),
                ).fetchone()
                if ctx_row and ctx_row.get("ctx_json"):
                    ctx = json.loads(ctx_row["ctx_json"])
                else:
                    ctx = None
            except Exception:
                ctx = None

            with st.expander("📡 市场&资讯（B）", expanded=True):
                if not ctx:
                    st.info("本次 run_id 尚未生成资讯快照（或 news.enabled 关闭）。")
                else:
                    # ---- External mapping (global markets)
                    st.markdown("**🌐 外盘映射（免费，Stooq）**")
                    ext_rows = ctx.get("external_prices") or []
                    if ext_rows:
                        import pandas as pd
                        dfE = pd.DataFrame(ext_rows)
                        show_cols = [c for c in ["name", "symbol", "asof", "last_close", "chg_pct"] if c in dfE.columns]
                        st.dataframe(dfE[show_cols], width="stretch", height=240, hide_index=True)
                    else:
                        if ctx.get("external_prices_error"):
                            st.caption(f"外盘数据失败：{ctx.get('external_prices_error')}")
                        else:
                            st.caption("暂无外盘数据")

                    ext_map = ctx.get("external_mapping") or []
                    if ext_map:
                        st.markdown("**🔁 映射到今日 Top 行业（粗规则）**")
                        for m in ext_map[:6]:
                            ind = m.get("industry")
                            score = m.get("score")
                            drivers = m.get("drivers") or []
                            if drivers:
                                brief = ", ".join([f"{d.get('name') or d.get('symbol')} {d.get('chg_pct')}%" for d in drivers if d.get('chg_pct') is not None])
                            else:
                                brief = "暂无驱动"
                            st.markdown(f"- **{ind}**  外盘得分 {score}  · {brief}")

                    cA, cB = st.columns(2)
                    with cA:
                        st.markdown("**🏭 行业资金流（同花顺口径，若网关支持）**")
                        rowsA = ctx.get("industry_moneyflow") or []
                        if rowsA:
                            import pandas as pd
                            dfA = pd.DataFrame(rowsA)[:10]
                            st.dataframe(dfA, width="stretch", height=240, hide_index=True)
                        else:
                            st.caption("暂无 / 网关不支持 / 积分不足")
                    with cB:
                        st.markdown("**🔥 概念涨停（若网关支持）**")
                        rowsB = ctx.get("concept_limitups") or []
                        if rowsB:
                            import pandas as pd
                            dfB = pd.DataFrame(rowsB)[:10]
                            st.dataframe(dfB, width="stretch", height=240, hide_index=True)
                        else:
                            st.caption("暂无 / 网关不支持 / 积分不足")

                    st.markdown("---")
                    st.markdown("**🌍 快讯标题（GDELT）**")
                    heads = ctx.get("headlines") or []
                    if not heads:
                        st.caption("暂无")
                    else:
                        for h in heads[:12]:
                            title = (h.get("title") or "").strip()
                            url = (h.get("url") or "").strip()
                            src = (h.get("source") or "").strip()
                            if title and url:
                                st.markdown(f"- [{title}]({url})  · {src}")
                            elif title:
                                st.markdown(f"- {title}  · {src}")

            topn = int(get(cfg, "strategy.top_n", 20) or 20)
            rows = conn.execute(
                "SELECT ts_code,name,industry,final_score,final_score_ai,rank_final,rank_ai,score_rule,trend_score,fund_score,flow_score "
                "FROM picks_daily WHERE trade_date=? AND run_id=? ORDER BY rank_final ASC LIMIT ?",
                (trade_date, run_id, max(topn, 50)),
            ).fetchall()
            import pandas as pd  # local import to keep UI fast
            df = pd.DataFrame([dict(r) for r in rows])
            if df.empty:
                st.warning("找到了 picks_daily，但该 run_id 下没有明细行。请检查 Night Job 日志。")
            else:
                show_cols=[c for c in ["rank_final","rank_ai","ts_code","name","industry","final_score","final_score_ai","score_rule","trend_score","fund_score","flow_score"] if c in df.columns]
                st.dataframe(df[show_cols], width='stretch', height=420)

                # (A-2) Single-stock detail card + related headlines
                st.markdown("#### 🧾 A-2：单只股票信息（含所属行业）")
                opts = [f"{r['ts_code']}  {r.get('name','')}" for _, r in df.iterrows()]
                sel = st.selectbox("选择一只股票查看详情", opts, index=0)
                sel_code = sel.split()[0]
                r0 = df[df["ts_code"] == sel_code].iloc[0].to_dict()
                with st.container(border=True):
                    st.markdown(f"### {r0.get('name','')}（{sel_code}）")
                    st.caption(f"行业：{r0.get('industry','')}   规则分：{r0.get('final_score')}   AI分：{r0.get('final_score_ai')}")
                    if ctx and (ctx.get("headlines") or []):
                        st.markdown("**相关快讯（按行业关键字粗筛）**")
                        kw = str(r0.get('industry') or '').strip()
                        shown = 0
                        for h in (ctx.get("headlines") or []):
                            title = (h.get("title") or "").strip()
                            url = (h.get("url") or "").strip()
                            if kw and (kw in title):
                                st.markdown(f"- [{title}]({url})")
                                shown += 1
                            if shown >= 3:
                                break
                        if shown == 0:
                            # fallback: show first 3
                            for h in (ctx.get("headlines") or [])[:3]:
                                title = (h.get("title") or "").strip()
                                url = (h.get("url") or "").strip()
                                if title and url:
                                    st.markdown(f"- [{title}]({url})")
                    else:
                        st.caption("暂无资讯快照")

                st.download_button(
                    "⬇️ 下载 picks_daily.csv",
                    df.to_csv(index=False).encode("utf-8-sig"),
                    file_name=f"picks_{trade_date}.csv",
                    mime="text/csv",
                    width='stretch',
                )
    except Exception as e:
        st.warning(f"读取 picks_daily 失败：{e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass