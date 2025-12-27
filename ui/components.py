from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import streamlit as st


def render_status_banner(status: Dict[str, Any]) -> None:
    """Top banner (God Mode): shows phase and critical alarms."""
    phase = status.get("phase", "IDLE")
    kill = bool(status.get("kill_switch", False))
    ptrade_ok = bool(status.get("ptrade_heartbeat_ok", True))
    last_error = status.get("last_error")

    if kill:
        st.error("🛑 **物理熔断已激活 (KILL SWITCH)** —— 交易系统已完全锁死！")
    elif phase == "NIGHT_JOB":
        st.info("🌙 **夜间作业运行中**：数据清洗 → 因子计算 → 规则分/模型 → 写入 picks_daily …")
    elif phase == "MORNING_JOB":
        st.warning("☀️ **晨间定价运行中 (09:26)**：读取竞价信息 → 风控闸门 → 生成 orders.csv …")
    else:
        st.success("✅ 系统待命 (IDLE)")

    if not ptrade_ok:
        st.warning("⚠️ **PTrade 心跳缺失/过期**：请检查终端是否在线、以及 research 目录映射是否正确。")

    if last_error:
        with st.expander("最近一次错误（技术详情）"):
            st.code(str(last_error))


def render_traffic_light(label: str, ok: bool, note: str = "") -> None:
    icon = "🟢" if ok else "🔴"
    st.markdown(f"{icon} **{label}**")
    if note:
        st.caption(note)


def render_model_confidence(conf_ds: float, conf_qw: float, disagreement: float) -> None:
    col1, col2, col3 = st.columns(3)
    col1.metric("DeepSeek 置信度", f"{conf_ds*100:.0f}%")
    col2.metric("Qwen 置信度", f"{conf_qw*100:.0f}%")

    color = "green" if disagreement < 0.3 else ("orange" if disagreement < 0.6 else "red")
    col3.markdown(
        f"🤖 模型分歧度: <span style='color:{color};font-weight:bold'>{disagreement:.2f}</span>",
        unsafe_allow_html=True,
    )


def render_human_error(title: str, human_msg: str, tech_msg: str) -> None:
    with st.container(border=True):
        st.error(f"⚠️ {title}")
        st.write(f"**人话解释：** {human_msg}")
        with st.expander("查看技术详情 (给程序员看)"):
            st.code(tech_msg)


def translate_exception(e: Exception) -> Tuple[str, str]:
    msg = str(e)
    # Heuristics for common SQLite schema errors
    if "no such column" in msg and "rank_final" in msg:
        return ("数据库结构可能是旧版本", "检测到 picks_daily 缺少 rank_final 列。点击【一键尝试修复】自动补齐字段即可。")
    if "no such column" in msg:
        return ("数据库结构可能是旧版本", "检测到数据库字段缺失。点击【一键尝试修复】自动补齐字段即可。")
    if "KILL_SWITCH" in msg or "STOP" in msg:
        return ("交易已被物理熔断", "检测到 STOP 文件存在，系统已拒绝出单/执行。")
    return ("作业执行失败", "建议先点击【一键尝试修复】，若仍失败请打开技术详情排查日志。")
