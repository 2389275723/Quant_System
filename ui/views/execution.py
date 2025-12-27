from __future__ import annotations

import glob
import os
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import streamlit as st

from src.core.config import load_cfg, get
from src.core.paths import resolve_from_cfg
from ui.state import get_status
from ui import components


def _count_orders(path: str) -> int:
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
        return int(df.shape[0])
    except Exception:
        return 0


def render(cfg_path: str = "config/config.yaml") -> None:
    st.subheader("🚀 傻瓜式发单（Execution）")

    cfg = load_cfg(cfg_path)
    outbox = str(resolve_from_cfg(cfg_path, get(cfg, "paths.outbox_dir")))
    inbox = str(resolve_from_cfg(cfg_path, get(cfg, "paths.inbox_dir")))

    status = get_status(cfg_path)

    # Step statuses
    orders_path = str(Path(outbox) / "orders.csv")
    orders_ok = os.path.exists(orders_path)
    orders_n = _count_orders(orders_path) if orders_ok else 0

    processed = sorted(glob.glob(str(Path(outbox) / "orders_processed_*.csv")))
    processed_ok = len(processed) > 0

    col1, col2, col3 = st.columns(3)
    if orders_ok:
        col1.success(f"第 1 步：生成订单 ✅ 成功（{orders_n} 笔）")
    else:
        col1.info("第 1 步：生成订单 ⏳ 等待中（先跑 Morning Job）")

    if orders_ok:
        col2.success("第 2 步：传输给 PTrade ✅ 已写出 orders.csv")
    else:
        col2.info("第 2 步：传输给 PTrade ⏳ 等待中")

    if processed_ok:
        col3.success("第 3 步：PTrade 确认 ✅ 已生成 orders_processed_*")
    else:
        col3.info("第 3 步：PTrade 确认 ⏳ 等待中…")

    st.markdown("---")

    # Asset check (scaffold)
    st.subheader("🛡️ 资产核对（Asset Check）")
    st.info("V1.5 脚手架：你可以在这里接入 real_positions.csv / exec_report.csv 做强校验。")
    st.write("当前系统状态：")
    st.json({
        "kill_switch": status.get("kill_switch"),
        "ptrade_heartbeat_ok": status.get("ptrade_heartbeat_ok"),
        "last_orders_path": status.get("last_orders_path"),
    })

    st.markdown("---")

    st.subheader("📄 文件区（只给你看结果，不吓你）")
    if orders_ok:
        st.success(f"orders.csv 已生成：{orders_path}")
        if st.button("预览 orders.csv"):
            df = pd.read_csv(orders_path, encoding="utf-8-sig")
            st.dataframe(df, width="stretch", hide_index=True)
    else:
        st.warning("orders.csv 尚未生成。请先运行 Morning Job。")

    if processed_ok:
        st.success(f"发现 PTrade 已处理文件：{Path(processed[-1]).name}")
