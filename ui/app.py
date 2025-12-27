from __future__ import annotations

import os
import sys
from pathlib import Path

import streamlit as st
from streamlit_autorefresh import st_autorefresh

# Ensure project root is on sys.path so `import src` and `import ui` work no matter how you start.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Load .env so UI and jobs can see API keys without restarting Python.
from src.core.env import load_env_from_root  # noqa: E402

try:
    load_env_from_root(ROOT, override=True)
except Exception:
    # Never break the app if env parsing fails.
    pass

from src.core.config import load_cfg, get  # noqa: E402
from src.core.paths import resolve_from_cfg  # noqa: E402
from src.storage.sqlite import connect  # noqa: E402
from src.storage.schema import ensure_schema  # noqa: E402
from ui import components  # noqa: E402
from ui.state import get_status  # noqa: E402
from ui.views import dashboard, model_lab, execution, settings  # noqa: E402


CFG_PATH = str((ROOT / "config" / "config.yaml").resolve())


def _repair_db() -> str:
    cfg = load_cfg(CFG_PATH)
    db_path = str(resolve_from_cfg(CFG_PATH, get(cfg, "paths.db_path")))
    conn = connect(db_path)
    try:
        ensure_schema(conn)
        return "已尝试修复数据库结构（补齐缺失字段）"
    finally:
        conn.close()


def _create_stop_file() -> None:
    cfg = load_cfg(CFG_PATH)
    stop_file = str(resolve_from_cfg(CFG_PATH, get(cfg, "paths.stop_file")))
    Path(stop_file).parent.mkdir(parents=True, exist_ok=True)
    Path(stop_file).write_text("STOP\n", encoding="utf-8")


def _remove_stop_file() -> None:
    cfg = load_cfg(CFG_PATH)
    stop_file = str(resolve_from_cfg(CFG_PATH, get(cfg, "paths.stop_file")))
    try:
        Path(stop_file).unlink(missing_ok=True)
    except Exception:
        pass


st.set_page_config(layout="wide", page_title="量化指挥中心 V1.5", page_icon="⚡")

cfg = load_cfg(CFG_PATH)
st_autorefresh(interval=int(get(cfg, "ui.refresh_ms", 10000)), key="main_refresh")

status = get_status(CFG_PATH)

# 1) 顶部通栏（God Mode）
components.render_status_banner(status)

# 2) 侧边栏：红绿灯 + 一键修复
with st.sidebar:
    st.title("⚡ Quant V1.5")
    st.caption("双头模型 · 严格风控 · 实盘闭环（文件协议）")

    st.markdown("---")
    components.render_traffic_light("数据源 (Tushare)", bool(status.get("tushare_ok")), "（脚手架：可接入真实健康检查）")
    components.render_traffic_light("交易端 (PTrade)", bool(status.get("ptrade_heartbeat_ok")), "心跳文件: inbox/ptrade_heartbeat.json")
    components.render_traffic_light("数据库 (SQLite)", bool(status.get("db_ok")), status.get("db_error", ""))

    any_red = (not status.get("tushare_ok")) or (not status.get("ptrade_heartbeat_ok")) or (not status.get("db_ok"))
    if any_red:
        st.markdown("---")
        if st.button("🛠️ 一键尝试修复", type="primary"):
            msg = _repair_db()
            st.toast(msg, icon="🛠️")
            st.rerun()

    st.markdown("---")
    # Kill switch controls
    if not status.get("kill_switch"):
        if st.button("🛑 紧急阻断 (KILL)", type="primary"):
            _create_stop_file()
            st.toast("已写入 STOP 文件，系统将拒绝出单/执行！", icon="🛑")
            st.rerun()
    else:
        if st.button("✅ 解除阻断 (UNLOCK)"):
            _remove_stop_file()
            st.toast("已删除 STOP 文件。", icon="✅")
            st.rerun()

# 3) Tabs
t1, t2, t3, t4 = st.tabs(["🌞 今日任务向导", "🧠 AI 辩论庭", "🚀 傻瓜式发单", "🧰 系统设置"])

with t1:
    dashboard.render(CFG_PATH)
with t2:
    model_lab.render(CFG_PATH)
with t3:
    execution.render(CFG_PATH)
with t4:
    settings.render(CFG_PATH)
