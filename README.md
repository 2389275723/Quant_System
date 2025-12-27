# Quant_System V1.5（Windows UI + PTrade 文件桥）

本包目标：**本地 Python 负责决策/风控/监控/出单文件**，PTrade 负责**真实下单/真实成交/真实持仓**，两端通过 `bridge/` 目录文件握手完成闭环。

---

## 1) Windows 快速启动（UI）

1. 解压后进入目录  
2. 双击运行：`run_ui.bat`

默认会：
- 创建 `.venv`
- 安装依赖
- 启动 Streamlit UI（量化指挥中心）

---

## 2) 一键修复（解决你现在的报错）

你截图的报错属于 **SQLite 表结构缺列**（旧库没 `rank_final` / `rank_rule`）。

修复方式二选一：

### 方式 A：UI 里点按钮
侧边栏出现红灯时，点击 **「🛠️ 一键尝试修复」**  
它会做：`ALTER TABLE ADD COLUMN ...` 补齐缺失字段（不会删数据）。

### 方式 B：命令行
```bash
python main.py repair
```

---

## 3) 跑通最小闭环（本包自带演示行情数据）

本包内置了一个演示 `data/bars/daily_bars.csv`（含 300/688 示例，会被 Universe 过滤掉）。

### Night Job（收盘后）
```bash
python main.py night
```

### Morning Job（9:26）
```bash
python main.py morning
```

会生成：
- `data/quant.db`（SQLite）
- `bridge/outbox/orders.csv`

---

## 4) PTrade 端（Receiver / Dumb Executor）

在 PTrade 环境里运行 `ptrade/PTrade_Dumb_Executor.py`（按你的要求：极度愚蠢）

它会：
- 写 `bridge/inbox/ptrade_heartbeat.json`（UI 显示交易端是否在线）
- 读取 `bridge/outbox/orders.csv`
- 逐行下单（需要你把占位的下单函数替换成券商环境可用的 API）
- 读后 rename 为 `orders_processed_YYYYMMDD_runid.csv` 防重复

> 你需要把 Windows 上的 `Quant_System/bridge` 映射到 PTrade research 目录（同路径可见）

---

## 5) 目录说明

- `ui/`：Streamlit UI（今日任务向导 / AI 辩论庭 / 傻瓜式发单 / 系统设置）
- `src/jobs/`：Night/Morning 作业
- `src/storage/schema.py`：**SchemaMigrator（补齐缺列）**
- `bridge/`：文件桥（outbox/inbox + STOP Kill Switch）
- `ptrade/`：PTrade Receiver + 你原来的策略脚本副本

---

## 6) 你提的 V1.5 功能开关

`config/config.yaml`：

- `v1_5.enable_regime_engine`
- `v1_5.enable_vol_damper`
- `v1_5.enable_strength_gate`

默认都为 `true`（但这版属于 **脚手架实现**：不会依赖外部指数数据，后续你接入 IndexSnapshot 后可升级成真实 MA20 / 熔断逻辑）。

双头模型：
- `model.enabled` 默认 `false`（Shadow 模式）
- UI 会仍然展示“辩论庭”卡片，但文案会提示模型未启用

---

## Repo cleanup (Windows)

Preview cleanup (no changes):
```powershell
powershell -ExecutionPolicy Bypass -File scripts/cleanup_repo.ps1 -Mode preview -MovePatchArtifacts

---

## 免责声明
本包为工程模板，不构成任何投资建议。
