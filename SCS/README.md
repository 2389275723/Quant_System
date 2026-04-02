# SCS (Story Control System)

SCS 是一套面向长篇网文的项目治理系统，用于把“题材启动 → 项目锁定 → 单章闭环 → 连载治理 → 终局收束”连成一条可执行工作流。

## 当前版本视图

- **V1.0 Core**：严格闭环主流程、单章生产、issue_queue、recap、finale、gates
- **V1.1a Route Layer**：xianxia / high_concept_relationship / court_intrigue / western_fantasy / bootstrap_router
- **V1.1b Workbench Layer**：project/chapter/volume dashboards、run packet、block report、transition gate、mode/route conflict support
- **V1.1c Deep Governance Layer**：character_drift_monitor、world_expansion_auditor、payoff_fatigue_monitor、premature_reveal_guard
- **V1.2a Legacy & Migration Layer**：旧项目 intake、foundation 回填、history reconstruction、payoff/finale recovery、reentry gate
- **V1.2b Auto Governance Layer**：milestone_tracker、auto_trigger_engine、deadline_alert_center、cross_layer_risk_fuser、governance_priority_panel

## 推荐实战栈

### 新项目标准包
- V1.0 + V1.1a + V1.1b

### 复杂新项目包
- V1.0 + V1.1a + V1.1b + V1.1c

### 旧项目纳管包
- V1.0 + V1.2a + V1.1b

### 完整治理包
- V1.0 + V1.1a + V1.1b + V1.1c + V1.2a + V1.2b

## 目录说明

- `docs/architecture.md`：总架构、版本分层、总流程
- `docs/recommended-stacks.md`：四套推荐实战包
- `docs/legacy-migration.md`：V1.2a 旧项目纳管链说明
- `docs/auto-governance.md`：V1.2b 自动治理链说明
- `templates/`：核心 JSON 模板起始集合

## 默认建议

如果从零开一本正式长篇，默认先用：

**V1.0 + V1.1a + V1.1b**

如果项目复杂度明显上升，再逐步加入：

- V1.1c Deep Governance
- V1.2b Auto Governance
