# -*- coding: utf-8 -*-
"""
Quant_System 版本/里程碑审计脚本
- 扫描仓库文件，按开发计划 M0~M8 里程碑做“存在性/迹象”检查
- 输出: reports/version_audit_YYYYMMDD_HHMMSS.md + .json
用法:
  python scripts/version_audit.py --root . --plan 开发计划.txt --out reports
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any


# ============== 可调：扫描哪些文件类型 ==============
SCAN_EXTS = {
    ".py", ".md", ".txt", ".yml", ".yaml", ".toml", ".json", ".ini",
    ".sql", ".bat", ".ps1", ".sh", ".cfg"
}

# ============== 可调：跳过哪些目录（避免扫描大文件夹） ==============
DEFAULT_IGNORE_DIRS = {
    ".git", ".idea", ".vscode",
    "__pycache__", ".pytest_cache",
    ".venv", "venv", "env",
    "node_modules",
    "dist", "build", "out",
    "data", "datasets", "dataset",   # 你如果把 schema 放在 data/，可把这行删掉
    "reports", "logs", "log",
}

# ============== 里程碑规则（按关键词/正则猜测你是否实现） ==============
def milestone_rules() -> Dict[str, Dict[str, Any]]:
    """
    每个 milestone:
      - title: 描述
      - checks: List[ (check_name, [regex...]) ]
      - pass_ratio: 命中比例 >= pass_ratio => milestone 判定为 DONE
    """
    return {
        "M0": {
            "title": "snapshot→factors→preprocess→rule_score→picks 基础闭环",
            "pass_ratio": 0.6,
            "checks": [
                ("snapshot 存在", [r"\bsnapshot\b", r"snapshot_raw", r"snapshot_table"]),
                ("factors/因子", [r"\bfactor\b", r"FactorRegistry", r"factors?_", r"因子"]),
                ("preprocess/清洗", [r"preprocess", r"clean", r"sanitize", r"winsor", r"standard", r"归一", r"清洗"]),
                ("rule_score/打分", [r"rule[_-]?score", r"score_rules?", r"打分", r"rank"]),
                ("picks/TopN", [r"\bpicks?\b", r"TopN", r"select_top", r"选股"]),
            ],
        },
        "M1": {
            "title": "labels (D+1 open → D+h close) + 一字板 buyable=0",
            "pass_ratio": 0.6,
            "checks": [
                ("labels/标签", [r"\blabels?\b", r"make_labels", r"label_table", r"标签"]),
                ("D\\+1 open / future", [r"D\+1", r"next[_-]?day", r"\bopen\b.*next", r"future", r"未来"]),
                ("close D\\+h", [r"close.*D\+\d+", r"horizon", r"holding_days", r"持有.*天"]),
                ("涨跌停/一字板", [r"up_limit|low_limit|limit_up|limit_down", r"一字板", r"buyable", r"可买"]),
            ],
        },
        "M2": {
            "title": "portfolio（Buffer/成本/锁仓/T+1结算）",
            "pass_ratio": 0.6,
            "checks": [
                ("portfolio/持仓簿", [r"\bportfolio\b", r"Position", r"holdings", r"持仓"]),
                ("成本/手续费/滑点", [r"commission|fee|cost", r"slippage", r"手续费", r"滑点", r"交易成本"]),
                ("Buffer", [r"\bbuffer\b", r"rebalance_buffer", r"缓冲"]),
                ("锁仓/T\\+1", [r"T\+1", r"settle", r"settlement", r"lock", r"锁仓", r"结算"]),
            ],
        },
        "M3": {
            "title": "monitor + factpack + execution_log + SQLite/WAL + 浮点isclose",
            "pass_ratio": 0.6,
            "checks": [
                ("monitor/监控", [r"\bmonitor\b", r"metrics", r"telemetry", r"监控"]),
                ("FactPack", [r"FactPack", r"factpack", r"facts_bundle", r"证据包"]),
                ("execution_log/执行日志", [r"execution[_-]?log", r"order_log", r"fill_log", r"执行日志"]),
                ("SQLite + WAL", [r"sqlite", r"journal_mode\s*=\s*WAL", r"PRAGMA\s+journal_mode", r"\bWAL\b"]),
                ("浮点 isclose/round", [r"math\.isclose", r"np\.isclose", r"\bisclose\b", r"round\(", r"Decimal"]),
            ],
        },
        "M4": {
            "title": "Night/Morning 两段作业 + orders 协议 + 对账闭环 + trade_cal",
            "pass_ratio": 0.6,
            "checks": [
                ("Night_Job", [r"Night[_-]?Job", r"night_job", r"nightly", r"夜盘|夜间"]),
                ("Morning_Job", [r"Morning[_-]?Job", r"morning_job", r"morning", r"早盘|早间"]),
                ("orders.csv / 订单协议", [r"orders\.csv", r"order(s)?_schema", r"generate_orders", r"订单协议"]),
                ("原子写", [r"atomic_write", r"os\.replace", r"tempfile", r"write.*tmp.*replace"]),
                ("trade_cal / is_trade_day", [r"trade_cal", r"is_trade_day", r"交易日历", r"calendar"]),
                ("reconcile/对账", [r"reconcile", r"ledger", r"对账", r"match.*broker"]),
            ],
        },
        "M5": {
            "title": "STOP + Fat-Finger + AssetCheck + smoke test",
            "pass_ratio": 0.6,
            "checks": [
                ("STOP/熔断", [r"\bSTOP\b", r"circuit_breaker", r"kill_switch", r"熔断", r"保险丝"]),
                ("Fat-Finger", [r"fat[_-]?finger", r"order_size_limit", r"price_guard", r"误操作"]),
                ("AssetCheck", [r"asset[_-]?check", r"balance_check", r"资金校验", r"资产校验"]),
                ("smoke test", [r"smoke[_-]?test", r"sanity_check", r"self_check", r"冒烟测试"]),
            ],
        },
        "M6": {
            "title": "双头模型 shadow（batch+context+熔断降级）",
            "pass_ratio": 0.6,
            "checks": [
                ("dual head", [r"dual[_-]?head", r"two[_-]?head", r"双头"]),
                ("shadow mode", [r"shadow", r"dry_run", r"旁路"]),
                ("batch/context", [r"\bbatch\b", r"context", r"上下文"]),
                ("fallback/降级", [r"fallback", r"degrade", r"graceful", r"降级", r"纯规则"]),
            ],
        },
        "M7": {
            "title": "开闸 rerank + risk gate",
            "pass_ratio": 0.6,
            "checks": [
                ("rerank", [r"rerank", r"re-rank", r"二次排序"]),
                ("risk gate", [r"risk[_-]?gate", r"gatekeeper", r"risk_filter", r"风控闸门"]),
                ("canary/灰度", [r"canary", r"rollout", r"灰度", r"开闸"]),
            ],
        },
        "M8": {
            "title": "除权除息 + Parquet + Patch/Registry/Migrator + Dashboard + Regime",
            "pass_ratio": 0.6,
            "checks": [
                ("除权除息", [r"ex[_-]?div", r"dividend", r"split", r"除权", r"除息"]),
                ("Parquet", [r"parquet", r"pyarrow", r"fastparquet"]),
                ("SchemaMigrator/Registry", [r"SchemaMigrator", r"migrat", r"registry", r"迁移", r"注册"]),
                ("Dashboard", [r"dashboard", r"streamlit", r"gradio", r"可视化"]),
                ("Regime", [r"regime", r"market_state", r"牛熊", r"状态机"]),
            ],
        },
    }


# ============== 核心“硬边界”专项检查（更严格） ==============
HARD_GUARDS = {
    "append_only_snapshot": {
        "desc": "snapshot_raw append-only（尽量只有 INSERT，不做 UPDATE/DELETE 覆盖历史）",
        "patterns": [r"snapshot_raw", r"INSERT\s+INTO\s+snapshot", r"append[_-]?only"],
        "anti_patterns": [r"UPDATE\s+snapshot", r"DELETE\s+FROM\s+snapshot"],
    },
    "no_future_leak": {
        "desc": "避免未来数据泄漏（D+1 open/up_limit/low_limit 不进入 D 日特征）",
        "patterns": [r"D\+1", r"next[_-]?day", r"up_limit|low_limit|limit_up|limit_down", r"label"],
        "anti_patterns": [],  # 很难静态判定，留空
    },
    "trade_calendar_gate": {
        "desc": "pipeline 首步 is_trade_day / trade_cal gate（非交易日退出）",
        "patterns": [r"is_trade_day", r"trade_cal", r"交易日历"],
        "anti_patterns": [],
    },
    "float_isclose": {
        "desc": "金额/收益/对账使用 isclose/round/Decimal，避免直接 ==",
        "patterns": [r"isclose", r"Decimal", r"round\("],
        "anti_patterns": [],
    },
    "sqlite_wal": {
        "desc": "SQLite WAL 或同等并发/可靠性设置",
        "patterns": [r"journal_mode\s*=\s*WAL", r"PRAGMA\s+journal_mode", r"\bWAL\b"],
        "anti_patterns": [],
    },
    "atomic_write_orders": {
        "desc": "orders 输出原子写（tmp + replace）防止半写文件",
        "patterns": [r"os\.replace", r"atomic_write", r"tempfile", r"orders\.csv"],
        "anti_patterns": [],
    },
}


def iter_repo_files(root: Path, ignore_dirs: set) -> List[Path]:
    files: List[Path] = []
    for p in root.rglob("*"):
        if p.is_dir():
            continue
        # skip ignored dirs
        parts = set(p.parts)
        if parts & ignore_dirs:
            continue
        if p.suffix.lower() in SCAN_EXTS:
            files.append(p)
    return files


def read_text_safely(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        try:
            return path.read_text(encoding="gbk", errors="ignore")
        except Exception:
            return ""


def find_patterns_in_files(files: List[Path], patterns: List[str]) -> Dict[str, List[Tuple[str, str]]]:
    """
    返回: pattern -> [(file, matched_snippet), ...]
    """
    compiled = [(pat, re.compile(pat, re.IGNORECASE | re.MULTILINE)) for pat in patterns]
    hits: Dict[str, List[Tuple[str, str]]] = {pat: [] for pat in patterns}

    for f in files:
        text = read_text_safely(f)
        if not text:
            continue
        for pat, cre in compiled:
            m = cre.search(text)
            if m:
                # 抓一点上下文作为证据
                s = max(0, m.start() - 60)
                e = min(len(text), m.end() + 60)
                snippet = text[s:e].replace("\n", " ").replace("\r", " ")
                hits[pat].append((str(f), snippet))
    return hits


def score_check(files: List[Path], regex_list: List[str]) -> Tuple[bool, List[Tuple[str, str, str]]]:
    """
    returns:
      passed, evidence: [(regex, file, snippet), ...] top few
    """
    evidence: List[Tuple[str, str, str]] = []
    compiled = [(rgx, re.compile(rgx, re.IGNORECASE | re.MULTILINE)) for rgx in regex_list]
    for f in files:
        text = read_text_safely(f)
        if not text:
            continue
        for rgx, cre in compiled:
            m = cre.search(text)
            if m:
                s = max(0, m.start() - 60)
                e = min(len(text), m.end() + 60)
                snippet = text[s:e].replace("\n", " ").replace("\r", " ")
                evidence.append((rgx, str(f), snippet))
    passed = len(evidence) > 0
    # 证据去重+截断
    uniq = []
    seen = set()
    for rgx, fp, sn in evidence:
        key = (rgx, fp)
        if key not in seen:
            uniq.append((rgx, fp, sn))
            seen.add(key)
    return passed, uniq[:6]


def milestone_audit(root: Path, files: List[Path]) -> Dict[str, Any]:
    rules = milestone_rules()
    out: Dict[str, Any] = {}
    for mid, m in rules.items():
        checks = m["checks"]
        pass_ratio = float(m["pass_ratio"])
        passed_cnt = 0
        details = []
        for check_name, rgxs in checks:
            ok, ev = score_check(files, rgxs)
            if ok:
                passed_cnt += 1
            details.append({
                "check": check_name,
                "ok": ok,
                "regex": rgxs,
                "evidence": [{"regex": a, "file": b, "snippet": c} for a, b, c in ev],
            })
        ratio = passed_cnt / max(1, len(checks))
        done = ratio >= pass_ratio
        out[mid] = {
            "title": m["title"],
            "done": done,
            "ratio": ratio,
            "passed_checks": passed_cnt,
            "total_checks": len(checks),
            "details": details,
        }
    return out


def hard_guard_audit(files: List[Path]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, cfg in HARD_GUARDS.items():
        patterns = cfg["patterns"]
        anti = cfg.get("anti_patterns", [])

        ok, ev = score_check(files, patterns)
        anti_ok = True
        anti_ev: List[Tuple[str, str, str]] = []
        if anti:
            anti_ok, anti_ev = score_check(files, anti)
            # anti_ok=True 表示找到了反例 => 这不是我们要的，应该视为 FAIL
            anti_found = anti_ok
        else:
            anti_found = False

        status = ok and (not anti_found)
        result[key] = {
            "desc": cfg["desc"],
            "ok": status,
            "found_positive": ok,
            "found_negative": anti_found,
            "positive_evidence": [{"regex": a, "file": b, "snippet": c} for a, b, c in ev],
            "negative_evidence": [{"regex": a, "file": b, "snippet": c} for a, b, c in anti_ev],
        }
    return result


def infer_version(milestones: Dict[str, Any]) -> Tuple[str, str]:
    """
    返回 (current_m, version_str)
    """
    order = ["M0", "M1", "M2", "M3", "M4", "M5", "M6", "M7", "M8"]
    current = "M0"
    for m in order:
        if milestones.get(m, {}).get("done", False):
            current = m
        else:
            break

    # 版本映射（按你的开发计划惯例）
    if current in {"M0", "M1", "M2", "M3", "M4", "M5"}:
        ver = "V1 主线（闭环阶段）" if current == "M5" else f"V1 进行中（已到 {current}）"
    elif current in {"M6", "M7"}:
        ver = f"V1 + 双头模型阶段（已到 {current}）"
    else:
        ver = "V1.1+（工程化/除权除息/看板等）"

    return current, ver


def md_escape(s: str) -> str:
    return s.replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def write_report_md(
    out_path: Path,
    root: Path,
    milestones: Dict[str, Any],
    guards: Dict[str, Any],
    plan_exists: bool
) -> None:
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_m, ver = infer_version(milestones)

    lines: List[str] = []
    lines.append(f"# Quant_System 版本审计报告\n")
    lines.append(f"- 时间: {now}\n")
    lines.append(f"- 根目录: `{root}`\n")
    lines.append(f"- 开发计划文件: {'已检测到' if plan_exists else '未检测到（不影响审计，但建议放根目录：开发计划.txt）'}\n")
    lines.append(f"- 判定完成到: **{current_m}**\n")
    lines.append(f"- 版本归类: **{ver}**\n")

    # 里程碑总览
    lines.append("\n## 里程碑总览（M0~M8）\n")
    lines.append("| Milestone | Done | HitRatio | Passed/Total | Title |\n")
    lines.append("|---|---:|---:|---:|---|\n")
    for mid in ["M0","M1","M2","M3","M4","M5","M6","M7","M8"]:
        m = milestones[mid]
        lines.append(
            f"| {mid} | {'✅' if m['done'] else '❌'} | {m['ratio']:.2f} | {m['passed_checks']}/{m['total_checks']} | {md_escape(m['title'])} |\n"
        )

    # 硬边界
    lines.append("\n## 硬边界检查（实盘可信度关键）\n")
    lines.append("| Guard | OK | 说明 |\n")
    lines.append("|---|---:|---|\n")
    for k, g in guards.items():
        lines.append(f"| `{k}` | {'✅' if g['ok'] else '❌'} | {md_escape(g['desc'])} |\n")

    # 缺口 Top
    missing = []
    for mid, m in milestones.items():
        if not m["done"]:
            # 挑出没过的 check
            for d in m["details"]:
                if not d["ok"]:
                    missing.append((mid, d["check"]))
    guard_missing = [k for k, g in guards.items() if not g["ok"]]

    lines.append("\n## 缺口清单（优先级建议：先补硬边界，再补 M4/M5）\n")
    if guard_missing:
        lines.append("### A. 硬边界未通过（优先修）\n")
        for k in guard_missing[:20]:
            lines.append(f"- `{k}`: {guards[k]['desc']}\n")
    else:
        lines.append("- ✅ 硬边界全部通过\n")

    if missing:
        lines.append("\n### B. 里程碑未覆盖的检查项（迹象不足）\n")
        for mid, ck in missing[:40]:
            lines.append(f"- {mid}: {ck}\n")
    else:
        lines.append("\n- ✅ 里程碑检查项全部命中（仅代表“存在实现迹象”，仍建议跑回测验收）\n")

    # 证据（精简）
    lines.append("\n## 关键证据（命中示例）\n")
    for mid in ["M0","M3","M4","M5","M6","M8"]:
        m = milestones.get(mid)
        if not m:
            continue
        lines.append(f"\n### {mid} - {m['title']}\n")
        for d in m["details"]:
            if d["ok"] and d["evidence"]:
                ev = d["evidence"][0]
                lines.append(f"- ✅ {d['check']}\n")
                lines.append(f"  - file: `{ev['file']}`\n")
                lines.append(f"  - snippet: `{md_escape(ev['snippet'])}`\n")
                break  # 每个 milestone 只展示一条代表证据

    out_path.write_text("".join(lines), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Quant_System 根目录")
    ap.add_argument("--plan", default="开发计划.txt", help="开发计划文件（可选）")
    ap.add_argument("--out", default="reports", help="输出目录")
    ap.add_argument("--no-ignore-data", action="store_true", help="不要忽略 data/ 目录（如果你的 schema 放 data/ 下）")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    ignore_dirs = set(DEFAULT_IGNORE_DIRS)
    if args.no_ignore_data:
        ignore_dirs.discard("data")
        ignore_dirs.discard("datasets")
        ignore_dirs.discard("dataset")

    files = iter_repo_files(root, ignore_dirs)
    plan_path = (root / args.plan)
    plan_exists = plan_path.exists()

    milestones = milestone_audit(root, files)
    guards = hard_guard_audit(files)
    current_m, ver = infer_version(milestones)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    md_path = out_dir / f"version_audit_{ts}.md"
    json_path = out_dir / f"version_audit_{ts}.json"

    write_report_md(md_path, root, milestones, guards, plan_exists)

    payload = {
        "timestamp": ts,
        "root": str(root),
        "plan_exists": plan_exists,
        "current_milestone": current_m,
        "version": ver,
        "milestones": milestones,
        "hard_guards": guards,
        "scanned_files": len(files),
        "ignored_dirs": sorted(list(ignore_dirs)),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # 控制台摘要
    print("=" * 72)
    print("Quant_System 版本审计完成")
    print(f"Root: {root}")
    print(f"Scanned files: {len(files)}")
    print(f"Plan file: {'FOUND' if plan_exists else 'NOT FOUND'} ({plan_path})")
    print(f"Current milestone: {current_m}")
    print(f"Version: {ver}")
    print(f"Report: {md_path}")
    print(f"JSON  : {json_path}")
    print("=" * 72)

    # 快速提示：硬边界
    bad_guards = [k for k, v in guards.items() if not v["ok"]]
    if bad_guards:
        print("⚠️ 硬边界未通过（建议优先修复）:")
        for k in bad_guards:
            print(f" - {k}: {guards[k]['desc']}")
    else:
        print("✅ 硬边界全部通过（仍建议跑回测对照验收）")


if __name__ == "__main__":
    main()
