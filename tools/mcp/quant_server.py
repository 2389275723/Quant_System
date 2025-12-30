from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

from mcp.server.fastmcp import FastMCP

# Optional: align with core pipeline reconcile_status location
try:
    from src.bridge.reconciliation import RECONCILE_STATUS_PATH as _RECONCILE_STATUS_PATH  # type: ignore
except Exception:  # pragma: no cover
    _RECONCILE_STATUS_PATH = None

ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "logs"
ORDERS_CSV = ROOT / "bridge" / "outbox" / "orders.csv"

def _read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))

def _iter_reconcile_status_files() -> list[Path]:
    """Return existing reconcile_status json files (core + logs + data/manual)."""
    candidates: list[Path] = []
    # 1) Core pipeline canonical path (if available)
    if _RECONCILE_STATUS_PATH:
        p = Path(str(_RECONCILE_STATUS_PATH))
        if not p.is_absolute():
            p = ROOT / p
        if p.exists():
            candidates.append(p)
    # 2) Common locations
    for d in [LOG_DIR, ROOT / "data" / "manual", ROOT / "data"]:
        if not d.exists():
            continue
        # both reconcile_status.json and reconcile_status_*.json
        for p in d.glob("reconcile_status*.json"):
            if p.exists():
                candidates.append(p)
        p0 = d / "reconcile_status.json"
        if p0.exists():
            candidates.append(p0)
    # de-dup
    seen: set[str] = set()
    uniq: list[Path] = []
    for p in candidates:
        key = str(p.resolve())
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)
    return uniq

def _pick_latest(paths: list[Path]) -> Path | None:
    if not paths:
        return None
    return max(paths, key=lambda p: p.stat().st_mtime)

mcp = FastMCP("quant-system")

@mcp.tool()
def project_info() -> Dict[str, Any]:
    """返回项目版本、git hash、tag 等信息。"""
    # best-effort git info (no hard dependency)
    git_dir = ROOT / ".git"
    return {
        "ok": True,
        "root": str(ROOT),
        "git_present": git_dir.exists(),
        "time": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

@mcp.tool()
def diagnose_artifacts() -> Dict[str, Any]:
    """
    诊断：告诉你当前能找到哪些关键产物（logs、reconcile_status、orders.csv）。
    reconcile_status 可能来自：
      - src.bridge.reconciliation.RECONCILE_STATUS_PATH（优先）
      - data/manual/reconcile_status*.json
      - logs/reconcile_status*.json
    """
    reconcile_files = _iter_reconcile_status_files()
    latest_status = _pick_latest(reconcile_files)

    log_files = sorted([p.name for p in LOG_DIR.glob("*.json")]) if LOG_DIR.exists() else []
    return {
        "ok": True,
        "log_dir": str(LOG_DIR),
        "log_json_count": len(log_files),
        "log_json_sample": log_files[-10:],
        "reconcile_status_candidates": [str(p) for p in reconcile_files[-10:]],
        "latest_reconcile_status_path": str(latest_status) if latest_status else None,
        "orders_csv": {"path": str(ORDERS_CSV), "exists": ORDERS_CSV.exists()},
    }

@mcp.tool()
def get_latest_reconcile_status() -> Dict[str, Any]:
    """读取最新的 reconcile_status*.json（按 mtime 选最新，支持 data/manual + logs）。"""
    candidates = _iter_reconcile_status_files()
    latest = _pick_latest(candidates)
    if not latest:
        return {
            "ok": False,
            "reason": "RECONCILE_STATUS_NOT_FOUND",
            "searched": [str(LOG_DIR), str(ROOT / "data" / "manual"), str(ROOT / "data")],
            "hint": "请先跑 run_night_job.bat / run_morning_job.bat（交易日）生成 reconcile_status。",
        }

    try:
        payload = _read_json(latest)
    except Exception as e:
        return {"ok": False, "reason": "RECONCILE_STATUS_PARSE_FAIL", "path": str(latest), "error": str(e)}

    return {"ok": True, "path": str(latest), "data": payload}

@mcp.tool()
def get_orders_head(n: int = 20) -> Dict[str, Any]:
    """读取 bridge/outbox/orders.csv 前 N 行（以 Dict 形式返回）。"""
    if not ORDERS_CSV.exists():
        return {"ok": False, "reason": "ORDERS_CSV_NOT_FOUND", "path": str(ORDERS_CSV)}
    rows: list[dict[str, Any]] = []
    with ORDERS_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for i, r in enumerate(reader):
            rows.append(r)
            if i + 1 >= n:
                break
    return {"ok": True, "path": str(ORDERS_CSV), "rows": rows, "n": len(rows)}

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", help="run via streamable-http transport")
    args = parser.parse_args()

    if args.http:
        # mcp python package supports streamable-http
        mcp.run(transport="streamable-http")
    else:
        mcp.run()

if __name__ == "__main__":
    main()
