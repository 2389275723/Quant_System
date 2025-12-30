from __future__ import annotations

import csv
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "logs"
ORDERS_CSV = ROOT / "bridge" / "outbox" / "orders.csv"
VERSION_TXT = ROOT / "VERSION.txt"

mcp = FastMCP("Quant_System", json_response=True)


def _run_git(args: List[str]) -> str:
    try:
        cp = subprocess.run(
            ["git", *args],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=True,
        )
        return cp.stdout.strip()
    except Exception:
        return ""


def _read_text(path: Path, max_bytes: int = 200_000) -> str:
    data = path.read_bytes()[:max_bytes]
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("utf-8", errors="replace")


def _read_json(path: Path) -> Any:
    return json.loads(_read_text(path))


def _glob_latest(patterns: List[str], base: Path) -> Optional[Path]:
    cands: List[Path] = []
    for pat in patterns:
        cands.extend(base.glob(pat))
    cands = [p for p in cands if p.is_file()]
    if not cands:
        return None
    return max(cands, key=lambda p: p.stat().st_mtime)


def _orders_preview(path: Path, n: int = 20) -> Dict[str, Any]:
    if not path.exists():
        return {"ok": False, "reason": "ORDERS_NOT_FOUND", "path": str(path)}
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= max(1, n):
                break
            rows.append(row)
    return {"ok": True, "path": str(path), "rows": rows, "rows_returned": len(rows)}


@mcp.tool()
def project_info() -> Dict[str, Any]:
    """返回项目版本、git hash、tag 等信息。"""
    version = VERSION_TXT.read_text(encoding="utf-8").strip() if VERSION_TXT.exists() else ""
    head = _run_git(["rev-parse", "--short", "HEAD"])
    desc = _run_git(["describe", "--tags", "--always"])
    branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"])
    return {
        "ok": True,
        "root": str(ROOT),
        "version_txt": version,
        "git": {"branch": branch, "head": head, "describe": desc},
    }


@mcp.tool()
def diagnose_artifacts() -> Dict[str, Any]:
    """
    诊断：告诉你当前能找到哪些关键产物（logs、reconcile_status、orders.csv）。
    """
    latest_status = _glob_latest(
        ["reconcile_status*.json", "*reconcile*status*.json", "*.reconcile*.json", "*.status*.json"],
        LOG_DIR,
    ) if LOG_DIR.exists() else None

    log_files = sorted([p.name for p in LOG_DIR.glob("*.json")]) if LOG_DIR.exists() else []
    return {
        "ok": True,
        "log_dir": str(LOG_DIR),
        "log_json_count": len(log_files),
        "log_json_sample": log_files[-10:],
        "latest_reconcile_status": str(latest_status) if latest_status else None,
        "orders_csv": {"path": str(ORDERS_CSV), "exists": ORDERS_CSV.exists()},
    }


@mcp.tool()
def get_latest_reconcile_status() -> Dict[str, Any]:
    """读取 logs 下最新的 reconcile_status*.json（按 mtime 选最新）。"""
    if not LOG_DIR.exists():
        return {"ok": False, "reason": "LOG_DIR_NOT_FOUND", "log_dir": str(LOG_DIR)}

    latest = _glob_latest(
        ["reconcile_status*.json", "*reconcile*status*.json", "*.reconcile*.json", "*.status*.json"],
        LOG_DIR,
    )
    if not latest:
        return {"ok": False, "reason": "RECONCILE_STATUS_NOT_FOUND", "log_dir": str(LOG_DIR)}

    try:
        payload = _read_json(latest)
    except Exception as e:
        return {"ok": False, "reason": "RECONCILE_STATUS_PARSE_FAIL", "path": str(latest), "error": str(e)}

    return {"ok": True, "path": str(latest), "data": payload}


@mcp.tool()
def get_orders_head(n: int = 20) -> Dict[str, Any]:
    """读取 bridge/outbox/orders.csv 前 N 行（以 Dict 形式返回）。"""
    n = int(max(1, min(n, 200)))
    return _orders_preview(ORDERS_CSV, n=n)


@mcp.resource("quant://reconcile_status/latest")
def r_latest_reconcile_status() -> str:
    """Resource：最新 reconcile_status（原始 JSON 文本，截断到 200KB）。"""
    if not LOG_DIR.exists():
        return json.dumps({"ok": False, "reason": "LOG_DIR_NOT_FOUND"}, ensure_ascii=False)

    latest = _glob_latest(
        ["reconcile_status*.json", "*reconcile*status*.json", "*.reconcile*.json", "*.status*.json"],
        LOG_DIR,
    )
    if not latest:
        return json.dumps({"ok": False, "reason": "RECONCILE_STATUS_NOT_FOUND"}, ensure_ascii=False)

    return _read_text(latest)


@mcp.resource("quant://orders/latest")
def r_orders_latest() -> str:
    """Resource：orders.csv（前 200KB 原始文本）。"""
    if not ORDERS_CSV.exists():
        return "ORDERS_NOT_FOUND"
    return _read_text(ORDERS_CSV)


def main() -> None:
    # Claude Desktop / Cursor 一般走 stdio；需要调试时可改成 streamable-http。
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--http", action="store_true", help="use streamable-http transport for debugging")
    args = ap.parse_args()

    if args.http:
        # 官方示例 transport=streamable-http（便于用 inspector 调试）
        mcp.run(transport="streamable-http")
    else:
        # Claude Desktop/多数客户端默认使用 stdio
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
