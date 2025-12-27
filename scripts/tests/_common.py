# scripts/tests/_common.py
from __future__ import annotations
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SAFE_RUN = ROOT / "scripts" / "safe_run.py"

def run_safe(job: str, trade_date: str, timeout: int = 120):
    cmd = [sys.executable, str(SAFE_RUN), job, "--trade-date", trade_date]
    return subprocess.run(
        cmd,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )

def out(cp) -> str:
    return (cp.stdout or "") + "\n" + (cp.stderr or "")

def has_any(text: str, keywords: list[str]) -> bool:
    t = text.lower()
    return any(k.lower() in t for k in keywords)
