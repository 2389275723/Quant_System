# -*- coding: utf-8 -*-
"""Patch13: ensure '排除科创/创业/北交所' filters apply consistently (Night Job + AI辩论显示)

- Creates .bak backups next to modified files (only once)

Usage:
  cd <Quant_System root>
  .\.venv\Scripts\python.exe patch13_apply.py
"""

import re
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def backup_once(path: Path):
    bak = path.with_suffix(path.suffix + ".bak")
    if not bak.exists():
        shutil.copy2(path, bak)

def patch_text(path: Path, fn):
    if not path.exists():
        print(f"[skip] missing: {path}")
        return False
    raw = path.read_text(encoding="utf-8", errors="ignore")
    new = fn(raw)
    if new == raw:
        print(f"[ok] no change: {path}")
        return False
    backup_once(path)
    path.write_text(new, encoding="utf-8")
    print(f"[patched] {path}")
    return True

def ensure_import(text: str, import_line: str, after_regex: str) -> str:
    if import_line in text:
        return text
    m = re.search(after_regex, text, flags=re.M)
    if not m:
        m = re.search(r"^(import .+|from .+ import .+)\n", text, flags=re.M)
    if not m:
        return import_line + "\n" + text
    insert_at = m.end()
    return text[:insert_at] + import_line + "\n" + text[insert_at:]

def patch_night_job(text: str) -> str:
    # 1) default exclude_prefixes -> []
    text = re.sub(
        r'get\(\s*cfg\s*,\s*["\']universe\.exclude_prefixes["\']\s*,\s*\[[^\]]*\]\s*\)',
        'get(cfg, "universe.exclude_prefixes", [])',
        text
    )

    # 2) ensure apply_universe_filters has exclude_prefixes=exclude_prefixes
    def fix_call(match: re.Match) -> str:
        call = match.group(0)
        if "exclude_prefixes=exclude_prefixes" in call:
            return call
        if "exclude_bj=" in call and "exclude_prefixes" not in call:
            return call.replace(
                "apply_universe_filters(snap,",
                "apply_universe_filters(snap, exclude_prefixes=exclude_prefixes,",
                1
            )
        return re.sub(
            r"apply_universe_filters\(\s*snap\s*,\s*[^,]+,",
            "apply_universe_filters(snap, exclude_prefixes=exclude_prefixes,",
            call
        )

    text = re.sub(r"apply_universe_filters\(\s*snap\s*,[^\n]*\)", fix_call, text)
    return text

FILTER_BLOCK = '''
# PATCH13_UNIVERSE_FILTER: make AI辩论/展示与夜间选股使用同一套 Universe 过滤
exclude_prefixes = (cfg.get("universe", {}) or {}).get("exclude_prefixes", [])
exclude_bj = bool((cfg.get("universe", {}) or {}).get("exclude_bj", False))
max_total_mv_yi = float((cfg.get("universe", {}) or {}).get("max_total_mv_yi", 0) or 0)
max_total_mv = max_total_mv_yi * 1e8 if max_total_mv_yi and max_total_mv_yi > 0 else None
try:
    df = apply_universe_filters(df, exclude_prefixes=exclude_prefixes, exclude_bj=exclude_bj, max_total_mv=max_total_mv)
except Exception:
    # UI 展示层兜底：过滤失败也不影响主流程
    pass
'''

def patch_model_lab(text: str) -> str:
    text = ensure_import(
        text,
        "from src.engine.filters import apply_universe_filters",
        r"^(import streamlit as st|from ui\.)\n"
    )
    if "PATCH13_UNIVERSE_FILTER" in text:
        return text

    patterns = [
        r"(\n\s*df\s*=\s*_merge_scores\([^\n]*\)\s*\n)",
        r"(\n\s*df\s*=\s*_load_latest\([^\n]*\)\s*\n)",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            insert_at = m.end(1)
            return text[:insert_at] + FILTER_BLOCK + "\n" + text[insert_at:]

    anchor = re.search(r"\n\s*st\.subheader\([\"\']今日金股", text)
    if anchor:
        insert_at = anchor.start()
        return text[:insert_at] + "\n" + FILTER_BLOCK + "\n" + text[insert_at:]

    return text + "\n" + FILTER_BLOCK + "\n"

def main():
    changed = 0
    changed += patch_text(ROOT / "src" / "jobs" / "night_job.py", patch_night_job)
    changed += patch_text(ROOT / "ui" / "views" / "model_lab.py", patch_model_lab)
    print("\nDone. changed_files =", changed)
    print("If Streamlit is running, restart it (Ctrl+C then run_all.bat / run_ui.bat).")

if __name__ == "__main__":
    main()
