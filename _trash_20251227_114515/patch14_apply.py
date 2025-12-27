# -*- coding: utf-8 -*-
"""Patch14: Fix AI辩论/展示使用最新 RunID 的 picks，并导出 picks_daily.csv

Symptoms:
- 你明明改了“排除科创/创业/北交所/市值上限”，但 AI辩论/今日金股 仍然出现 300/688 等股票。
  原因通常是：同一天你跑了多次 Night Job，SQLite 里有多套结果，页面未按“最新 run_id”取数。

- 你在磁盘上找不到 picks_daily.csv。
  原因是：此前只写入 SQLite，页面下载是临时生成，不会落盘。

This patch will:
1) model_lab 页面按最新 run_id 读取 picks_daily/model_scores_daily
2) night_job 结束时同时导出：
   - research/picks_daily.csv
   - research/runs/<run_id>/picks_daily.csv

Usage:
  cd <Quant_System 根目录>
  .\.venv\Scripts\python.exe patch14_apply.py

After apply:
  Restart Streamlit (Ctrl+C then run_all.bat / run_ui.bat)
"""

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "patch14_files"

FILES = [
    (SRC / "ui" / "views" / "model_lab.py", ROOT / "ui" / "views" / "model_lab.py"),
    (SRC / "src" / "jobs" / "night_job.py", ROOT / "src" / "jobs" / "night_job.py"),
]


def backup_once(path: Path):
    bak = path.with_suffix(path.suffix + ".bak")
    if path.exists() and not bak.exists():
        shutil.copy2(path, bak)


def main():
    changed = 0
    for src, dst in FILES:
        if not src.exists():
            print(f"[ERROR] missing patch file: {src}")
            continue
        if not dst.exists():
            print(f"[ERROR] missing target file: {dst}")
            continue
        backup_once(dst)
        shutil.copy2(src, dst)
        print(f"[patched] {dst}")
        changed += 1

    print(f"\nDone. changed_files={changed}")
    print("Restart Streamlit if it's running.")


if __name__ == "__main__":
    main()
