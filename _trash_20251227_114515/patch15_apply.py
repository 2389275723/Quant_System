# -*- coding: utf-8 -*-
"""
Patch 15: 兼容 universe.exclude_markets，并修复“允许300/688”开关保存逻辑（同时写 exclude_prefixes & exclude_markets）

用法：
1) 解压本补丁到 Quant_System 根目录（与 config.yaml 同级）
2) 运行：  .\.venv\Scripts\python.exe patch15_apply.py
3) 重启 UI，并重新运行一次 Night Job（新 run_id）即可看到过滤后的结果与 research/picks_daily.csv

说明：
- 本补丁不会改你的 token / key。
- 会对被覆盖的文件生成 .bak 备份。
"""
import os
import shutil
from pathlib import Path

PATCH_ROOT = Path(__file__).resolve().parent / "patch15_files"

TARGETS = [
    ("src/jobs/night_job.py", "src/jobs/night_job.py"),
    ("ui/views/settings.py", "ui/views/settings.py"),
]

def copy_with_backup(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        bak = dst.with_suffix(dst.suffix + ".bak")
        shutil.copy2(dst, bak)
        print(f"[backup] {dst} -> {bak}")
    shutil.copy2(src, dst)
    print(f"[patch]  {src} -> {dst}")

def main():
    proj_root = Path.cwd()
    # try auto-detect if user runs from elsewhere
    if not (proj_root / "config.yaml").exists() and (proj_root / "Quant_System").exists():
        proj_root = proj_root / "Quant_System"

    print("[info] project root =", proj_root)
    if not (proj_root / "config.yaml").exists():
        print("[err] 找不到 config.yaml。请在 Quant_System 根目录运行本脚本。")
        return 2

    for s_rel, d_rel in TARGETS:
        src = PATCH_ROOT / s_rel
        dst = proj_root / d_rel
        if not src.exists():
            print("[err] patch file missing:", src)
            return 3
        copy_with_backup(src, dst)

    print("\n[ok] Patch15 已应用。请：\n"
          "1) 重启 Streamlit UI\n"
          "2) 在【系统设置】取消勾选“允许300/301/688/689”（并点保存）\n"
          "3) 重新运行一次 Night Job（会生成新的 run_id）\n"
          "4) 在 Quant_System\\research\\picks_daily.csv 查看导出结果\n")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
