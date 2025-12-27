Patch13 说明（排除科创/创业/北交所过滤在 AI 辩论页生效）

现象：
- 夜间选股 TopN 可能已经过滤了 300/301/688/689/BJ
- 但 AI 辩论页偶尔仍显示这些票（常见原因：历史 run 结果 / 展示层没再过滤）

补丁做的事：
1) src/jobs/night_job.py：确保 apply_universe_filters() 一定带 exclude_prefixes=exclude_prefixes（避免参数写坏）
2) ui/views/model_lab.py：在展示/重排前再次应用 Universe 过滤（兜底隐藏 300/301/688/689 和 .BJ）

使用方式：
1) 把 patch13_apply.py 放到 Quant_System 根目录（与 ui/、src/ 同级）
2) 在 Quant_System 目录运行：
   .\.venv\Scripts\python.exe patch13_apply.py
3) 重启 UI（run_all.bat / run_ui.bat）

回滚：
- 被改动的文件会生成同目录 .bak 备份，直接替换回去即可。
