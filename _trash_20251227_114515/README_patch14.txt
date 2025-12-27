Patch14 (V1.5.3): 解决“AI辩论仍显示科创/创业/老结果” + 导出 picks_daily.csv

你会遇到：
1) 明明设置里已经【不允许 300/301/688/689】并且【排除北交所】并保存，但 AI辩论页仍出现 688/300。
   常见原因：同一天跑了多次 Night Job，SQLite 里存了多套 picks，页面没按最新 run_id 取。

2) 磁盘上找不到 picks_daily.csv。
   原因：此前只写 SQLite；页面“下载”是临时生成，不会落盘。

本补丁做了：
- AI辩论/模型实验室页面按【最新 run_id】读取 picks_daily + model_scores_daily
- Night Job 结束后导出 CSV：
  - research/picks_daily.csv
  - research/runs/<run_id>/picks_daily.csv

使用方法：
1) 解压本补丁到 Quant_System 根目录（和 run_ui.bat 同级）。
2) 运行：
   .\.venv\Scripts\python.exe patch14_apply.py
3) 重启 UI：Ctrl+C 停掉 Streamlit，然后重新 run_ui.bat / run_all.bat
4) 回到 UI 点击【运行 Night Job】重新生成。

验证：
- 看 AI辩论页不应再出现 300/688/8xxxx/BJ 的股票（除非你勾选了允许）。
- 在 Quant_System\research\picks_daily.csv 能看到当天 picks。
