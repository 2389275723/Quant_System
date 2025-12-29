\# Quant\_System - Codex instructions (Windows)



\## Hard rules

\- NEVER create/commit `.env` or any secrets. If config examples are needed, use `.env.example` only.

\- Prefer minimal, surgical diffs. Keep changes small and testable.

\- After any code change, run:

&nbsp; - `python -m compileall -q src`

&nbsp; - `run\_morning\_job.bat 2025-12-26 --cfg config\\config.yaml`

&nbsp; - `run\_night\_job.bat 2025-12-26 --cfg config\\config.yaml`

\- If a change affects IO/data, add/adjust a small unit test under `scripts/tests`.



\## Project context

\- Tushare goes via HTTP gateway: `TUSHARE\_HTTP\_URL` env var.

\- Token via env var: `TUSHARE\_TOKEN`.

\- Jobs:

&nbsp; - morning: generates `bridge\\outbox\\orders.csv`

&nbsp; - night: produces nightly artifacts and must never crash on scalar/NaN edge cases.



\## Safety

\- Default approval mode should be conservative (ask before edits/commands). Use `/diff` and `/review` before committing.



