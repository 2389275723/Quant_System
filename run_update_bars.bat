@echo off
setlocal

cd /d "%~dp0"
call ".venv\Scripts\activate.bat"
set PYTHONPATH=%CD%

REM Usage:
REM   run_update_bars.bat 20251224
python main.py update-bars --trade-date %1
pause
