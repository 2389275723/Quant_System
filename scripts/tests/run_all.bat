@echo off
setlocal
cd /d %~dp0\..\..
python -m scripts.tests.run_all
set rc=%errorlevel%
echo exit_code=%rc%
exit /b %rc%
