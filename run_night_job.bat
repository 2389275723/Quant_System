@echo off
setlocal

cd /d "%~dp0"
call ".venv\Scripts\activate.bat"
set PYTHONPATH=%CD%

call scripts\preflight.bat
if errorlevel 1 exit /b %errorlevel%

set "TRADE_DATE=%~1"
if defined TRADE_DATE (
  echo %TRADE_DATE% | findstr /r "^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$" >nul
  if %errorlevel%==0 (
    shift
  ) else (
    set "TRADE_DATE="
  )
)

if not defined TRADE_DATE (
  for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') do set "TRADE_DATE=%%I"
)

python scripts\safe_run.py night --trade-date %TRADE_DATE% %*
set "RC=%ERRORLEVEL%"

exit /b %RC%
