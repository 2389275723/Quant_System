@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

REM Defaults
set "CFG=config\config.yaml"
set "TRADE_DATE="

:parse
if "%~1"=="" goto run

if /i "%~1"=="--cfg" (
  if "%~2"=="" (echo [ERR] --cfg requires a path& exit /b 2)
  set "CFG=%~2"
  shift
  shift
  goto parse
)

if /i "%~1"=="--trade-date" (
  if "%~2"=="" (echo [ERR] --trade-date requires a date& exit /b 2)
  set "TRADE_DATE=%~2"
  shift
  shift
  goto parse
)

REM If first arg looks like a date, accept it as trade_date
set "ARG=%~1"
echo %ARG%| findstr /r "^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$" >nul && (
  set "TRADE_DATE=%ARG%"
  shift
  goto parse
)

echo %ARG%| findstr /r "^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$" >nul && (
  set "TRADE_DATE=%ARG:~0,4%-%ARG:~4,2%-%ARG:~6,2%"
  shift
  goto parse
)

echo [ERR] Unknown arg: %~1
exit /b 2

:run
if "%TRADE_DATE%"=="" (
  for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TRADE_DATE=%%i"
)

echo Running: .venv\Scripts\python.exe main.py --cfg "%CFG%" --trade-date "%TRADE_DATE%" update-bars
.\.venv\Scripts\python.exe main.py --cfg "%CFG%" --trade-date "%TRADE_DATE%" update-bars
set "EC=%ERRORLEVEL%"
if not "%EC%"=="0" (
  echo [ERR] update-bars failed with code %EC%
  pause
  exit /b %EC%
)

echo OK: bars updated -> %cd%\data\bars\daily_bars.csv
pause
exit /b 0
