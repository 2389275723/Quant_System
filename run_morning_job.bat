@echo off
setlocal

cd /d "%~dp0"
set "PYTHON_EXE=.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
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

set "REST_ARGS="
:collect_args
if "%~1"=="" goto args_done
if not defined REST_ARGS (
  set "REST_ARGS=%~1"
) else (
  set "REST_ARGS=%REST_ARGS% %~1"
)
shift
goto collect_args
:args_done

"%PYTHON_EXE%" scripts\safe_run.py morning --trade-date %TRADE_DATE% %REST_ARGS%
set "RC=%ERRORLEVEL%"

exit /b %RC%
