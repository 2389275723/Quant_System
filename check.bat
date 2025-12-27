@echo off
setlocal
pushd %~dp0

set "RC=0"

call scripts\preflight.bat
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" goto :end

python scripts\tests\run_all.py
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" goto :end

python -m compileall -q .
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" goto :end

call audit.bat
set "RC=%ERRORLEVEL%"

:end
popd
exit /b %RC%
