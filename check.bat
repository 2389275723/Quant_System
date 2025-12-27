@echo off
setlocal
pushd %~dp0

call scripts\preflight.bat
if errorlevel 1 exit /b %errorlevel%

python scripts\tests\run_all.py
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" exit /b %RC%

python -m compileall -q .
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" exit /b %RC%

call audit.bat
set "RC=%ERRORLEVEL%"
popd
exit /b %RC%
