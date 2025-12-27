@echo off
setlocal

rem Ensure we run from repo root regardless of current directory
pushd "%~dp0\.." || exit /b 1

python scripts\tests\run_all.py
set "RC=%ERRORLEVEL%"

popd
if not "%RC%"=="0" exit /b %RC%

echo OK
exit /b 0
