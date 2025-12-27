


@echo off
chcp 65001 >nul

rem 切到bat所在目录（这里的 %~dp0 必须是单百分号）
pushd "%~dp0" || (echo [ERR] cannot pushd to "%~dp0" & pause & exit /b 1)

if not exist "开发计划.txt" (
  echo Plan file NOT FOUND: "%~dp0开发计划.txt"
  dir /b
  popd
  pause
  exit /b 1
)

python scripts\version_audit.py --root . --plan "开发计划.txt" --out reports

popd
pause
