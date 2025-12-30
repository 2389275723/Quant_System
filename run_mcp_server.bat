@echo off
setlocal
cd /d %~dp0

set PY=.\.venv_mcp\Scripts\python.exe
if not exist "%PY%" (
  echo [ERR] .venv_mcp not found. Create it first:
  echo   py -3.12 -m venv .venv_mcp
  echo   .\.venv_mcp\Scripts\python.exe -m pip install -r tools\mcp\requirements-mcp.txt
  exit /b 1
)

set ARGS=%*
if "%ARGS%"=="" set ARGS=--http

%PY% tools\mcp\quant_server.py %ARGS%
