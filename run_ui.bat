@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

REM 1) create venv
if not exist ".venv\" (
  python -m venv .venv
)

REM 2) install deps
call ".venv\Scripts\activate.bat"
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

REM 3) env
set STREAMLIT_CONFIG_DIR=%CD%\config\.streamlit
set PYTHONPATH=%CD%

REM 4) run
streamlit run ui\app.py
