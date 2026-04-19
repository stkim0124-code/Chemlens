@echo off
setlocal
cd /d %~dp0

echo ========================================================================
echo PDF EXAMPLE AUTOMATION - FULL STAGE HARVEST
echo ========================================================================
python pdf_example_automation.py --backend-root .
if errorlevel 1 (
  echo [ERROR] full stage harvest failed.
  exit /b 1
)
