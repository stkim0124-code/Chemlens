@echo off
setlocal
cd /d %~dp0

echo ========================================================================
echo PDF EXAMPLE AUTOMATION - DRYRUN
echo ========================================================================
python pdf_example_automation.py --backend-root .
if errorlevel 1 (
  echo [ERROR] dryrun failed.
  exit /b 1
)
