@echo off
setlocal
cd /d %~dp0

echo ========================================================================
echo PDF EXAMPLE AUTOMATION - GEMINI
echo ========================================================================
python pdf_example_automation.py --backend-root . --call-gemini --prompt-api-key
if errorlevel 1 (
  echo [ERROR] gemini run failed.
  exit /b 1
)
