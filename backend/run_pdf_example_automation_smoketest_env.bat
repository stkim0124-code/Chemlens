@echo off
setlocal
cd /d %~dp0

echo ========================================================================
echo PDF EXAMPLE AUTOMATION - GEMINI SMOKETEST (ENV KEY)
echo ========================================================================
if "%GEMINI_API_KEY%"=="" (
  echo [ERROR] GEMINI_API_KEY is not set in this Anaconda Prompt session.
  echo         Example:
  echo         set GEMINI_API_KEY=YOUR_REAL_KEY
  exit /b 1
)
python pdf_example_automation.py --backend-root . --call-gemini --families "Alkene (Olefin) Metathesis;Aldol Reaction" --limit-pages 2
if errorlevel 1 exit /b 1
