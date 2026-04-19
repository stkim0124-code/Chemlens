@echo off
setlocal

if "%GEMINI_API_KEY%"=="" (
  echo [ERROR] GEMINI_API_KEY is not set.
  echo Example:
  echo   set GEMINI_API_KEY=your_real_key
  exit /b 1
)

echo ========================================================================
echo PDF EXAMPLE GEMINI STEP2 - SMOKE TEST
echo ========================================================================
python pdf_example_automation.py --backend-root . --call-gemini --families "Alkene (Olefin) Metathesis;Aldol Reaction" --limit-pages 2
if errorlevel 1 (
  echo [ERROR] smoke test failed.
  exit /b 1
)
echo ========================================================================
echo [DONE] smoke test finished.
echo ========================================================================
endlocal
