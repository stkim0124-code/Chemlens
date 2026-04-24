@echo off
setlocal EnableExtensions
cd /d "%~dp0"

echo ========================================================================
echo PDF EXAMPLE GEMINI BATCH1 ULTRASAFE FIXED
echo ========================================================================

echo [INFO] Working directory: %CD%

call conda activate chemlens
if errorlevel 1 (
  echo [ERROR] conda activate chemlens failed.
  pause
  exit /b 1
)

where python
python -c "import sys; print(sys.executable)"
if errorlevel 1 (
  echo [ERROR] python is not available after conda activate.
  pause
  exit /b 1
)

echo.
echo [STEP 0] redact old API key traces in stage DB / reports
echo.
python redact_pdf_example_api_keys.py --backend-root .
if errorlevel 1 (
  echo [ERROR] redact_pdf_example_api_keys.py failed.
  pause
  exit /b 1
)

echo.
echo [STEP 1] run Gemini batch1
echo.
python pdf_example_automation.py --backend-root . --call-gemini --families "Aldol Reaction;Alkene (Olefin) Metathesis;Diels-Alder Cycloaddition;Swern Oxidation;Ritter Reaction;Pinner Reaction;Schwartz Hydrozirconation;Tsuji-Wilkinson Decarbonylation Reaction;Barton-McCombie Radical Deoxygenation Reaction;Baeyer-Villiger Oxidation/Rearrangement" --limit-pages 10 --sleep 60 --max-regions-per-page 1 --cooldown-on-429 0
if errorlevel 1 (
  echo [ERROR] pdf_example_automation.py failed.
  pause
  exit /b 1
)

echo.
echo [STEP 2] verify latest extraction batch
echo.
python verify_pdf_example_gemini_batch.py --backend-root .
if errorlevel 1 (
  echo [ERROR] verify_pdf_example_gemini_batch.py failed.
  pause
  exit /b 1
)

echo.
echo [STEP 3] inspect latest extraction errors
echo.
python inspect_pdf_example_automation_errors.py --backend-root .
if errorlevel 1 (
  echo [ERROR] inspect_pdf_example_automation_errors.py failed.
  pause
  exit /b 1
)

echo.
echo ========================================================================
echo [DONE] Batch1 finished.
echo ========================================================================
pause
exit /b 0
