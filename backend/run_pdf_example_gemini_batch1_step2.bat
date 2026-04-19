@echo off
setlocal

if "%GEMINI_API_KEY%"=="" (
  echo [ERROR] GEMINI_API_KEY is not set.
  echo Example:
  echo   set GEMINI_API_KEY=your_real_key
  exit /b 1
)

echo ========================================================================
echo PDF EXAMPLE GEMINI STEP2 - BATCH 1 (10 FAMILIES)
echo ========================================================================
python pdf_example_automation.py --backend-root . --call-gemini --families "Aldol Reaction;Alkene (Olefin) Metathesis;Diels-Alder Cycloaddition;Swern Oxidation;Ritter Reaction;Pinner Reaction;Schwartz Hydrozirconation;Tsuji-Wilkinson Decarbonylation Reaction;Barton-McCombie Radical Deoxygenation Reaction;Baeyer-Villiger Oxidation/Rearrangement"
if errorlevel 1 (
  echo [ERROR] Batch 1 failed.
  exit /b 1
)
echo ========================================================================
echo [DONE] Batch 1 finished.
echo ========================================================================
endlocal
