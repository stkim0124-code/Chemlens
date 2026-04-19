@echo off
setlocal
cd /d %~dp0

echo ========================================================================
echo PDF EXAMPLE AUTOMATION - GEMINI BATCH1 (ENV KEY)
echo ========================================================================
if "%GEMINI_API_KEY%"=="" (
  echo [ERROR] GEMINI_API_KEY is not set in this Anaconda Prompt session.
  echo         Example:
  echo         set GEMINI_API_KEY=YOUR_REAL_KEY
  exit /b 1
)
python pdf_example_automation.py --backend-root . --call-gemini --families "Aldol Reaction;Alkene (Olefin) Metathesis;Diels-Alder Cycloaddition;Swern Oxidation;Ritter Reaction;Pinner Reaction;Schwartz Hydrozirconation;Tsuji-Wilkinson Decarbonylation Reaction;Barton-McCombie Radical Deoxygenation Reaction;Baeyer-Villiger Oxidation/Rearrangement"
if errorlevel 1 exit /b 1
