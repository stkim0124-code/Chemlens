@echo off
setlocal
cd /d "%~dp0"
echo ========================================================================
echo GEMINI SINGLE-FAMILY REBUILD - SWERN OXIDATION ROUND1
echo ========================================================================
python gemini_rebuild_single_family.py --config family_prompts\swern_oxidation_round1.json
if errorlevel 1 (
  echo [ERROR] run failed.
  exit /b 1
)
