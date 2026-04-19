@echo off
setlocal
cd /d "%~dp0"
echo ========================================================================
echo APPLY SINGLE-FAMILY REBUILD - TSUJI-WILKINSON DECARBONYLATION REACTION ROUND1 APPLY
echo ========================================================================
python apply_single_family_rebuild.py --summary reports\gemini_single_family_rebuild\20260419_064232\gemini_single_family_rebuild_summary.json --apply
if errorlevel 1 (
  echo [ERROR] apply failed.
  exit /b 1
)
