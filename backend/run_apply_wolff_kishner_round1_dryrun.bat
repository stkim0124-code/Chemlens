@echo off
setlocal
cd /d %~dp0

echo ========================================================================
echo APPLY SINGLE-FAMILY REBUILD - WOLFF-KISHNER REDUCTION DRY-RUN
echo ========================================================================
python apply_single_family_rebuild.py --summary "%~dp0reports\gemini_single_family_rebuild\20260419_062053\gemini_single_family_rebuild_summary.json"
if errorlevel 1 (
  echo [ERROR] dry-run failed.
  exit /b 1
)
endlocal
