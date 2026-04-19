@echo off
setlocal
cd /d %~dp0

echo ========================================================================
echo APPLY SINGLE-FAMILY REBUILD - PINNER REACTION ROUND2 DRY-RUN
echo ========================================================================
python apply_single_family_rebuild.py --summary reports\gemini_single_family_rebuild\20260419_062836\gemini_single_family_rebuild_summary.json
if errorlevel 1 (
  echo [ERROR] dry-run failed.
  exit /b 1
)
endlocal
