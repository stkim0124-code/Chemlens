@echo off
setlocal
cd /d "%~dp0"

echo ========================================================================
echo APPLY SINGLE-FAMILY REBUILD - RITTER REACTION ROUND1 APPLY
echo ========================================================================
python apply_single_family_rebuild.py --summary "reports\gemini_single_family_rebuild\20260419_063255\gemini_single_family_rebuild_summary.json" --apply
if errorlevel 1 (
  echo [ERROR] apply failed.
  exit /b 1
)
endlocal
