@echo off
setlocal
cd /d "%~dp0"
echo ========================================================================
echo APPLY SINGLE-FAMILY REBUILD - SWERN OXIDATION APPLY
echo ========================================================================
python apply_single_family_rebuild.py --apply
if errorlevel 1 (
  echo [ERROR] apply failed.
  exit /b 1
)
endlocal
