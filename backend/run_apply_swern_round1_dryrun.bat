@echo off
setlocal
cd /d "%~dp0"
echo ========================================================================
echo APPLY SINGLE-FAMILY REBUILD - SWERN OXIDATION DRY-RUN
echo ========================================================================
python apply_single_family_rebuild.py
if errorlevel 1 (
  echo [ERROR] dry-run failed.
  exit /b 1
)
endlocal
