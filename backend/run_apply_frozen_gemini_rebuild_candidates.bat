@echo off
setlocal
cd /d %~dp0

echo =============================================================
echo APPLY FROZEN GEMINI REBUILD CANDIDATES
echo =============================================================

echo.
echo [STEP 0] DRY-RUN using latest DRY-RUN summary and excluding latest APPLY summary
python apply_frozen_gemini_rebuild_candidates.py --dry-run
if errorlevel 1 (
  echo [ERROR] dry-run failed.
  pause
  exit /b 1
)

echo =============================================================
echo Review the dry-run result above.
echo Press any key to start REAL apply, or Ctrl+C to cancel.
echo =============================================================
pause >nul

echo.
echo [STEP 1] Applying frozen accepted families
python apply_frozen_gemini_rebuild_candidates.py
if errorlevel 1 (
  echo [ERROR] apply failed.
  pause
  exit /b 1
)

echo.
echo =============================================================
echo DONE. Check reports\gemini_rebuild_rejected8_frozen_apply\
echo =============================================================
pause
