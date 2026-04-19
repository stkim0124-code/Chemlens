@echo off
chcp 65001 >nul 2>&1
setlocal

set BACKEND=%~dp0
cd /d %BACKEND%

echo =============================================================
echo INJECT REJECTED 8 FAMILIES v3
echo =============================================================
echo.

echo [STEP 0] DRY-RUN preview
python inject_rejected_8_families.py --dry-run --canonical app\labint.db --stage app\labint_v5_stage.db --benchmark benchmark\named_reaction_benchmark_small.json --report-dir reports\inject_rejected_8
if errorlevel 1 (
  echo [ERROR] dry-run failed.
  pause
  exit /b 1
)

echo.
echo =============================================================
echo Review the dry-run result above.
echo Press any key to start REAL injection, or Ctrl+C to cancel.
echo =============================================================
pause

echo.
echo [STEP 1] Applying injection...
python inject_rejected_8_families.py --apply --canonical app\labint.db --stage app\labint_v5_stage.db --benchmark benchmark\named_reaction_benchmark_small.json --report-dir reports\inject_rejected_8
if errorlevel 1 (
  echo [ERROR] injection failed.
  pause
  exit /b 1
)

echo.
echo [STEP 2] Final benchmark verification
python run_named_reaction_benchmark_small.py --benchmark benchmark\named_reaction_benchmark_small.json

echo.
echo [STEP 3] State verification
python VERIFY_CURRENT_BACKEND_STATE.py

echo.
echo =============================================================
echo DONE. Check reports\inject_rejected_8\ for details.
echo =============================================================
pause
