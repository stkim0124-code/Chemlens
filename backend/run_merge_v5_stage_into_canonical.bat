@echo off
setlocal
cd /d %~dp0

echo [STEP 1] DRY-RUN
python merge_v5_stage_into_canonical.py --dry-run
if errorlevel 1 goto :end

echo.
echo [CHECK] If the dry-run numbers look correct, press any key to APPLY.
pause >nul

echo [STEP 2] APPLY
python merge_v5_stage_into_canonical.py --apply
if errorlevel 1 goto :end

echo.
echo [STEP 3] Run gate benchmark on canonical
python run_named_reaction_benchmark_small.py --benchmark benchmark\named_reaction_benchmark_small.json

echo.
echo [STEP 4] Optional current-state verification
python VERIFY_CURRENT_BACKEND_STATE.py

:end
echo.
echo [DONE]
endlocal
