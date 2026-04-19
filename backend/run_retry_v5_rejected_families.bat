@echo off
setlocal
cd /d %~dp0

echo [STEP 0] DRY-RUN rejected-family subset retry
python retry_v5_rejected_families.py --dry-run
if errorlevel 1 exit /b %errorlevel%

echo ============================================================
echo [CHECK] Review dry-run above.
echo        Press any key to start APPLY rejected-family subset retry.
echo        Press Ctrl+C to cancel.
echo ============================================================
pause

echo [STEP 1] APPLY rejected-family subset retry
python retry_v5_rejected_families.py --apply
exit /b %errorlevel%
