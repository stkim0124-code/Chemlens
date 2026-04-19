@echo off
setlocal
cd /d %~dp0
echo [STEP 0] DRY-RUN selective merge
python merge_v5_stage_selective.py --dry-run
if errorlevel 1 goto :eof
echo ============================================================
echo [CHECK] Review dry-run above.
echo         Press any key to start APPLY selective merge.
echo         Press Ctrl+C to cancel.
echo ============================================================
pause
echo [STEP 1] APPLY selective merge
python merge_v5_stage_selective.py --apply
endlocal
