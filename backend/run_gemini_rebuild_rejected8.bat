@echo off
setlocal
cd /d %~dp0
echo =============================================================
echo GEMINI REBUILD REJECTED 8 SEEDS
echo =============================================================
echo.
echo [STEP 0] DRY-RUN generation + benchmark screening
python gemini_rebuild_rejected8_seeds.py
if errorlevel 1 goto :dryfail
echo.
echo =============================================================
echo Review the dry-run result above.
echo Press any key to start REAL apply for accepted families only,
echo or Ctrl+C to cancel.
echo =============================================================
pause
echo.
echo [STEP 1] Applying accepted families only
python gemini_rebuild_rejected8_seeds.py --apply
if errorlevel 1 goto :applyfail
echo.
echo =============================================================
echo DONE. Check reports\gemini_rebuild_rejected8\ for details.
echo =============================================================
goto :eof
:dryfail
echo [ERROR] dry-run failed.
pause
exit /b 1
:applyfail
echo [ERROR] apply failed.
pause
exit /b 1
