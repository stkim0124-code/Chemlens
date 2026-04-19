@echo off
setlocal
cd /d %~dp0

echo =============================================================
echo CONTINUOUS GEMINI REBUILD LOOP FOR REMAINING 4
echo =============================================================
echo.
echo This runner keeps screening remaining 4 families and applies only frozen PASS candidates.
echo.
python continuous_gemini_rebuild_remaining4.py --apply --max-attempts 3 --max-rounds 8 --max-empty-rounds 3
if errorlevel 1 (
  echo [ERROR] continuous remaining4 loop failed.
  pause
  exit /b 1
)
echo.
echo =============================================================
echo DONE. Check reports\gemini_rebuild_remaining4_loop\
echo =============================================================
pause
endlocal
