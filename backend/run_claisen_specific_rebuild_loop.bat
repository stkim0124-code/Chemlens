@echo off
setlocal
cd /d %~dp0

echo =============================================================
echo CONTINUOUS GEMINI REBUILD LOOP - CLAISEN ONLY
echo =============================================================

echo.
echo This runner screens ONLY Claisen Condensation / Claisen Reaction.
echo It applies only benchmark-PASS frozen candidates.
echo.

python continuous_gemini_rebuild_one_family.py --family "Claisen Condensation / Claisen Reaction" --max-attempts 3 --max-rounds 8 --max-empty-rounds 3
if errorlevel 1 (
  echo [ERROR] Claisen loop failed.
  pause
  exit /b 1
)

echo.
echo =============================================================
echo DONE. Check reports\gemini_rebuild_single_family_loop\
echo =============================================================
pause
endlocal
