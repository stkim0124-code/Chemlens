@echo off
setlocal
cd /d %~dp0

echo =============================================================
echo CONTINUOUS EXPAND V5 CAMPAIGN
echo =============================================================
echo.
echo This runner keeps screening unresolved registry families and applies

echo only frozen PASS candidates. It is configured to aim for 305 family coverage.
echo Stop manually with Ctrl+C if needed.
echo.

python continuous_gemini_expand_v5.py --family-target 305 --families-per-round 12 --max-attempts 3 --max-rounds 999999 --max-empty-rounds 999999
if errorlevel 1 (
  echo [ERROR] expand v5 run failed.
  exit /b 1
)

echo =============================================================
echo DONE. Check reports\gemini_expand_v5\
echo =============================================================
endlocal
