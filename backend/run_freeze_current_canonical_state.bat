@echo off
setlocal
cd /d %~dp0
echo =============================================================
echo FREEZE CURRENT CANONICAL STATE
echo =============================================================
python freeze_current_canonical_state.py
if errorlevel 1 goto :fail
echo.
echo DONE. Check reports\stable_freeze and backups\stable_freeze
echo =============================================================
endlocal
exit /b 0
:fail
echo.
echo [ERROR] freeze failed.
echo =============================================================
endlocal
exit /b 1
