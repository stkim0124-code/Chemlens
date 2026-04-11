@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"
set "LABINT_DB_PATH=%~dp0app\labint_round9_bridge_work.db"
echo [bridge] LABINT_DB_PATH=%LABINT_DB_PATH%
call run_backend.bat --no-install
