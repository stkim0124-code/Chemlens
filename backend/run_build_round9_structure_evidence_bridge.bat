@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"
echo [bridge] build labint_round9_bridge_work.db ...
python build_round9_structure_evidence_bridge.py
if errorlevel 1 (
  echo [ERR] bridge DB build failed.
  pause
  exit /b 1
)
echo [OK] bridge DB built.
pause
