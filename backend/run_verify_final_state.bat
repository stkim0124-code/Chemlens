@echo off
setlocal
cd /d "%~dp0"
echo ========================================================================
echo FINAL STATE VERIFICATION
echo ========================================================================
python verify_final_state.py
if errorlevel 1 (
  echo [ERROR] final verification failed.
  exit /b 1
)
