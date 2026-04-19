@echo off
setlocal
cd /d %~dp0

echo ========================================================================
echo VERIFY PDF EXAMPLE STAGE HARVEST
echo ========================================================================
python verify_pdf_example_stage_harvest.py --backend-root .
if errorlevel 1 (
  echo [ERROR] verification failed.
  exit /b 1
)
