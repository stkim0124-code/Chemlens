@echo off
setlocal

echo ========================================================================
echo VERIFY PDF EXAMPLE GEMINI BATCH
echo ========================================================================
python verify_pdf_example_gemini_batch.py --backend-root .
if errorlevel 1 (
  echo [ERROR] verification failed.
  exit /b 1
)
echo ========================================================================
echo [DONE] verification finished.
echo ========================================================================
endlocal
