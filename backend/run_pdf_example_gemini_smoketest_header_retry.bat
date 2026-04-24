@echo off
call conda activate chemlens
cd /d C:\chemlens\backend

echo ========================================================================
echo PDF EXAMPLE GEMINI SMOKETEST ^(HEADER AUTH + RETRY^)
echo ========================================================================
python pdf_example_automation.py --backend-root . --call-gemini --model gemini-2.5-flash --api-auth header --families "Aldol Reaction" --limit-pages 1 --dpi 120 --sleep 60 --max-retries 2 --retry-initial-sleep 20 --retry-backoff 2 --max-regions-per-page 1 --cooldown-on-429 0
python verify_pdf_example_gemini_batch.py --backend-root .
python inspect_pdf_example_automation_errors.py --backend-root .
pause
