@echo off
call conda activate chemlens
cd /d C:\chemlens\backend

echo ========================================================================
echo PDF EXAMPLE GEMINI SMOKETEST ^(HEADER AUTH + RETRY + TIMEOUT RETRY^)
echo ========================================================================
python pdf_example_automation.py --backend-root . --call-gemini --model gemini-2.5-flash --api-auth header --families "Aldol Reaction;Alkene (Olefin) Metathesis" --limit-pages 2 --dpi 120 --timeout 180 --sleep 60 --max-retries 3 --retry-initial-sleep 20 --retry-backoff 2 --max-regions-per-page 1 --cooldown-on-429 0
python verify_pdf_example_gemini_batch.py --backend-root .
python inspect_pdf_example_automation_errors.py --backend-root .
pause
