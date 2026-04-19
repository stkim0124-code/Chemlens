@echo off
setlocal
echo ========================================================================
echo PDF EXAMPLE AUTOMATION - GEMINI SMOKETEST RATE SAFE
echo ========================================================================
python pdf_example_automation.py --backend-root . --call-gemini --families "Alkene (Olefin) Metathesis;Aldol Reaction" --limit-pages 2 --sleep 3 --max-retries 8 --retry-initial-sleep 15
