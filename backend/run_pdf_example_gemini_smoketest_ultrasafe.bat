@echo off
conda activate chemlens
cd /d C:\chemlens\backend
python redact_pdf_example_api_keys.py --backend-root .
python pdf_example_automation.py --backend-root . --call-gemini --families "Alkene (Olefin) Metathesis;Aldol Reaction" --limit-pages 2 --sleep 45 --max-regions-per-page 1 --cooldown-on-429 0
python verify_pdf_example_gemini_batch.py --backend-root .
python inspect_pdf_example_automation_errors.py --backend-root .
pause
