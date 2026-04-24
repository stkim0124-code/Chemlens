@echo off
setlocal
call conda activate chemlens
cd /d C:\chemlens\backend

echo ================================================================
echo PDF EXAMPLE GEMINI SMOKETEST ^(PASTE-KEY MODE^)
echo ================================================================
set /p GEMINI_API_KEY=Paste Gemini API key and press Enter: 
if "%GEMINI_API_KEY%"=="" (
  echo [ERROR] Empty key. Aborting.
  exit /b 1
)

python redact_pdf_example_api_keys.py --backend-root .
python pdf_example_automation.py --backend-root . --call-gemini --model gemini-2.5-flash --families "Alkene (Olefin) Metathesis;Aldol Reaction" --limit-pages 2 --sleep 45 --max-regions-per-page 1 --cooldown-on-429 0
python verify_pdf_example_gemini_batch.py --backend-root .
python inspect_pdf_example_automation_errors.py --backend-root .

endlocal
