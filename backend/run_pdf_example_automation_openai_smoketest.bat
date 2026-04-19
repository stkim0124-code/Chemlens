@echo off
setlocal
if "%OPENAI_API_KEY%"=="" echo [ERROR] OPENAI_API_KEY is not set & exit /b 1
python pdf_example_automation.py --backend-root . --call-openai --families "Alkene (Olefin) Metathesis;Aldol Reaction" --limit-pages 2 --sleep 2 --max-retries 6 --retry-initial-sleep 10
