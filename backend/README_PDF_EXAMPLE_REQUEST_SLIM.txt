PDF EXAMPLE AUTOMATION - REQUEST SLIM PATCH

What changed
- Shrinks the text sent to Gemini dramatically.
- Sends only a short paragraph hint and a very short OCR hint instead of a huge nearby_text blob.
- Lowers crop DPI from 220 to 160 by default.
- In the current minimal run pattern, this reduces tokens/bytes per request and is the most direct way to test whether the request itself was too heavy.

Recommended test
conda activate chemlens
cd /d C:\chemlens\backend
set GEMINI_API_KEY=YOUR_NEW_KEY
python pdf_example_automation.py --backend-root . --call-gemini --families "Aldol Reaction" --limit-pages 1
python verify_pdf_example_gemini_batch.py --backend-root .
python inspect_pdf_example_automation_errors.py --backend-root .
