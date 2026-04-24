CHEMLENS PDF example Gemini header-auth + retry patch

What changed
1) pdf_example_automation.py now sends the Gemini API key by default with the official-style x-goog-api-key header instead of putting ?key=... in the URL.
2) Added retry/backoff for transient Gemini HTTP errors (default: 429, 500, 503).
3) Error messages are sanitized again before being written to stage DB / reports.
4) Default smoke-test posture is lighter: gemini-2.5-flash + dpi 120.

Overwrite target
- Copy pdf_example_automation.py to:
  C:\chemlens\backend\pdf_example_automation.py
- Optionally copy the BAT file to:
  C:\chemlens\backend\run_pdf_example_gemini_smoketest_header_retry.bat

Recommended first test
1) conda activate chemlens
2) cd /d C:\chemlens\backend
3) run_pdf_example_gemini_smoketest_header_retry.bat

Manual command equivalent
python pdf_example_automation.py --backend-root . --call-gemini --model gemini-2.5-flash --api-auth header --families "Aldol Reaction" --limit-pages 1 --dpi 120 --sleep 60 --max-retries 2 --retry-initial-sleep 20 --retry-backoff 2 --max-regions-per-page 1 --cooldown-on-429 0

Notes
- If this still fails with 503, the console now shows [RETRY] lines and HTTP response previews.
- If you ever want to compare old behavior, you can force the old key-in-URL style with:
  --api-auth query
