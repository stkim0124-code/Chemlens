CHEMLENS PDF Example Timeout Retry Patch
=======================================

What changed
------------
1. Gemini auth remains x-goog-api-key header based.
2. Read timeout default increased from 120s to 180s.
3. requests.post now uses timeout=(30, read_timeout).
4. Retry logic now treats transport-level timeout/connection errors as retryable,
   in addition to HTTP 429/500/503.
5. Added smoketest BAT that runs 2-family validation with timeout retry enabled.

Overwrite target
----------------
- Copy pdf_example_automation.py into C:\chemlens\backend\
- Optionally copy run_pdf_example_gemini_smoketest_timeout_retry.bat into C:\chemlens\backend\

Recommended first run
---------------------
run_pdf_example_gemini_smoketest_timeout_retry.bat

Or command directly
-------------------
python pdf_example_automation.py --backend-root . --call-gemini --model gemini-2.5-flash --api-auth header --families "Aldol Reaction;Alkene (Olefin) Metathesis" --limit-pages 2 --dpi 120 --timeout 180 --sleep 60 --max-retries 3 --retry-initial-sleep 20 --retry-backoff 2 --max-regions-per-page 1 --cooldown-on-429 0
python verify_pdf_example_gemini_batch.py --backend-root .
python inspect_pdf_example_automation_errors.py --backend-root .
