CHEMLENS PDF example resume-safe patch

What this patch does
1) Keeps the current request_slim_fix extraction logic.
2) Redacts API keys from future error messages before they are printed or stored.
3) Adds a cleanup utility to scrub already-written keys from the stage DB and report files.
4) Adds conservative .bat runners for Gemini smoke test and Batch 1.

Recommended restart order
1) Rotate the previously exposed Gemini API key first.
2) Overwrite these files into C:\chemlens\backend\
   - pdf_example_automation.py
   - verify_pdf_example_gemini_batch.py
   - inspect_pdf_example_automation_errors.py
   - redact_pdf_example_api_keys.py
   - run_pdf_example_gemini_smoketest_ultrasafe.bat
   - run_pdf_example_gemini_batch1_ultrasafe.bat
3) In Anaconda Prompt, run:
   run_pdf_example_gemini_smoketest_ultrasafe.bat
4) Only if the smoke test is stable enough, run:
   run_pdf_example_gemini_batch1_ultrasafe.bat

Notes
- named reactions.pdf is expected at C:\chemlens\backend\app\data\pdfs\named reactions.pdf
- This patch does not change the feeder-lane principle. It only helps resume Gemini extraction more safely.
- The current script already uses lean requests, max-regions-per-page=1, and immediate stop on 429.
