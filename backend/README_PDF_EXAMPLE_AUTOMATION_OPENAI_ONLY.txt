OPENAI-ONLY PDF EXAMPLE AUTOMATION PATCH

Why this patch:
- Do NOT add provider switch logic.
- Keep the stage DB schema and verification scripts exactly the same.
- Replace only the extractor call path with OpenAI API for A/B testing.

What is included:
- pdf_example_automation.py  (OpenAI-only version)
- verify_pdf_example_gemini_batch.py
- inspect_pdf_example_automation_errors.py
- run_pdf_example_automation_openai_smoketest.bat
- run_pdf_example_automation_openai_batch1.bat

Required env var:
- OPENAI_API_KEY
Optional env var:
- OPENAI_MODEL (default: gpt-4.1-mini)

Smoke test:
  set OPENAI_API_KEY=your_key
  python pdf_example_automation.py --backend-root . --call-openai --families "Alkene (Olefin) Metathesis;Aldol Reaction" --limit-pages 2 --sleep 2 --max-retries 6 --retry-initial-sleep 10
  python verify_pdf_example_gemini_batch.py --backend-root .
  python inspect_pdf_example_automation_errors.py --backend-root .

Batch 1:
  set OPENAI_API_KEY=your_key
  python pdf_example_automation.py --backend-root . --call-openai --families "Aldol Reaction;Alkene (Olefin) Metathesis;Diels-Alder Cycloaddition;Swern Oxidation;Ritter Reaction;Pinner Reaction;Schwartz Hydrozirconation;Tsuji-Wilkinson Decarbonylation Reaction;Barton-McCombie Radical Deoxygenation Reaction;Baeyer-Villiger Oxidation/Rearrangement" --sleep 2 --max-retries 6 --retry-initial-sleep 10
  python verify_pdf_example_gemini_batch.py --backend-root .
  python inspect_pdf_example_automation_errors.py --backend-root .
