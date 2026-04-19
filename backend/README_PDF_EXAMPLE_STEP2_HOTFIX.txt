PDF EXAMPLE STEP 2 HOTFIX
=========================

Why this hotfix exists
----------------------
Your Step 2 smoke test and Batch 1 logs showed:
- page/region harvest is normal,
- but extraction is mostly status=error,
- and the old verifier reported target/smiles counts as all zero.

From the uploaded report ZIP, at least one successful JSON already contains:
- example_target_name
- extracts[*].reactant_smiles
- extracts[*].product_smiles

So two things are happening at once:
1) real extraction errors are present,
2) the old verifier is too flat-column oriented and under-counts nested JSON success payloads.

Files included
--------------
- verify_pdf_example_gemini_batch.py   (nested-JSON aware replacement)
- inspect_pdf_example_automation_errors.py
- README_PDF_EXAMPLE_STEP2_HOTFIX.txt

How to run
----------
conda activate chemlens
cd /d C:\chemlens\backend

1) Re-run verification with the new verifier
python verify_pdf_example_gemini_batch.py --backend-root .

2) Inspect the latest extraction error reasons
python inspect_pdf_example_automation_errors.py --backend-root .

How to interpret
----------------
If the new verifier now recovers some target/smiles counts from nested JSON,
then the previous zero counts were partly a verifier/schema-mismatch problem.

If top_error_messages show a dominant repeated failure mode,
then the next patch target is not batch expansion, but pdf_example_automation.py
prompt/response-parse handling for that specific error class.

Recommended next step
---------------------
Do NOT expand beyond Batch 1 yet.
First run the two commands above and share:
- the new verification console output,
- the error inspection console output,
- or the generated summary JSON/MD files.
