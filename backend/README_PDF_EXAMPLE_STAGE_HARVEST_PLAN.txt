CHEMLENS PDF example automation - Step 1 formalization

Purpose
- Step 1 = full stage-only harvest of all application-example pages from named reactions.pdf.
- This does NOT touch canonical reaction_extracts/extract_molecules.
- It only populates/extends app\labint_pdf_examples_stage.db and writes reports/crops.

Files
- run_pdf_example_full_stage_harvest.bat
- verify_pdf_example_stage_harvest.py
- run_verify_pdf_example_stage_harvest.bat

How to run
1) Full stage harvest
   conda activate chemlens
   cd /d C:\chemlens\backend
   run_pdf_example_full_stage_harvest.bat

2) Verify / summarize current stage state
   conda activate chemlens
   cd /d C:\chemlens\backend
   run_verify_pdf_example_stage_harvest.bat

Expected meaning
- Harvest success means page/region/crop pipeline is sealed for all reactions.
- Verification success gives the latest report_run, total families/pages/regions, region histogram,
  and the latest Gemini extraction status metrics already stored in the stage DB.

Important
- This step is safe to rerun. Each run is isolated by report_run timestamp.
- Canonical DB is not modified in this step.
