CHEMLENS PDF Example Automation Restore Patch (v3 baseline + nested verifier)

Why this patch exists
- The user-provided v3 patch is a previously successful baseline for pdf_example_automation.py.
- Recent Step 2 runs showed many extraction errors, but also a verifier undercount problem: successful JSON stores nested fields inside raw_json -> extracts[*].reactant_smiles/product_smiles.
- Therefore the safe move is:
  1) restore the known-good v3 automation script,
  2) keep the newer nested-JSON-aware verifier,
  3) inspect actual error reasons before changing the Gemini automation logic again.

Files included
- pdf_example_automation.py                        # restored from successful v3 patch
- run_pdf_example_automation_smoketest_env.bat    # uses GEMINI_API_KEY env var
- run_pdf_example_automation_batch1_env.bat       # uses GEMINI_API_KEY env var
- verify_pdf_example_gemini_batch.py              # nested-JSON-aware verifier
- inspect_pdf_example_automation_errors.py        # latest error reason inspector
- README_PDF_EXAMPLE_AUTOMATION_RESTORE_V3.txt

Recommended usage
1) Overwrite these files into C:\chemlens\backend

2) Smoke test with direct python
   conda activate chemlens
   cd /d C:\chemlens\backend
   set GEMINI_API_KEY=YOUR_REAL_KEY
   python pdf_example_automation.py --backend-root . --call-gemini --families "Alkene (Olefin) Metathesis;Aldol Reaction" --limit-pages 2

3) Verify with nested-aware verifier
   python verify_pdf_example_gemini_batch.py --backend-root .

4) Inspect actual extraction errors
   python inspect_pdf_example_automation_errors.py --backend-root .

5) If smoke test is acceptable, run Batch 1
   python pdf_example_automation.py --backend-root . --call-gemini --families "Aldol Reaction;Alkene (Olefin) Metathesis;Diels-Alder Cycloaddition;Swern Oxidation;Ritter Reaction;Pinner Reaction;Schwartz Hydrozirconation;Tsuji-Wilkinson Decarbonylation Reaction;Barton-McCombie Radical Deoxygenation Reaction;Baeyer-Villiger Oxidation/Rearrangement"

Key observation from the successful v3 design
- v3 stores example_target_name / example_summary as top-level columns,
- but reactant_smiles/product_smiles remain nested inside raw_json -> extracts[*],
- so any verifier that only counts flat columns will underreport target/smiles quality.

What to do next
- Do NOT expand beyond Batch 1 until the nested verifier + error inspector are checked.
- If errors are dominated by one repeated Gemini/parse issue, patch pdf_example_automation.py only for that specific failure class.
