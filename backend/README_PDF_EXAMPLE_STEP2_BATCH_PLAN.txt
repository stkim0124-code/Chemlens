PDF EXAMPLE STEP 2 - GEMINI BATCH EXPANSION PATCH (ENV-VAR MODE)
===================================================================

This patch is the Step 2 follow-up after Step 1 stage-only harvest verification.
It does NOT touch canonical DB promotion rules. It is for:

1) running a very small Gemini smoke test,
2) running Batch 1 (10 families),
3) verifying extraction quality from the stage DB.

This revision is changed so you can run it in the style:

  set GEMINI_API_KEY=your_real_key
  python ...

No --prompt-api-key interactive input is used in this version.

Files included
--------------
- run_pdf_example_gemini_smoketest_step2.bat
- run_pdf_example_gemini_batch1_step2.bat
- verify_pdf_example_gemini_batch.py
- run_verify_pdf_example_gemini_batch.bat

Recommended direct Python execution
-----------------------------------
1. Set Gemini API key in the current prompt session
   set GEMINI_API_KEY=your_real_key

2. Smoke test first
   python pdf_example_automation.py --backend-root . --call-gemini --families "Alkene (Olefin) Metathesis;Aldol Reaction" --limit-pages 2

3. Verify smoke test result
   python verify_pdf_example_gemini_batch.py --backend-root .

4. If the result looks acceptable, run Batch 1
   python pdf_example_automation.py --backend-root . --call-gemini --families "Aldol Reaction;Alkene (Olefin) Metathesis;Diels-Alder Cycloaddition;Swern Oxidation;Ritter Reaction;Pinner Reaction;Schwartz Hydrozirconation;Tsuji-Wilkinson Decarbonylation Reaction;Barton-McCombie Radical Deoxygenation Reaction;Baeyer-Villiger Oxidation/Rearrangement"

5. Verify Batch 1 result
   python verify_pdf_example_gemini_batch.py --backend-root .

BAT files
---------
The BAT files are still included, but they now also use GEMINI_API_KEY from the environment.
They no longer prompt for the key.

What the verifier reports
-------------------------
- stage_db path
- detected extraction table name
- latest extraction run id (if available)
- total extraction rows
- rows in the latest extraction run
- status_counts
- target name extraction rate
- reactant/product/both/either smiles non-null counts
- parse-safe counts and ratios (if RDKit is available and smiles columns exist)
- family mismatch count and ratio (only if both source/requested family and extracted family columns exist)
- text-only extraction count and ratio
- per-family row counts for latest extraction run (if family column exists)

Suggested interpretation
------------------------
For safe expansion, use these as practical guide rails:
- status=ok ratio >= 80%
- target name extraction >= 85%
- either reactant or product smiles >= 60%
- both parse-safe reactant/product smiles >= 40% to 50%
- family mismatch ratio should stay low

Notes
-----
- This patch assumes your backend already has the upgraded pdf_example_automation.py with:
  - PDF auto-discovery working,
  - GEMINI_API_KEY environment-variable use supported,
  - stage DB writing enabled.
- The verifier is schema-tolerant: it inspects the SQLite schema dynamically and uses columns/tables that exist.
- Output goes to:
  reports\pdf_example_batch_verification\<timestamp>
