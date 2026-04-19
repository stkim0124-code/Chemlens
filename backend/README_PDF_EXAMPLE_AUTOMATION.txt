CHEMLENS Patch: PDF Synthetic-Example Automation Prototype (v3)

What changed in v3
- Gemini mode no longer requires a pre-saved API key in .env or a hardcoded placeholder.
- You can now enter the Gemini API key directly in the current Anaconda Prompt session.
- If --call-gemini is used and no valid env key is found, the script will prompt interactively by default when possible.
- The convenience GEMINI .bat now launches with --prompt-api-key as well.

Recommended usage (directly in Anaconda Prompt)
1) Dry-run harvest only
conda activate chemlens
cd /d C:\chemlens\backend
python pdf_example_automation.py --backend-root .

2) Gemini test with interactive API-key prompt
conda activate chemlens
cd /d C:\chemlens\backend
python pdf_example_automation.py --backend-root . --call-gemini --prompt-api-key --families "Alkene (Olefin) Metathesis;Aldol Reaction" --limit-pages 2

Alternative: set the key only for the current Prompt session
set GEMINI_API_KEY=YOUR_REAL_KEY_HERE
python pdf_example_automation.py --backend-root . --call-gemini --families "Alkene (Olefin) Metathesis;Aldol Reaction" --limit-pages 2

Behavior
- Placeholder keys like 여기에_본인_API키 / YOUR_API_KEY are treated as invalid.
- If a valid env key is absent and the terminal is interactive, the script asks: Enter Gemini API key (input hidden):
- The key is used only for the current process/session unless you manually save it elsewhere.

Files
- pdf_example_automation.py
- run_pdf_example_automation_dryrun.bat
- run_pdf_example_automation_gemini.bat
- README_PDF_EXAMPLE_AUTOMATION.txt

Output
- reports\pdf_example_automation\<timestamp>\
- app\labint_pdf_examples_stage.db


RATE-LIMIT HOTFIX
----------------
If Gemini returns HTTP 429 / Too Many Requests, this version retries with exponential backoff and redacts the API key from stored error messages.

Recommended smoke test:
  set GEMINI_API_KEY=YOUR_KEY
  python pdf_example_automation.py --backend-root . --call-gemini --families "Alkene (Olefin) Metathesis;Aldol Reaction" --limit-pages 2 --sleep 3 --max-retries 8 --retry-initial-sleep 15

Recommended Batch 1:
  set GEMINI_API_KEY=YOUR_KEY
  python pdf_example_automation.py --backend-root . --call-gemini --families "Aldol Reaction;Alkene (Olefin) Metathesis;Diels-Alder Cycloaddition;Swern Oxidation;Ritter Reaction;Pinner Reaction;Schwartz Hydrozirconation;Tsuji-Wilkinson Decarbonylation Reaction;Barton-McCombie Radical Deoxygenation Reaction;Baeyer-Villiger Oxidation/Rearrangement" --sleep 5 --max-retries 8 --retry-initial-sleep 15
