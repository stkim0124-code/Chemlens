CHEMLENS continuous remaining4 rebuild loop

Files:
- continuous_gemini_rebuild_remaining4.py
- run_continuous_gemini_rebuild_remaining4.bat

Purpose:
- Target only the 4 unresolved Buchner-cluster families:
  - Claisen Condensation / Claisen Reaction
  - Horner-Wadsworth-Emmons Olefination
  - Krapcho Dealkoxycarbonylation
  - Regitz Diazo Transfer
- For each round, generate up to 3 Gemini candidates per family
- Screen each candidate against the small benchmark on a temp DB
- Freeze only PASS candidates
- Apply only frozen PASS candidates to canonical
- Repeat until all 4 are resolved or max_empty_rounds is reached

Notes:
- This script never mutates canonical during screening.
- APPLY mode inserts only after a frozen candidate passes benchmark again on a temp copy.
- Newly inserted rows use structure_source='gemini_rebuild_seed'.
