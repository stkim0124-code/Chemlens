FINAL STATE VERIFIER V2 PATCH
=============================

What this patch changes
-----------------------
1. final_state_verifier.py now collapses known raw/long-form alias rows to canonical family names
   before computing missing / shallow / rich buckets.
2. Missing-family output is now based on canonical family view, while raw/un-collapsed counts are
   preserved as diagnostics only.
3. family_completion_dashboard.py is updated to understand the v2 verifier output and render both
   canonical and raw diagnostic counts.

Why this patch exists
---------------------
After phase4, the data actually advanced, but the previous verifier still counted missing families
from raw registry names such as:
- Alkene (olefin) Metathesis
- Barton-Mccombie Radical Deoxygenation Reaction
- Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement
- Hofmann-Löffler-Freytag Reaction (Remote Functionalization)
- Houben-Hoesch Reaction/Synthesis

That made the missing count and missing sample look stale even though phase3/phase4 family completion
had already succeeded.

Files included
--------------
- final_state_verifier.py
- family_completion_dashboard.py
- README_FINAL_STATE_VERIFIER_V2.txt

How to run
----------
conda activate chemlens
cd /d C:\chemlens\backend

python final_state_verifier.py --db app\labint.db
python family_completion_dashboard.py --backend-root .

Outputs
-------
reports\final_state_verifier\<timestamp>\final_state_verifier.json
reports\final_state_verifier\<timestamp>\final_state_verifier.md
reports\family_completion_dashboard\<timestamp>\family_completion_dashboard.json
reports\family_completion_dashboard\<timestamp>\family_completion_dashboard.md
reports\family_completion_dashboard\<timestamp>\family_completion_dashboard.html

Expected effect
---------------
- missing family sample should stop re-listing phase3/phase4-completed alias drift names
- missing / shallow / rich counts should better reflect canonical family truth
- dashboard should remain human-readable while exposing raw/un-collapsed counts as diagnostics
