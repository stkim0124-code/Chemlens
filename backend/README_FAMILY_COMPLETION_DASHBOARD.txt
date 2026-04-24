FAMILY COMPLETION DASHBOARD PATCH
=================================

Purpose
-------
This patch builds a display-oriented dashboard from final_state_verifier JSON.
It is intentionally a presentation/triage layer, not a replacement for the
canonical DB truth source.

What it fixes compared with a naive dashboard
---------------------------------------------
1) Canonical alias collapse for display lists
   - Example: Alkene (olefin) Metathesis -> Alkene (Olefin) Metathesis
   - Example: Barton-Mccombie Radical Deoxygenation Reaction -> Barton-McCombie Radical Deoxygenation Reaction
   - Also collapses the known long-form drift cases called out in prior handover.

2) Separates display surfaces
   - completion summary
   - recent completed focus families
   - missing sample (canonicalized)
   - top shallow families (canonicalized)
   - alias cleanup candidates
   - duplicate family pattern diagnostics

3) Produces a viewable HTML file
   - easier to inspect than raw JSON/MD

Files
-----
- family_completion_dashboard.py
- README_FAMILY_COMPLETION_DASHBOARD.txt

Run
---
conda activate chemlens
cd /d C:\chemlens\backend

python family_completion_dashboard.py --backend-root .

Optional: point to a specific verifier JSON
python family_completion_dashboard.py --backend-root . --verifier-json reports\final_state_verifier\20260419_231253\final_state_verifier.json

Output
------
reports\family_completion_dashboard\<timestamp>\family_completion_dashboard.json
reports\family_completion_dashboard\<timestamp>\family_completion_dashboard.md
reports\family_completion_dashboard\<timestamp>\family_completion_dashboard.html

Notes
-----
- The dashboard preserves raw verifier totals in the summary cards.
- Only the display lists are canonicalized for alias/case drift.
- If you later improve final_state_verifier itself, the dashboard can continue to consume the newest JSON without changing the canonical DB.
