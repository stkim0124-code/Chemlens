CHEMLENS family completion phase5 patch
======================================

Purpose
-------
This patch starts the first shallow-family completion sprint after the canonicalized missing-count reached zero.
It thickens five canonical families into rich/searchable family-evidence layers and updates verifier/dashboard alias collapse so the same short/long family drift does not reappear in the operational view.

Important design choice
-----------------------
For the ene family, the shallow dashboard previously surfaced both:
- Alder Ene Reaction
- Alder (Ene) Reaction (Hydro-Allyl Addition)

This patch treats those short forms as data-table aliases of the canonical official family:
- Alder (Ene) Reaction (Hydro-Allyl Addition)

That is why the fifth canonical family in this sprint is:
- Baeyer-Villiger Oxidation/Rearrangement

instead of promoting two separate ene-family rows that describe the same chemistry.

Canonical families covered in this sprint
-----------------------------------------
1. Alder (Ene) Reaction (Hydro-Allyl Addition)
2. Amadori Reaction / Rearrangement
3. Arbuzov Reaction (Michaelis-Arbuzov Reaction)
4. Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)
5. Baeyer-Villiger Oxidation/Rearrangement

Files in this patch
-------------------
- complete_family_evidence_phase5_shallow_top5.py
- verify_family_completion_phase5_shallow_top5.py
- final_state_verifier.py
- family_completion_dashboard.py
- README_FAMILY_COMPLETION_PHASE5_SHALLOW_TOP5.txt

What the completion script does
-------------------------------
- normalizes data-table family drift for the shallow-top5 sprint
- backfills reaction_extracts reactant/product smiles from existing extract_molecules where possible
- inserts curated application-class seeds for the five canonical families above
- writes JSON/MD summary reports
- supports --dry-run

What the updated verifier/dashboard do
-------------------------------------
- collapse the new short/long shallow-family aliases into canonical family display
- count recently completed phase5 families in the canonicalized overview
- keep raw/un-collapsed drift only in diagnostic sections, not in the main completion truth

Run order
---------
1) Apply the shallow-top5 sprint patch

   conda activate chemlens
   cd /d C:\chemlens\backend

   python complete_family_evidence_phase5_shallow_top5.py --db app\labint.db --dry-run
   python complete_family_evidence_phase5_shallow_top5.py --db app\labint.db
   python verify_family_completion_phase5_shallow_top5.py --db app\labint.db

2) Refresh the canonicalized final state and dashboard

   python final_state_verifier.py --db app\labint.db
   python family_completion_dashboard.py --backend-root .

Expected direction of change
----------------------------
- missing_count should remain 0
- rich_count should increase
- shallow_count should decrease
- the ene short-form duplicate should disappear from the main operational view
- the sprint families should move into recent_completed_families and rich bucket

Notes
-----
- This patch intentionally does NOT mutate reaction_family_patterns registry rows.
- The canonical view is enforced through data-table alias cleanup + verifier/dashboard alias collapse.
