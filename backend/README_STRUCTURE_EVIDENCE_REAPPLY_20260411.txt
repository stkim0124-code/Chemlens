CHEMLENS structure evidence reapply patch (2026-04-11)

Purpose
- Fix NameError in app/evidence_search.py during structure evidence search
- Re-enable evidence fallback merge in app/main.py /search endpoint

Overwrite targets
- backend/app/main.py
- backend/app/evidence_search.py

Validation after overwrite
1) GET /api/search/structure-evidence/stats returns page_images=132, reaction_extracts=275, extract_molecules=488
2) POST /search with smiles=c1ccccc1 returns reaction_card_hits>=1 and structure_evidence_hits>=1
3) Frontend structure search should show both structure cards and named reaction evidence
