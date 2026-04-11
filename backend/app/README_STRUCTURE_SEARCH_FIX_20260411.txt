CHEMLENS backend structure-search fix patch

What changed
1) Fixed runtime bug in app/evidence_search.py
   - Removed undefined variable usage in _fetch_extract_details()
   - This bug caused structure-evidence search to fail at runtime

2) Strengthened app/main.py /search and /api/search
   - Existing reaction_cards similarity search remains intact
   - If reaction_card hits are insufficient, backend now falls back to structure-evidence bridge results
   - Evidence hits are converted into the same hits[] shape so existing frontend can render them without blocking
   - Added response diagnostics:
     * reaction_card_hits
     * structure_evidence_hits
     * structure_evidence_attempted
     * structure_evidence_added
     * structure_evidence_error

Verified locally
- structure_evidence_stats() returns:
  page_images=132, reaction_extracts=275, extract_molecules=488
- _search_by_structure("c1ccccc1") returns live results
- api_search("c1ccccc1") returns both reaction_card and structure_evidence hits without error

How to apply
- Overwrite backend/app/main.py
- Overwrite backend/app/evidence_search.py

Notes
- This patch does not remove node_modules/.vite/__pycache__/venv because they are not included.
- This patch improves search execution, but deeper quality gains will still come from better structure backfilling into reaction_cards and stronger extract/evidence linking.
