CHEMLENS Track B patch (overwrite into existing backend folder)

Included file:
- app/evidence_search.py

What changed:
- strengthened family prior / forbidden mismatch scoring
- added query-side feature: urea_like
- stronger penalties for mismatched families on reaction queries
- especially suppresses Amadori / Baeyer-Villiger noise under Buchner-like queries

How to apply:
1) Open your existing backend folder.
2) Extract this ZIP directly into that backend folder.
3) Allow overwrite for app/evidence_search.py.
4) Restart backend.
