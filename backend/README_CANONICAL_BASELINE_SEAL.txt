CHEMLENS canonical search baseline seal
Date: 2026-04-18

This bundle freezes the CURRENT WORKING BASELINE that satisfies:
- batch59 restore complete for p502-p553
- benchmark small v3 restored to 27/27 top1 and 27/27 top3
- queryable structure pool = 426
- tier1 exact structures = 340
- queryable family coverage = 36

What is included
- app/labint.db  <-- canonical DB
- app/evidence_search.py
- benchmark results (CSV / JSON / MD)
- VERIFY_CURRENT_BACKEND_STATE.py
- CANONICAL_BASELINE_SEAL_REPORT.json

Important truthfulness note
This baseline is GOOD ENOUGH to seal for continued work, but it does NOT prove that all data are perfectly and fully practical.
Known limitations:
- only 36 / 305 families currently have queryable structure evidence
- reaction_cards direct SMILES coverage is still sparse
- the benchmark is still small, not exhaustive
- app/labint_round9_bridge_work.db should NOT be treated as canonical

Rule going forward
- Use app/labint.db as the only source of truth
- Do not treat bridge_work DB as authoritative
- Apply future dataization and evaluation against this sealed baseline first
