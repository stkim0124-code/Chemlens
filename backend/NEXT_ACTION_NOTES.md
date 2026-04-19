# Next action — confirmed after re-audit

## What was rechecked
- `frontend.zip`: EvidencePanel patch is already present
  - `no_confident_hit`
  - `coarse_gate_notes`
  - warning banner UI
- `backend.zip`: scorer / single-SMILES split / A-lite fallback / benchmark(35 cases) are already present
- `batch59` is **not** a missing named-reaction block.
  - Actual page 502 in the source images is `VIII. APPENDIX`
  - So the earlier “p502~p553 missing reactions” diagnosis was incorrect

## Current confirmed state of the latest backend.zip
- manual_page_knowledge: 1054
- manual_page_entities: 2573
- distinct pages: 809
- page range: 2~810
- reaction_family_patterns (distinct): 291
- abbreviation_aliases: 1038
- reaction_extracts: 275
- extract_molecules total: 1799
- queryable: 368
- tier1: 282
- tier2: 86
- queryable family coverage: 31
- structure_source:
  - NULL: 1575
  - vision_raw_json_promote: 224

## Practical conclusion
The latest uploaded backend is already at **STEP 1 applied**.
The highest-value next move is **STEP 2 local execution** on the user's machine:
- `tier3_pubchem_backfill.py`
- then benchmark re-run

Expected direction after successful STEP 2:
- queryable: ~368 -> ~439
- tier1: ~282 -> ~353
- family coverage: ~31 -> ~36

## Files in this patch
- `VERIFY_CURRENT_BACKEND_STATE.py`
- `RUN_STEP2_AND_BENCHMARK.bat`
- this note
