ChemLens v5 automation patch
============================

Goal
----
Use the proven v3/v4.5 discovery pattern to expand family coverage automatically,
while keeping rollback SQL-safe and frontier-aware.

What v5 changes
---------------
1. Auto mode selection
   - frontier_discovery: when top candidates are all seed=0 / extracts=0 / gemini lane
   - seed_assisted_bulk: when reusable seeds are present in the top set
   - bulk_discovery: otherwise

2. Frontier control (v4.5 success pattern)
   - starts with 3 candidates per round
   - batch_size=1 in frontier mode
   - if 2 consecutive rounds are full-success, candidate limit grows by +1
   - up to frontier_max_candidate_limit (default 6)

3. SQL-only safety
   - no candidate_backup db copies
   - rollback by DELETE of inserted IDs only
   - bounded snapshots only

4. Gemini schema normalization
   - handles dict / list[dict] / fenced JSON / string JSON
   - family mismatch becomes clean failure, not crash

5. Candidate purity
   - excludes Rules / Guidelines / Principles / classification / glossary / index style pseudo-families

Files in this patch
-------------------
- gemini_family_automation_v5.py
- run_gemini_family_automation_v5.bat
- named_reaction_benchmark_gate.json
- named_reaction_benchmark_v4.json
- PATCH_NOTES_v5.md

Recommended workflow
--------------------
1. Let the currently running automation finish.
2. Copy these files into C:\chemlens\backend\ and overwrite.
3. Run run_gemini_family_automation_v5.bat
4. After completion, inspect:
   - reports/v5/[run_id]/run_summary.json
   - reports/v5/[run_id]/run_items.jsonl
   - benchmark_*.json files in that report directory

Important defaults
------------------
- report dir: reports/v5
- stage db: app/labint_v5_stage.db
- gate benchmark: named_reaction_benchmark_gate.json
- diagnostic benchmark: named_reaction_benchmark_v4.json
- gemini model: gemini-2.5-pro

Quick sanity target
-------------------
Starting from canonical queryable=482 / family_coverage=51,
v5 should behave like this:
- frontier mode: small safe increments, not long zero-insert loops
- bulk mode: wider discovery if non-frontier candidates appear
- gate benchmark must remain top1=1.0 / top3=1.0 / violations=0
