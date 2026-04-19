RETRY V5 REJECTED FAMILIES
==========================

What this patch does
--------------------
- Reads the latest selective merge summary (or a supplied one)
- Finds rejected families from that run
- Retries each rejected family with narrower molecule subsets
- Runs the benchmark after each retry variant
- Accepts the first variant that preserves benchmark quality
- Leaves still-failing families in stage only

Default retry variants per rejected family
------------------------------------------
1. minimal_pair                : 1 reactant + 1 product
2. core_pair_set               : up to 1 reactant + 1 product per role bucket
3. core_plus_intermediate      : core pair + 1 intermediate
4. core_plus_reagent           : core pair + 1 reagent/catalyst
5. queryable_only              : only queryable molecules from stage
6. all_original                : original family payload as last resort

Files
-----
- retry_v5_rejected_families.py
- run_retry_v5_rejected_families.bat

Recommended usage
-----------------
1. conda activate chemlens
2. cd /d C:\chemlens\backend
3. python retry_v5_rejected_families.py --dry-run
4. python retry_v5_rejected_families.py --apply
5. python run_named_reaction_benchmark_small.py --benchmark benchmark\named_reaction_benchmark_small.json
6. python VERIFY_CURRENT_BACKEND_STATE.py

Or use:
- run_retry_v5_rejected_families.bat

Notes
-----
- This patch does NOT re-run Gemini. It re-tries rejected families using narrower evidence subsets already present in labint_v5_stage.db.
- Still-rejected families remain in stage and can later be sent to a Gemini re-generation patch.
- A backup of canonical is created automatically before apply.
