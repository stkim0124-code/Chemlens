V5 rejected-family loader fix patch

What this patch fixes
- The previous retry script loaded 0 rejected families because it assumed one summary schema.
- This version recursively scans the selective_merge_summary.json and collects rejected family names from:
  - rejected lists of strings
  - rejected lists of dicts
  - nested objects with status/result/decision = rejected
  - rejected_* containers anywhere in the JSON tree

Files
- retry_v5_rejected_families.py
- run_retry_v5_rejected_families.bat

Recommended usage
1) Put both files into C:\chemlens\backend\
2) Run:
   conda activate chemlens
   cd /d C:\chemlens\backend
   run_retry_v5_rejected_families.bat

Notes
- This patch only fixes the rejected-family loader.
- It keeps the retry behavior:
  minimal_pair -> core_pair_set -> core_plus_intermediate -> core_plus_reagent -> queryable_only -> all_original
- Benchmark is run explicitly against app\labint.db after each retry variant.
- Regression causes immediate rollback of that variant only.
