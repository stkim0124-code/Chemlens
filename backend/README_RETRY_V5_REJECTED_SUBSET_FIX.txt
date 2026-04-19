REJECTED 13 FAMILY SUBSET-ONLY RETRY PATCH
==========================================

Purpose
-------
Retry ONLY the families that were rejected by v5 selective merge.
This patch fixes two issues seen in prior retry scripts:

1) rejected-family loader now works
2) the main loop now processes ONLY the rejected family subset, not all 105 families

Files
-----
- retry_v5_rejected_families.py
- run_retry_v5_rejected_families.bat

How it works
------------
- Reads the latest selective_merge_summary.json (or the path you pass)
- Loads only rejected families
- Looks up those families in labint_v5_stage.db
- Tries narrower evidence subsets in this order:
  1. minimal_pair
  2. queryable_pair
  3. core_pair_set
  4. core_plus_intermediate
  5. core_plus_reagent
  6. queryable_only
  7. all_original
- After each variant it runs the benchmark on canonical
- If benchmark regresses, that variant is rolled back immediately
- If benchmark stays at baseline, that variant is accepted

Default paths
-------------
canonical db:   app\labint.db
stage db:       app\labint_v5_stage.db
benchmark file: benchmark\named_reaction_benchmark_small.json
summary root:   reports\v5_selective_merge
report root:    reports\v5_rejected_retry

Recommended usage
-----------------
1) Backup your current canonical db if you want an extra manual safety copy
2) Put the two files in C:\chemlens\backend\
3) Run:
   run_retry_v5_rejected_families.bat

Manual usage
------------
Dry-run:
python retry_v5_rejected_families.py --dry-run

Apply:
python retry_v5_rejected_families.py --apply

Expected dry-run behavior
-------------------------
- total rejected candidates: 13
- lists the 13 rejected family names
- shows "would try" lines for those 13 only

Expected apply behavior
-----------------------
- [RETRY] lines start with rejected family names only
- no Cope / Wittig / other previously accepted families should appear
- summary is saved to:
  reports\v5_rejected_retry\<timestamp>\rejected_retry_summary.json


Compatibility note:
- Fixed Python 3.11/3.10 incompatible f-string on Windows by avoiding same-quote nesting in line that prints mode.
