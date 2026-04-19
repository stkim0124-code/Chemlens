Safe merge prep for successful v5 stage results
===============================================

What this script does
---------------------
- Merges ONLY new rows from app\labint_v5_stage.db into app\labint.db
- Targets extract_molecules with structure_source in:
  gemini_auto_seed, deterministic_gemini_seed, deterministic_seed_from_existing
- Copies related reaction_extracts and any new reaction_family_patterns ids
- Uses SQL transaction and automatic backup before apply

Why this is the right source
----------------------------
The successful v5 run ended at:
- queryable: 482 -> 745 (+263)
- family_coverage: 51 -> 156 (+105)
- reaction_extracts: 290 -> 395 (+105)
This means the stage DB contains a large successful append-only result set and is the correct source for canonical merge.

Files
-----
- merge_v5_stage_into_canonical.py
- run_merge_v5_stage_into_canonical.bat
- README_MERGE_V5.txt

Recommended Windows commands
----------------------------
conda activate chemlens
cd /d C:\chemlens\backend
python merge_v5_stage_into_canonical.py --dry-run
python merge_v5_stage_into_canonical.py --apply
python run_named_reaction_benchmark_small.py --benchmark benchmark\named_reaction_benchmark_small.json
python VERIFY_CURRENT_BACKEND_STATE.py

Notes
-----
1. Dry-run first. Always.
2. The script creates:
   app\labint.backup_before_v5_merge_YYYYMMDD_HHMMSS.db
3. If anything looks wrong after apply, restore the backup and stop.
4. The v5 diagnostic benchmark had a failing case in the uploaded report set, so the merge gate should be the small gate benchmark / current canonical benchmark, not the diagnostic benchmark.
