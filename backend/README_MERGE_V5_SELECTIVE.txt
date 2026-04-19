CHEMLENS v5 stage selective merge

Purpose
- Safely merge only benchmark-safe families from app\labint_v5_stage.db into app\labint.db.
- Unlike wholesale merge, this script merges family-by-family and runs the benchmark after each accepted family.
- If a family causes regression, it is immediately removed and recorded as rejected.

Files
- merge_v5_stage_selective.py
- run_merge_v5_stage_selective.bat

Default behavior
- source DB:      app\labint_v5_stage.db
- canonical DB:   app\labint.db
- benchmark:      benchmark\named_reaction_benchmark_small.json
- run order hint: reports\v5\20260418_142426\run_items.jsonl
- only new families are considered by default
- pseudo families (Rules / Guidelines / Principles / classification / glossary / index) are skipped

Recommended workflow
1) Dry-run only:
   python merge_v5_stage_selective.py --dry-run

2) Apply:
   python merge_v5_stage_selective.py --apply

3) Re-check benchmark manually:
   python run_named_reaction_benchmark_small.py --benchmark benchmark\named_reaction_benchmark_small.json
   python VERIFY_CURRENT_BACKEND_STATE.py

Useful options
- --family-limit 20
  only test the first 20 candidate families.

- --start-index 10
  skip the first 10 candidate families.

- --include-existing-families
  also consider families already queryable in canonical.
  Default is OFF for safety.

- --allow-top1-drop 0.0
- --allow-top3-drop 0.0
- --allow-extra-violations 0
  benchmark guard thresholds.

Important notes
- The script creates a canonical backup automatically before apply.
- Benchmark is always executed with explicit --db <canonical_path>.
- Accepted / rejected family lists are saved in reports/v5_selective_merge/<timestamp>/.
