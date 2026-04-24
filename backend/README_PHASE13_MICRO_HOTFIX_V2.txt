Phase13 micro-hotfix v2 patch

What changed:
- Added one extra Glaser Coupling application seed (1-hexyne -> 5-decyne)
- Added one extra Hell-Volhard-Zelinsky application seed (pentanoic acid -> 2-bromopentanoic acid)
- Updated phase13 tag to phase13_shallow_top10_completion_v2 so the extra seeds are tracked cleanly

Why:
- After the first hotfix, phase13 still had two families below rich completion:
  - Glaser Coupling (unique_queryable_pair_count = 2)
  - Hell-Volhard-Zelinsky Reaction (unique_queryable_pair_count = 2)
- This micro-hotfix adds one more distinct parse-safe pair to each family so phase13 can close cleanly.

Recommended rerun:
  python complete_family_evidence_phase13_shallow_top10.py --db app\labint.db --batch a --dry-run
  python complete_family_evidence_phase13_shallow_top10.py --db app\labint.db --batch a
  python verify_family_completion_phase13_shallow_top10.py --db app\labint.db --batch a

  python final_state_verifier.py --db app\labint.db
  python family_completion_dashboard.py --backend-root .
