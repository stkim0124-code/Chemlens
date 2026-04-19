CHUGAEV vs BARTON hotfix

Apply by overwriting app/evidence_search.py in C:\chemlens\backend\app\

What changed:
1) Added a hardcoded Chugaev coarse profile with stronger dehydration requirement and radical/decarboxylation forbids.
2) Added Chugaev-specific delta adjustment so non-elimination reaction queries are penalized.

Why:
- Benchmark regressions showed two Barton-family cases being outranked by Chugaev Elimination due to overly permissive elimination gating.
- top3 remained correct, so this is a ranking hotfix, not a recall/data-coverage change.

After overwrite, run:
conda activate chemlens
cd /d C:\chemlens\backend
python run_named_reaction_benchmark_small.py --benchmark benchmark\named_reaction_benchmark_small.json
