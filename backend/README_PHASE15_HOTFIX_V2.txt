Phase15 micro-hotfix v2

What this patch does
- fixes phase15 report directory naming so outputs now go under reports/family_completion_phase15_shallow_top10 and ..._verify
- adds one extra application seed to Kornblum Oxidation
- adds one extra application seed to Ley Oxidation

Why
- reports (17).zip shows the phase15 scripts wrote to phase14_* output folders, but the internal tag already says phase15_shallow_top10_completion_v1. This is only a report-path labeling bug.
- the same reports show Kornblum Oxidation and Ley Oxidation both stopped at unique_queryable_pair_count = 2, so they passed minimum completion but not rich completion.

Run
python complete_family_evidence_phase15_shallow_top10.py --db app\labint.db --batch a --dry-run
python complete_family_evidence_phase15_shallow_top10.py --db app\labint.db --batch a
python verify_family_completion_phase15_shallow_top10.py --db app\labint.db --batch a

python complete_family_evidence_phase15_shallow_top10.py --db app\labint.db --batch b --dry-run
python complete_family_evidence_phase15_shallow_top10.py --db app\labint.db --batch b
python verify_family_completion_phase15_shallow_top10.py --db app\labint.db --batch b

Then rerun
python final_state_verifier.py --db app\labint.db
python family_completion_dashboard.py --backend-root .
