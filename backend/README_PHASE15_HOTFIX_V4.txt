Phase15 hotfix v4

This patch fixes the previous schema/serialization mistakes by restoring the working phase15 script base and applying only the intended hotfixes:
- report path labels use phase15_*
- Kornblum Oxidation gets one extra application seed
- Ley Oxidation gets one extra application seed

Run:
python complete_family_evidence_phase15_shallow_top10.py --db app\labint.db --batch a --dry-run
python complete_family_evidence_phase15_shallow_top10.py --db app\labint.db --batch a
python verify_family_completion_phase15_shallow_top10.py --db app\labint.db --batch a

python complete_family_evidence_phase15_shallow_top10.py --db app\labint.db --batch b --dry-run
python complete_family_evidence_phase15_shallow_top10.py --db app\labint.db --batch b
python verify_family_completion_phase15_shallow_top10.py --db app\labint.db --batch b

python final_state_verifier.py --db app\labint.db
python family_completion_dashboard.py --backend-root .
