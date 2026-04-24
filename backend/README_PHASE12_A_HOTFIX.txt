Phase12 A hotfix

Fixes one invalid Fischer Indole Synthesis product SMILES that caused batch A to abort before apply.

Corrected seed:
- tetrahydrocarbazole
- from: c1ccc2[nH]c3CCCCCc3cc2c1
- to:   c1ccc2c3c([nH]c2c1)CCCCC3

Recommended rerun:
python complete_family_evidence_phase12_shallow_top10.py --db app\labint.db --batch a --dry-run
python complete_family_evidence_phase12_shallow_top10.py --db app\labint.db --batch a
python verify_family_completion_phase12_shallow_top10.py --db app\labint.db --batch a
python final_state_verifier.py --db app\labint.db
python family_completion_dashboard.py --backend-root .
