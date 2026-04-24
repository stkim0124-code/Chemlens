Phase13 hotfix patch

What changed:
- Fixed invalid Glaser Coupling seed SMILES for 4-ethynyltoluene:
  - old: Cc1ccc(C#CH)cc1
  - new: C#Cc1ccc(C)cc1 (canonical form of 4-ethynyltoluene)
- Added one extra Hofmann Rearrangement application seed so batch B reaches rich completion
  with unique_queryable_pair_count >= 3.

Recommended rerun:
  python complete_family_evidence_phase13_shallow_top10.py --db app\labint.db --batch a --dry-run
  python complete_family_evidence_phase13_shallow_top10.py --db app\labint.db --batch a
  python verify_family_completion_phase13_shallow_top10.py --db app\labint.db --batch a

  python complete_family_evidence_phase13_shallow_top10.py --db app\labint.db --batch b --dry-run
  python complete_family_evidence_phase13_shallow_top10.py --db app\labint.db --batch b
  python verify_family_completion_phase13_shallow_top10.py --db app\labint.db --batch b

  python final_state_verifier.py --db app\labint.db
  python family_completion_dashboard.py --backend-root .
