CHEMLENS family completion phase7 shallow-top5 patch

Targets:
- Charette Asymmetric Cyclopropanation
- Chugaev Elimination Reaction (Xanthate Ester Pyrolysis)
- Combes Quinoline Synthesis
- Curtius Rearrangement
- Dakin Oxidation

This patch performs:
1. shallow-top5 alias cleanup for phase7 families
2. reaction_extracts reactant/product backfill from extract_molecules where possible
3. curated application-class seed insertion (2 per family)
4. phase7 family verify
5. refreshed final_state_verifier / family_completion_dashboard alias collapse

Run:
  conda activate chemlens
  cd /d C:\chemlens\backend

  python complete_family_evidence_phase7_shallow_top5.py --db app\labint.db --dry-run
  python complete_family_evidence_phase7_shallow_top5.py --db app\labint.db
  python verify_family_completion_phase7_shallow_top5.py --db app\labint.db

  python final_state_verifier.py --db app\labint.db
  python family_completion_dashboard.py --backend-root .

Expected:
- missing_count remains 0
- rich_count increases
- shallow_count decreases
- phase7 target families pass rich completion
