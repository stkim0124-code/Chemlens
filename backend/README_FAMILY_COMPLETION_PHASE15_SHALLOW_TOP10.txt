PHASE15 SHALLOW TOP10 (5+5) PATCH
=================================

This patch continues the official corpus digestion process using a 10-family phase split into A/B 5-family batches.

Phase15-A families:
- Knoevenagel Condensation
- Knorr Pyrrole Synthesis
- Kornblum Oxidation
- Krapcho Dealkoxycarbonylation (Krapcho Reaction)
- Kumada Cross-Coupling

Phase15-B families:
- Ley Oxidation
- Lieben Haloform Reaction
- Lossen Rearrangement
- Luche Reduction
- Mannich Reaction

Files:
- complete_family_evidence_phase15_shallow_top10.py
- verify_family_completion_phase15_shallow_top10.py
- final_state_verifier.py
- family_completion_dashboard.py

Recommended run order:
  python complete_family_evidence_phase15_shallow_top10.py --db app\labint.db --batch a --dry-run
  python complete_family_evidence_phase15_shallow_top10.py --db app\labint.db --batch a
  python verify_family_completion_phase15_shallow_top10.py --db app\labint.db --batch a

  python complete_family_evidence_phase15_shallow_top10.py --db app\labint.db --batch b --dry-run
  python complete_family_evidence_phase15_shallow_top10.py --db app\labint.db --batch b
  python verify_family_completion_phase15_shallow_top10.py --db app\labint.db --batch b

  python final_state_verifier.py --db app\labint.db
  python family_completion_dashboard.py --backend-root .
