
CHEMLENS family completion phase13 patch

This patch continues the official shallow-family completion process in 10-family phases,
operated internally as 5+5 batches.

Phase13-A:
- Glaser Coupling
- Grignard Reaction
- Heck Reaction
- Hell-Volhard-Zelinsky Reaction
- Henry Reaction

Phase13-B:
- Hantzsch Dihydropyridine Synthesis
- Hetero Diels-Alder Cycloaddition (HDA)
- Hofmann Elimination
- Hofmann Rearrangement
- Horner-Wadsworth-Emmons Olefination

Run:
  python complete_family_evidence_phase13_shallow_top10.py --db app\labint.db --batch a --dry-run
  python complete_family_evidence_phase13_shallow_top10.py --db app\labint.db --batch a
  python verify_family_completion_phase13_shallow_top10.py --db app\labint.db --batch a

  python complete_family_evidence_phase13_shallow_top10.py --db app\labint.db --batch b --dry-run
  python complete_family_evidence_phase13_shallow_top10.py --db app\labint.db --batch b
  python verify_family_completion_phase13_shallow_top10.py --db app\labint.db --batch b

  python final_state_verifier.py --db app\labint.db
  python family_completion_dashboard.py --backend-root .
