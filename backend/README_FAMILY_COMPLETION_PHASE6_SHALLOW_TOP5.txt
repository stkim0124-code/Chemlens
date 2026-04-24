CHEMLENS family completion phase6 shallow-top5 patch

Targets
- Balz-Schiemann Reaction (Schiemann Reaction)
- Buchner Method of Ring Expansion (Buchner Reaction)
- Carroll Rearrangement (Kimel-Cope Rearrangement)
- Chichibabin Amination Reaction (Chichibabin Reaction)
- Claisen Condensation / Claisen Reaction

This patch performs:
- data-table alias cleanup for short/long drift
- backfill of reaction_extracts reactant/product smiles from extract_molecules where possible
- insertion of 2 curated application-example seeds per family
- phase6 verify report generation
- updated final_state_verifier / family_completion_dashboard alias collapse

Run
1) python complete_family_evidence_phase6_shallow_top5.py --db app\labint.db --dry-run
2) python complete_family_evidence_phase6_shallow_top5.py --db app\labint.db
3) python verify_family_completion_phase6_shallow_top5.py --db app\labint.db
4) python final_state_verifier.py --db app\labint.db
5) python family_completion_dashboard.py --backend-root .
