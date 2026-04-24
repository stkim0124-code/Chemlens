# CHEMLENS family completion phase12 shallow top10 (5+5)

## Batch A
- Feist-Bénary Furan Synthesis
- Fischer Indole Synthesis
- Fleming-Tamao Oxidation
- Friedel-Crafts Acylation
- Friedel-Crafts Alkylation

## Batch B
- Finkelstein Reaction
- Gabriel Synthesis
- Favorskii Rearrangement
- Ferrier Reaction
- Evans Aldol Reaction

Run:
python complete_family_evidence_phase12_shallow_top10.py --db app\labint.db --batch a --dry-run
python complete_family_evidence_phase12_shallow_top10.py --db app\labint.db --batch a
python verify_family_completion_phase12_shallow_top10.py --db app\labint.db --batch a
python complete_family_evidence_phase12_shallow_top10.py --db app\labint.db --batch b --dry-run
python complete_family_evidence_phase12_shallow_top10.py --db app\labint.db --batch b
python verify_family_completion_phase12_shallow_top10.py --db app\labint.db --batch b
python final_state_verifier.py --db app\labint.db
python family_completion_dashboard.py --backend-root .
