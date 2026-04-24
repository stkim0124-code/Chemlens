CHEMLENS FAMILY COMPLETION PHASE9 - SHALLOW TOP5

Targets
- Cope Rearrangement
- Corey-Nicolaou Macrolactonization
- Corey-Winter Olefination
- Cornforth Rearrangement
- Dakin-West Reaction

What this patch does
- phase9 shallow-top5 data-table alias cleanup
- backfill reaction_extracts.reactant_smiles / product_smiles from extract_molecules where possible
- add curated application seeds (2 per family)
- update final_state_verifier.py / family_completion_dashboard.py so phase9 aliases and recent completion tags are reflected in canonicalized truth

Run
conda activate chemlens
cd /d C:\chemlens\backend

python complete_family_evidence_phase9_shallow_top5.py --db app\labint.db --dry-run
python complete_family_evidence_phase9_shallow_top5.py --db app\labint.db
python verify_family_completion_phase9_shallow_top5.py --db app\labint.db

python final_state_verifier.py --db app\labint.db
python family_completion_dashboard.py --backend-root .
