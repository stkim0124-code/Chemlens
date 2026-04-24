# FAMILY COMPLETION PHASE10 SHALLOW TOP5

Phase10 shallow-family completion sprint patch.

Canonical targets:
- Criegee Oxidation
- Danheiser Benzannulation
- Danheiser Cyclopentene Annulation
- Darzens Glycidic Ester Condensation
- Davis' Oxaziridine Oxidations

What this patch does:
- cleans up known data-table alias drift for the phase10 families
- backfills reaction_extracts reactant_smiles / product_smiles from extract_molecules where possible
- inserts two curated application-example seeds for each target family
- updates final_state_verifier.py and family_completion_dashboard.py so phase10 recent-completion tags and alias collapse are reflected in the canonical dashboard view

Run:

conda activate chemlens
cd /d C:\chemlens\backend

python complete_family_evidence_phase10_shallow_top5.py --db app\labint.db --dry-run
python complete_family_evidence_phase10_shallow_top5.py --db app\labint.db
python verify_family_completion_phase10_shallow_top5.py --db app\labint.db

python final_state_verifier.py --db app\labint.db
python family_completion_dashboard.py --backend-root .
