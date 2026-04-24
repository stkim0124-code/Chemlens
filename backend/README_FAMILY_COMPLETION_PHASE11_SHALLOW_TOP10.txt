PHASE11 SHALLOW-TOP10 (5+5) PATCH
=================================

This patch moves from 5-family phases to a 10-family phase operated internally as 5+5.

BATCH A families
----------------
1. Danishefsky's Diene Cycloaddition
2. De Mayo Cycloaddition
3. Demjanov Rearrangement and Tiffeneau-Demjanov Rearrangement
4. Dess-Martin Oxidation
5. Dieckmann Condensation

BATCH B families
----------------
6. Diels-Alder Cycloaddition
7. Dienone-Phenol Rearrangement
8. Enyne Metathesis
9. Eschenmoser-Claisen Rearrangement
10. Eschweiler-Clarke Methylation

Recommended execution order
---------------------------
1) Batch A dry-run/apply/verify
2) Batch B dry-run/apply/verify
3) Final verifier + dashboard refresh once at the end

Commands
--------
conda activate chemlens
cd /d C:\chemlens\backend

python complete_family_evidence_phase11_shallow_top10.py --db app\labint.db --batch a --dry-run
python complete_family_evidence_phase11_shallow_top10.py --db app\labint.db --batch a
python verify_family_completion_phase11_shallow_top10.py --db app\labint.db --batch a

python complete_family_evidence_phase11_shallow_top10.py --db app\labint.db --batch b --dry-run
python complete_family_evidence_phase11_shallow_top10.py --db app\labint.db --batch b
python verify_family_completion_phase11_shallow_top10.py --db app\labint.db --batch b

python final_state_verifier.py --db app\labint.db
python family_completion_dashboard.py --backend-root .

Optional: verify all phase11 families together
----------------------------------------------
python verify_family_completion_phase11_shallow_top10.py --db app\labint.db --batch all
