PHASE4 MISSING-TRIO COMPLETION PATCH
===================================

Goal
----
Promote the three remaining canonicalized missing families into official searchable
reaction-evidence layers:

1. Fries Rearrangement
2. Hofmann-Loffler-Freytag Reaction
3. Houben-Hoesch Reaction

What this patch does
--------------------
1. Normalizes family-name drift in data tables only:
   - Alkene (olefin) Metathesis -> Alkene (Olefin) Metathesis
   - Barton-Mccombie Radical Deoxygenation Reaction -> Barton-McCombie Radical Deoxygenation Reaction
   - Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement -> Fries Rearrangement
   - Hofmann-Löffler-Freytag Reaction (Remote Functionalization) -> Hofmann-Loffler-Freytag Reaction
   - Houben-Hoesch Reaction/Synthesis -> Houben-Hoesch Reaction

2. Backfills reaction_extracts.reactant_smiles / product_smiles from existing
   extract_molecules where possible for the three target families.

3. Inserts curated completion seeds:
   - Fries Rearrangement
     * 2 application-example pairs
   - Hofmann-Loffler-Freytag Reaction
     * 2 application-example pairs
   - Houben-Hoesch Reaction
     * 2 application-example pairs

4. Verifies that each target family now satisfies:
   - overview >= 1
   - application examples >= 2
   - distinct queryable pairs >= 3
   - rich completion pass = true

Files
-----
- complete_family_evidence_phase4_missing_trio.py
- verify_family_completion_phase4_missing_trio.py
- README_FAMILY_COMPLETION_PHASE4_MISSING_TRIO.txt

Recommended run order
---------------------
conda activate chemlens
cd /d C:\chemlens\backend

1) Preview only
python complete_family_evidence_phase4_missing_trio.py --db app\labint.db --dry-run

2) Real apply
python complete_family_evidence_phase4_missing_trio.py --db app\labint.db

3) Verify the three families directly
python verify_family_completion_phase4_missing_trio.py --db app\labint.db

4) Optional: refresh the global truth/dashboard
python final_state_verifier.py --db app\labint.db
python family_completion_dashboard.py --backend-root .

Important note
--------------
This patch intentionally does NOT mutate reaction_family_patterns.
The long-form registry alias rows remain as historical ontology rows, so the raw
final_state_verifier missing sample may still include those legacy names.
Use the dashboard's canonicalized display layer for current operational triage.

Why this is still the right next step
-------------------------------------
The official process is to make the canonical families themselves searchable and rich.
This patch does exactly that for the three canonicalized missing families, without
re-opening broad registry surgery in the same step.
