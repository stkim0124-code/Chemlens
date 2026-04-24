PHASE3 FAMILY COMPLETION PATCH
==============================

Goal
----
Complete two still-shallow families as official searchable reaction-evidence layers:

1. Alkene (Olefin) Metathesis
2. Barton-McCombie Radical Deoxygenation Reaction

What this patch does
--------------------
1. Normalizes lower-case family drift already present in the DB:
   - Alkene (olefin) Metathesis -> Alkene (Olefin) Metathesis
   - Barton-Mccombie Radical Deoxygenation Reaction -> Barton-McCombie Radical Deoxygenation Reaction

2. Backfills reaction_extracts.reactant_smiles / product_smiles from existing extract_molecules where possible.

3. Inserts curated completion seeds:
   - Alkene (Olefin) Metathesis
     * 1 canonical_overview pair (explicit RCM)
     * 2 application_example pairs (explicit CM + explicit RCM application)
   - Barton-McCombie Radical Deoxygenation Reaction
     * 1 canonical_overview pair (explicit alcohol->alkane deoxygenation)
     * 2 application_example pairs (isolated from real page-level route examples)

4. Writes summary reports.

Files
-----
- complete_family_evidence_phase3_metathesis_barton.py
- verify_family_completion_phase3.py
- README_FAMILY_COMPLETION_PHASE3.txt

Recommended run order
---------------------
conda activate chemlens
cd /d C:\chemlens\backend

1) Preview only
python complete_family_evidence_phase3_metathesis_barton.py --db app\labint.db --dry-run

2) Real apply
python complete_family_evidence_phase3_metathesis_barton.py --db app\labint.db

3) Verify
python verify_family_completion_phase3.py --db app\labint.db

Expected outcome
----------------
- official family names normalized
- both families have explicit queryable reactant/product evidence
- each family has at least:
  * canonical overview: 1+
  * application examples: 2+
  * both-side parse-safe extracts: 3+

Important note
--------------
This patch is intentionally narrow.
It is not a generic family dashboard patch yet.
The point is to finish these two families cleanly first, then continue with final verifier / completion dashboard work.
