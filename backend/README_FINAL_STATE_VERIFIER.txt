CHEMLENS FINAL STATE VERIFIER PATCH
==================================

What this patch is for
----------------------
This patch creates a single DB-truth final verifier for the current canonical state.
It is intentionally read-only.
It does NOT mutate the DB.

Why this comes before the dashboard
----------------------------------
The dashboard should not invent its own state.
It should read a finalized verifier output that already answers:
- how many canonical families really exist now
- how many are queryable now
- which families are missing / shallow / rich
- whether alias residue still exists
- whether the two recently completed families are really reflected in canonical

Files in this patch
-------------------
- final_state_verifier.py
- README_FINAL_STATE_VERIFIER.txt

What final_state_verifier.py outputs
------------------------------------
A timestamped report under:
reports/final_state_verifier/<timestamp>/

It writes:
- final_state_verifier.json
- final_state_verifier.md

What the verifier checks
------------------------
1) Canonical totals
   - registry_family_total
   - registry_family_distinct_total
   - registry_duplicate_name_rows
   - families_with_extracts
   - queryable_family_count
   - reaction_extract_count
   - extract_molecule_count
   - queryable_molecule_count
   - extracts_with_both_smiles

2) Integrity hints
   - PRAGMA quick_check
   - PRAGMA integrity_check

3) Focus family verification
   Default focus families are:
   - Alkene (Olefin) Metathesis
   - Barton-McCombie Radical Deoxygenation Reaction

   For each focus family it reports:
   - extract_count
   - overview_count
   - application_count
   - extract_with_both
   - queryable_reactants
   - queryable_products
   - unique_queryable_pair_count
   - completion_minimum_pass
   - rich_completion_pass
   - completion_bucket
   - collision_prone_candidate
   - recent_curated_extract_summaries

4) Alias residue
   It checks reaction_extracts and extract_molecules for:
   - case-only alias drift
   - family names not found in reaction_family_patterns

5) Completion buckets
   Families are grouped into:
   - missing
   - shallow
   - rich

6) Collision-prone candidates
   It flags families that have several extracts but too few distinct queryable pairs.
   This is only a heuristic warning layer, not a hard failure.

How to run
----------
conda activate chemlens
cd /d C:\chemlens\backend

python final_state_verifier.py --db app\labint.db

Optional:
python final_state_verifier.py --db app\labint.db --focus-families "Alkene (Olefin) Metathesis;Barton-McCombie Radical Deoxygenation Reaction"

What success looks like right now
---------------------------------
For the current workflow, the important success condition is:
- the two recently completed families appear in focus_families
- completion_minimum_pass is true for both
- alias_residue is empty or near-empty for those two families

After this patch
----------------
The next patch should be the family completion dashboard draft.
That dashboard should consume the verifier truth, not re-infer state independently.
