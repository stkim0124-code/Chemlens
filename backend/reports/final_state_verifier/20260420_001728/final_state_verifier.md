# FINAL STATE VERIFIER

generated_at: 2026-04-20 00:17:28
db_path: app\labint.db

## Canonical totals
- registry_family_total: 305
- registry_family_distinct_total: 291
- registry_duplicate_name_rows: 14
- families_with_extracts: 286
- queryable_family_count: 280
- reaction_extract_count: 535
- extract_molecule_count: 2816
- queryable_molecule_count: 988
- extracts_with_both_smiles: 261

## Integrity
- quick_check: ok
- integrity_check_head: ok

## Completion buckets
- missing_count: 5
- shallow_count: 276
- rich_count: 10
- collision_prone_candidate_count: 30
- recent_completed_family_count: 2
- recent_completed_families: Alkene (Olefin) Metathesis; Barton-McCombie Radical Deoxygenation Reaction

## Focus families

### Alkene (Olefin) Metathesis
- extract_count: 11
- overview_count: 5
- application_count: 5
- extract_with_both: 3
- queryable_reactants: 5
- queryable_products: 5
- unique_queryable_pair_count: 3
- completion_minimum_pass: True
- rich_completion_pass: True
- completion_bucket: rich
- collision_prone_candidate: False
- recent_curated_extract_summaries:
  - [792] canonical_overview :: Curated representative seed: ring-closing metathesis of 1,7-octadiene to cyclohexene.
  - [793] application_example :: Curated representative seed: cross-metathesis of styrene with methyl acrylate to methyl cinnamate.
  - [794] application_example :: Application example: ring-closing metathesis toward the (+)-Prelaureatin precursor.

### Barton-McCombie Radical Deoxygenation Reaction
- extract_count: 8
- overview_count: 2
- application_count: 3
- extract_with_both: 4
- queryable_reactants: 6
- queryable_products: 4
- unique_queryable_pair_count: 4
- completion_minimum_pass: True
- rich_completion_pass: True
- completion_bucket: rich
- collision_prone_candidate: False
- recent_curated_extract_summaries:
  - [795] canonical_overview :: Curated representative seed: 1-phenylethanol is deoxygenated to ethylbenzene via a xanthate intermediate under Barton-McCombie conditions.
  - [796] application_example :: Application example: Barton-McCombie deoxygenation in the synthesis of (±)-Δ9(12)-Capnellene.
  - [797] application_example :: Application example: Barton-McCombie deoxygenation in the synthesis of the octenoic acid side chain of zaragozic acid.

## Duplicate family names in reaction_family_patterns
- Arndt-Eistert Homologation / Synthesis :: row_count=2
- Aza-Cope Rearrangement :: row_count=2
- Aza-Wittig Reaction :: row_count=2
- Aza-[2,3]-Wittig Rearrangement :: row_count=2
- Baker-Venkataraman Rearrangement :: row_count=2
- Balz-Schiemann Reaction :: row_count=2
- Bamford-Stevens-Shapiro Olefination :: row_count=2
- Baylis-Hillman Reaction :: row_count=2
- Benzoin and Retro-Benzoin Condensation :: row_count=2
- Bischler-Napieralski Isoquinoline Synthesis :: row_count=2
- Buchwald-Hartwig Cross-Coupling :: row_count=2
- Castro-Stephens Coupling :: row_count=2
- Ciamician-Dennstedt Rearrangement :: row_count=2
- Claisen-Ireland Rearrangement :: row_count=2

## Alias residue
- reaction_extracts: case_aliases=0, unknown_family_names=0
- extract_molecules: case_aliases=0, unknown_family_names=0

## Top shallow families
- Alder Ene Reaction :: extract_count=1, overview=0, application=0, pairs=0
- Baeyer-Villiger Oxidation/rearrangement :: extract_count=1, overview=0, application=0, pairs=0
- Alder (Ene) Reaction (Hydro-Allyl Addition) :: extract_count=1, overview=1, application=0, pairs=1
- Amadori Reaction / Rearrangement :: extract_count=1, overview=1, application=0, pairs=1
- Arbuzov Reaction (Michaelis-Arbuzov Reaction) :: extract_count=1, overview=1, application=0, pairs=1
- Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement) :: extract_count=1, overview=1, application=0, pairs=1
- Baeyer-Villiger Oxidation/Rearrangement :: extract_count=1, overview=1, application=0, pairs=1
- Baldwin's Rules / Guidelines for Ring-Closing Reactions :: extract_count=1, overview=1, application=0, pairs=1
- Balz-Schiemann Reaction (Schiemann Reaction) :: extract_count=1, overview=1, application=0, pairs=1
- Buchner Method of Ring Expansion (Buchner Reaction) :: extract_count=1, overview=1, application=0, pairs=1
- Buchner Reaction :: extract_count=1, overview=1, application=0, pairs=1
- Carroll Rearrangement (Kimel-Cope Rearrangement) :: extract_count=1, overview=1, application=0, pairs=1
- Charette Asymmetric Cyclopropanation :: extract_count=1, overview=1, application=0, pairs=1
- Chichibabin Amination Reaction (Chichibabin Reaction) :: extract_count=1, overview=1, application=0, pairs=1
- Chugaev Elimination Reaction (Xanthate Ester Pyrolysis) :: extract_count=1, overview=1, application=0, pairs=1
- Claisen Condensation / Claisen Reaction :: extract_count=1, overview=1, application=0, pairs=1
- Combes Quinoline Synthesis :: extract_count=1, overview=1, application=0, pairs=1
- Cope Elimination / Cope Reaction :: extract_count=1, overview=1, application=0, pairs=1
- Cope Rearrangement :: extract_count=1, overview=1, application=0, pairs=1
- Corey-Bakshi-Shibata Reduction (CBS Reduction) :: extract_count=1, overview=1, application=0, pairs=1
- Corey-Chaykovsky Epoxidation and Cyclopropanation :: extract_count=1, overview=1, application=0, pairs=1
- Corey-Fuchs Alkyne Synthesis :: extract_count=1, overview=1, application=0, pairs=1
- Corey-Kim Oxidation :: extract_count=1, overview=1, application=0, pairs=1
- Corey-Nicolaou Macrolactonization :: extract_count=1, overview=1, application=0, pairs=1
- Corey-Winter Olefination :: extract_count=1, overview=1, application=0, pairs=1

## Missing family sample
- Alkene (olefin) Metathesis
- Barton-Mccombie Radical Deoxygenation Reaction
- Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement
- Hofmann-Löffler-Freytag Reaction (Remote Functionalization)
- Houben-Hoesch Reaction/Synthesis
