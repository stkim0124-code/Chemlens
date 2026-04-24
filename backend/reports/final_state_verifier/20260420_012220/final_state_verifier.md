# FINAL STATE VERIFIER V2

generated_at: 2026-04-20 01:22:21
db_path: app\labint.db

## Canonical totals
- registry_family_total_raw: 305
- registry_family_distinct_total_raw: 291
- registry_duplicate_name_rows_raw: 14
- registry_family_distinct_total_canonicalized: 280
- families_with_extracts_canonicalized: 280
- queryable_family_count_canonicalized: 271
- reaction_extract_count: 545
- extract_molecule_count: 2842
- queryable_molecule_count: 1014
- extracts_with_both_smiles: 276

## Integrity
- quick_check: ok
- integrity_check_head: ok

## Completion buckets (canonicalized)
- missing_count: 0
- shallow_count: 265
- rich_count: 15
- collision_prone_candidate_count: 27
- recent_completed_family_count: 7
- recent_completed_families: Alder (Ene) Reaction (Hydro-Allyl Addition); Alkene (Olefin) Metathesis; Amadori Reaction / Rearrangement; Arbuzov Reaction (Michaelis-Arbuzov Reaction); Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement); Baeyer-Villiger Oxidation/Rearrangement; Barton-McCombie Radical Deoxygenation Reaction

## Completion buckets (raw/un-collapsed registry view)
- missing_count: 10
- shallow_count: 266
- rich_count: 15
- collision_prone_candidate_count: 27
- recent_completed_family_count: 7

## Focus families

### Alkene (Olefin) Metathesis
- raw_family_names_collapsed: Alkene (Olefin) Metathesis; Alkene (olefin) Metathesis
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
- raw_family_names_collapsed: Barton-McCombie Radical Deoxygenation Reaction; Barton-Mccombie Radical Deoxygenation Reaction
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

### Fries Rearrangement
- raw_family_names_collapsed: Fries Rearrangement; Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement
- extract_count: 3
- overview_count: 1
- application_count: 2
- extract_with_both: 3
- queryable_reactants: 3
- queryable_products: 3
- unique_queryable_pair_count: 3
- completion_minimum_pass: True
- rich_completion_pass: True
- completion_bucket: rich
- collision_prone_candidate: False

### Hofmann-Loffler-Freytag Reaction
- raw_family_names_collapsed: Hofmann-Loffler-Freytag Reaction; Hofmann-Löffler-Freytag Reaction (Remote Functionalization)
- extract_count: 3
- overview_count: 1
- application_count: 2
- extract_with_both: 3
- queryable_reactants: 3
- queryable_products: 3
- unique_queryable_pair_count: 3
- completion_minimum_pass: True
- rich_completion_pass: True
- completion_bucket: rich
- collision_prone_candidate: False

### Houben-Hoesch Reaction
- raw_family_names_collapsed: Houben-Hoesch Reaction; Houben-Hoesch Reaction/Synthesis
- extract_count: 3
- overview_count: 1
- application_count: 2
- extract_with_both: 3
- queryable_reactants: 5
- queryable_products: 3
- unique_queryable_pair_count: 3
- completion_minimum_pass: True
- rich_completion_pass: True
- completion_bucket: rich
- collision_prone_candidate: False

## Canonical alias groups sample
- Alder (Ene) Reaction (Hydro-Allyl Addition) :: raw_names=Alder (Ene) Reaction (Hydro-Allyl Addition) ; Alder (ene) Reaction ; Alder Ene Reaction
- Alkene (Olefin) Metathesis :: raw_names=Alkene (Olefin) Metathesis ; Alkene (olefin) Metathesis
- Amadori Reaction / Rearrangement :: raw_names=Amadori Reaction / Rearrangement ; Amadori Rearrangement
- Arbuzov Reaction (Michaelis-Arbuzov Reaction) :: raw_names=Arbuzov Reaction ; Arbuzov Reaction (Michaelis-Arbuzov Reaction)
- Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement) :: raw_names=Aza-Claisen Rearrangement ; Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)
- Baeyer-Villiger Oxidation/Rearrangement :: raw_names=Baeyer-Villiger Oxidation/Rearrangement ; Baeyer-Villiger Oxidation/rearrangement
- Barton-McCombie Radical Deoxygenation Reaction :: raw_names=Barton-McCombie Radical Deoxygenation Reaction ; Barton-Mccombie Radical Deoxygenation Reaction
- Fries Rearrangement :: raw_names=Fries Rearrangement ; Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement
- Hofmann-Loffler-Freytag Reaction :: raw_names=Hofmann-Loffler-Freytag Reaction ; Hofmann-Löffler-Freytag Reaction (Remote Functionalization)
- Houben-Hoesch Reaction :: raw_names=Houben-Hoesch Reaction ; Houben-Hoesch Reaction/Synthesis

## Duplicate family names in reaction_family_patterns (raw rows)
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
- reaction_extracts: case_aliases=1, unknown_family_names=0
- extract_molecules: case_aliases=0, unknown_family_names=0

## Top shallow families (canonicalized)
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
- Cornforth Rearrangement :: extract_count=1, overview=1, application=0, pairs=1
- Criegee Oxidation :: extract_count=1, overview=1, application=0, pairs=1
- Curtius Rearrangement :: extract_count=1, overview=1, application=0, pairs=1
- Dakin Oxidation :: extract_count=1, overview=1, application=0, pairs=1
- Dakin-West Reaction :: extract_count=1, overview=1, application=0, pairs=1
- Danheiser Benzannulation :: extract_count=1, overview=1, application=0, pairs=1
- Danheiser Cyclopentene Annulation :: extract_count=1, overview=1, application=0, pairs=1

## Missing family sample (canonicalized)

## Missing family sample (raw/un-collapsed)
- Alder (ene) Reaction
- Alder Ene Reaction
- Alkene (olefin) Metathesis
- Amadori Rearrangement
- Arbuzov Reaction
- Aza-Claisen Rearrangement
- Barton-Mccombie Radical Deoxygenation Reaction
- Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement
- Hofmann-Löffler-Freytag Reaction (Remote Functionalization)
- Houben-Hoesch Reaction/Synthesis
