# FINAL STATE VERIFIER V2

generated_at: 2026-04-20 23:54:42
db_path: C:\chemlens\backend\app\labint.db

## Canonical totals
- registry_family_total_raw: 305
- registry_family_distinct_total_raw: 291
- registry_duplicate_name_rows_raw: 14
- registry_family_distinct_total_canonicalized: 264
- families_with_extracts_canonicalized: 264
- queryable_family_count_canonicalized: 257
- reaction_extract_count: 851
- extract_molecule_count: 3501
- queryable_molecule_count: 1673
- extracts_with_both_smiles: 588

## Integrity
- quick_check: ok
- integrity_check_head: ok

## Completion buckets (canonicalized)
- missing_count: 0
- shallow_count: 129
- rich_count: 135
- collision_prone_candidate_count: 23
- recent_completed_family_count: 82
- recent_completed_families: Alder (Ene) Reaction (Hydro-Allyl Addition); Alkene (Olefin) Metathesis; Amadori Reaction / Rearrangement; Arbuzov Reaction (Michaelis-Arbuzov Reaction); Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement); Baeyer-Villiger Oxidation/Rearrangement; Balz-Schiemann Reaction (Schiemann Reaction); Barton-McCombie Radical Deoxygenation Reaction; Buchner Method of Ring Expansion (Buchner Reaction); Carroll Rearrangement (Kimel-Cope Rearrangement); Charette Asymmetric Cyclopropanation; Chichibabin Amination Reaction (Chichibabin Reaction); Chugaev Elimination Reaction (Xanthate Ester Pyrolysis); Claisen Condensation / Claisen Reaction; Combes Quinoline Synthesis; Cope Elimination / Cope Reaction; Cope Rearrangement; Corey-Bakshi-Shibata Reduction (CBS Reduction); Corey-Chaykovsky Epoxidation and Cyclopropanation; Corey-Fuchs Alkyne Synthesis; Corey-Kim Oxidation; Corey-Nicolaou Macrolactonization; Corey-Winter Olefination; Cornforth Rearrangement; Criegee Oxidation; Curtius Rearrangement; Dakin Oxidation; Dakin-West Reaction; Danheiser Benzannulation; Danheiser Cyclopentene Annulation; Danishefsky's Diene Cycloaddition; Darzens Glycidic Ester Condensation; Davis' Oxaziridine Oxidations; De Mayo Cycloaddition; Demjanov Rearrangement and Tiffeneau-Demjanov Rearrangement; Dess-Martin Oxidation; Dieckmann Condensation; Diels-Alder Cycloaddition; Dienone-Phenol Rearrangement; Enyne Metathesis; Eschenmoser-Claisen Rearrangement; Eschweiler-Clarke Methylation; Evans Aldol Reaction; Favorskii Rearrangement; Feist-Bénary Furan Synthesis; Ferrier Reaction; Finkelstein Reaction; Fischer Indole Synthesis; Fleming-Tamao Oxidation; Friedel-Crafts Acylation; Friedel-Crafts Alkylation; Furukawa Modification; Gabriel Synthesis; Gattermann and Gattermann-Koch Formylation; Glaser Coupling; Grignard Reaction; Hantzsch Dihydropyridine Synthesis; Heck Reaction; Hell-Volhard-Zelinsky Reaction; Henry Reaction; Hetero Diels-Alder Cycloaddition (HDA); Hofmann Elimination; Hofmann Rearrangement; Horner-Wadsworth-Emmons Olefination; Horner-Wadsworth-Emmons Olefination – Still-Gennari Modification; Hunsdiecker Reaction; Jacobsen Hydrolytic Kinetic Resolution; Jacobsen-Katsuki Epoxidation; Japp-Klingemann Reaction; Johnson-Claisen Rearrangement; Jones Oxidation; Julia-Lythgoe Olefination; Knoevenagel Condensation; Knorr Pyrrole Synthesis; Kornblum Oxidation; Krapcho Dealkoxycarbonylation (Krapcho Reaction); Kumada Cross-Coupling; Ley Oxidation; Lieben Haloform Reaction; Lossen Rearrangement; Luche Reduction; Mannich Reaction

## Completion buckets (raw/un-collapsed registry view)
- missing_count: 26
- shallow_count: 130
- rich_count: 135
- collision_prone_candidate_count: 23
- recent_completed_family_count: 82

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
- Balz-Schiemann Reaction (Schiemann Reaction) :: raw_names=Balz-Schiemann Reaction ; Balz-Schiemann Reaction (Schiemann Reaction)
- Barton-McCombie Radical Deoxygenation Reaction :: raw_names=Barton-McCombie Radical Deoxygenation Reaction ; Barton-Mccombie Radical Deoxygenation Reaction
- Buchner Method of Ring Expansion (Buchner Reaction) :: raw_names=Buchner Method of Ring Expansion ; Buchner Method of Ring Expansion (Buchner Reaction) ; Buchner Reaction
- Carroll Rearrangement (Kimel-Cope Rearrangement) :: raw_names=Carroll Rearrangement ; Carroll Rearrangement (Kimel-Cope Rearrangement)
- Chichibabin Amination Reaction (Chichibabin Reaction) :: raw_names=Chichibabin Amination Reaction ; Chichibabin Amination Reaction (Chichibabin Reaction)
- Chugaev Elimination Reaction (Xanthate Ester Pyrolysis) :: raw_names=Chugaev Elimination Reaction ; Chugaev Elimination Reaction (Xanthate Ester Pyrolysis)
- Claisen Condensation / Claisen Reaction :: raw_names=Claisen Condensation ; Claisen Condensation / Claisen Reaction
- De Mayo Cycloaddition :: raw_names=De Mayo Cycloaddition ; De Mayo Cycloaddition (Enone-Alkene [2+2] Photocycloaddition)
- Demjanov Rearrangement and Tiffeneau-Demjanov Rearrangement :: raw_names=Demjanov Rearrangement and Tiffeneau-Demjanov Rearrangement ; Demjanov and Tiffeneau-Demjanov Rearrangement
- Eschweiler-Clarke Methylation :: raw_names=Eschweiler-Clarke Methylation ; Eschweiler-Clarke Methylation (Reductive Alkylation)
- Ferrier Reaction :: raw_names=Ferrier Reaction ; Ferrier Reaction/Rearrangement
- Fries Rearrangement :: raw_names=Fries Rearrangement ; Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement
- Hetero Diels-Alder Cycloaddition (HDA) :: raw_names=Hetero Diels-Alder Cycloaddition ; Hetero Diels-Alder Cycloaddition (HDA)
- Hofmann-Loffler-Freytag Reaction :: raw_names=Hofmann-Loffler-Freytag Reaction ; Hofmann-Löffler-Freytag Reaction (Remote Functionalization)
- Horner-Wadsworth-Emmons Olefination :: raw_names=Horner-Wadsworth-Emmons ; Horner-Wadsworth-Emmons Olefination
- Horner-Wadsworth-Emmons Olefination – Still-Gennari Modification :: raw_names=Horner-Wadsworth-Emmons Olefination – Still-Gennari Modification ; Still-Gennari Modification
- Houben-Hoesch Reaction :: raw_names=Houben-Hoesch Reaction ; Houben-Hoesch Reaction/Synthesis
- Jones Oxidation :: raw_names=Jones Oxidation ; Jones Oxidation/Oxidation of Alcohols by Chromium Reagents
- Krapcho Dealkoxycarbonylation (Krapcho Reaction) :: raw_names=Krapcho Dealkoxycarbonylation ; Krapcho Dealkoxycarbonylation (Krapcho Reaction)

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
- Paal-Knorr Furan Synthesis :: extract_count=1, overview=1, application=0, pairs=1
- Paal-Knorr Pyrrole Synthesis :: extract_count=1, overview=1, application=0, pairs=1
- Passerini Multicomponent Reaction :: extract_count=1, overview=1, application=0, pairs=1
- Paternò-Büchi Reaction :: extract_count=1, overview=1, application=0, pairs=1
- Pauson-Khand Reaction :: extract_count=1, overview=1, application=0, pairs=1
- Payne Rearrangement :: extract_count=1, overview=1, application=0, pairs=1
- Perkin Reaction :: extract_count=1, overview=1, application=0, pairs=1
- Petasis Boronic Acid-Mannich Reaction :: extract_count=1, overview=1, application=0, pairs=1
- Petasis-Ferrier Rearrangement :: extract_count=1, overview=1, application=0, pairs=1
- Peterson Olefination :: extract_count=1, overview=1, application=0, pairs=1
- Pfitzner-Moffatt Oxidation :: extract_count=1, overview=1, application=0, pairs=1
- Pictet-Spengler Tetrahydroisoquinoline Synthesis :: extract_count=1, overview=1, application=0, pairs=1
- Pinacol and Semipinacol Rearrangement :: extract_count=1, overview=1, application=0, pairs=1
- Pinner Reaction :: extract_count=1, overview=1, application=0, pairs=1
- Pinnick Oxidation :: extract_count=1, overview=1, application=0, pairs=1
- Polonovski Reaction :: extract_count=1, overview=1, application=0, pairs=1
- Pomeranz-Fritsch Reaction :: extract_count=1, overview=1, application=0, pairs=1
- Prilezhaev Reaction :: extract_count=1, overview=1, application=0, pairs=1
- Prins Reaction :: extract_count=1, overview=1, application=0, pairs=1
- Prins-Pinacol Rearrangement :: extract_count=1, overview=1, application=0, pairs=1
- Prévost Reaction :: extract_count=1, overview=1, application=0, pairs=1
- Pummerer Rearrangement :: extract_count=1, overview=1, application=0, pairs=1
- Quasi-Favorskii Rearrangement :: extract_count=1, overview=1, application=0, pairs=1
- Ramberg-Bäcklund Rearrangement :: extract_count=1, overview=1, application=0, pairs=1
- Reformatsky Reaction :: extract_count=1, overview=1, application=0, pairs=1

## Missing family sample (canonicalized)

## Missing family sample (raw/un-collapsed)
- Alder (ene) Reaction
- Alder Ene Reaction
- Alkene (olefin) Metathesis
- Amadori Rearrangement
- Arbuzov Reaction
- Aza-Claisen Rearrangement
- Balz-Schiemann Reaction
- Barton-Mccombie Radical Deoxygenation Reaction
- Buchner Method of Ring Expansion
- Buchner Reaction
- Carroll Rearrangement
- Chichibabin Amination Reaction
- Chugaev Elimination Reaction
- Claisen Condensation
- De Mayo Cycloaddition (Enone-Alkene [2+2] Photocycloaddition)
- Demjanov and Tiffeneau-Demjanov Rearrangement
- Eschweiler-Clarke Methylation (Reductive Alkylation)
- Ferrier Reaction/Rearrangement
- Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement
- Hetero Diels-Alder Cycloaddition
- Hofmann-Löffler-Freytag Reaction (Remote Functionalization)
- Horner-Wadsworth-Emmons
- Houben-Hoesch Reaction/Synthesis
- Jones Oxidation/Oxidation of Alcohols by Chromium Reagents
- Krapcho Dealkoxycarbonylation
- Still-Gennari Modification
