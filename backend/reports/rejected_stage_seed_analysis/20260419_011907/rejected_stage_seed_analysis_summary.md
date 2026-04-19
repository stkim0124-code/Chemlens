# Rejected stage seed analysis v2

Generated: 2026-04-19T01:19:07

- Canonical DB: `C:\chemlens\backend\app\labint.db`
- Stage DB: `C:\chemlens\backend\app\labint_v5_stage.db`
- Rejected JSON: `C:\chemlens\backend\reports\v5_selective_merge\20260418_160546\rejected_families.json`
- Diagnose summary: `C:\chemlens\backend\reports\v5_rejected_diagnose\20260418_201257\rejected_diagnosis_summary.json`
- Apply summary: `C:\chemlens\backend\reports\gemini_salvage_apply\20260418_201125\gemini_salvage_apply_summary.json`

- Base rejected families in selective merge report: **13**
- Applied salvage families: **2**
- Active rejected families from latest post-apply diagnose: **8**

## Applied salvage families

- Finkelstein Reaction
- Hunsdiecker Reaction

## Base rejected families no longer active

- Diels-Alder Cycloaddition
- Finkelstein Reaction
- Fries Rearrangement
- Houben-Hoesch Reaction
- Hunsdiecker Reaction

## Active rejected families

### Enyne Metathesis

- changed_cases: **2**
- first_case: `barton_radical_rxn_353_exact_extract`
- first_top1: `Enyne Metathesis`
- cluster: `barton`
- stage_only_extract_ids: `[504]`
- seed_extract_ids: `[504]`
- core_queryable_count: `3` roles=`['reactant', 'reactant', 'product']`
- recommendation: `review_single_smiles_exposure_then_consider_seed_replacement`
- suspicion_flags: `organometallic_reagent_present, three_or_more_queryable_core_molecules, single_smiles_exposure_risk`

#### stage_seed_extracts

- extract_id=504 kind=canonical_overview reactant_smiles=`C=CCCCCCC | C#CCCCC` product_smiles=`CCCC/C=C/C=C/CCCCCC`

### Hofmann-Loffler-Freytag Reaction

- changed_cases: **2**
- first_case: `barton_radical_rxn_353_exact_extract`
- first_top1: `Hofmann-Loffler-Freytag Reaction`
- cluster: `barton`
- stage_only_extract_ids: `[530]`
- seed_extract_ids: `[530]`
- core_queryable_count: `2` roles=`['reactant', 'product']`
- recommendation: `review_single_smiles_exposure_then_consider_seed_replacement`
- suspicion_flags: `single_smiles_exposure_risk`

#### stage_seed_extracts

- extract_id=530 kind=canonical_overview reactant_smiles=`CCCCN(C)Cl` product_smiles=`CN1CCCC1`

### Horner-Wadsworth-Emmons Olefination

- changed_cases: **2**
- first_case: `buchner_403_exact_extract`
- first_top1: `Horner-Wadsworth-Emmons Olefination`
- cluster: `buchner`
- stage_only_extract_ids: `[532]`
- seed_extract_ids: `[532]`
- core_queryable_count: `3` roles=`['reactant', 'reactant', 'product']`
- recommendation: `review_core_pair_simplification_then_consider_seed_replacement`
- suspicion_flags: `three_or_more_queryable_core_molecules, core_pair_similarity_risk`

#### stage_seed_extracts

- extract_id=532 kind=canonical_overview reactant_smiles=`CCOC(=O)CP(=O)(OCC)OCC | O=Cc1ccccc1` product_smiles=`CCOC(=O)/C=C/c1ccccc1`

### Krapcho Dealkoxycarbonylation

- changed_cases: **2**
- first_case: `buchner_403_exact_extract`
- first_top1: `Krapcho Dealkoxycarbonylation`
- cluster: `buchner`
- stage_only_extract_ids: `[549]`
- seed_extract_ids: `[549]`
- core_queryable_count: `2` roles=`['reactant', 'product']`
- recommendation: `review_core_pair_simplification_then_consider_seed_replacement`
- suspicion_flags: `core_pair_similarity_risk`

#### stage_seed_extracts

- extract_id=549 kind=canonical_overview reactant_smiles=`CCOC(=O)C(C)(C(=O)OCC)c1ccccc1` product_smiles=`CCOC(=O)C(C)c1ccccc1`

### Mitsunobu Reaction

- changed_cases: **2**
- first_case: `barton_radical_rxn_353_exact_extract`
- first_top1: `Mitsunobu Reaction`
- cluster: `barton`
- stage_only_extract_ids: `[563]`
- seed_extract_ids: `[563]`
- core_queryable_count: `3` roles=`['reactant', 'reactant', 'product']`
- recommendation: `review_single_smiles_exposure_then_consider_seed_replacement`
- suspicion_flags: `three_or_more_queryable_core_molecules, single_smiles_exposure_risk`

#### stage_seed_extracts

- extract_id=563 kind=canonical_overview reactant_smiles=`CCCCCC[C@H](C)O | O=C(O)c1ccccc1` product_smiles=`CCCCCC[C@@H](C)OC(=O)c1ccccc1`

### Claisen Condensation / Claisen Reaction

- changed_cases: **1**
- first_case: `buchner_403_exact_extract`
- first_top1: `Claisen Condensation / Claisen Reaction`
- cluster: `buchner`
- stage_only_extract_ids: `[479]`
- seed_extract_ids: `[479]`
- core_queryable_count: `2` roles=`['reactant', 'product']`
- recommendation: `review_core_pair_simplification_then_consider_seed_replacement`
- suspicion_flags: `core_pair_similarity_risk`

#### stage_seed_extracts

- extract_id=479 kind=canonical_overview reactant_smiles=`CCOC(C)=O` product_smiles=`CCOC(=O)CC(C)=O`

### Michael Addition Reaction

- changed_cases: **1**
- first_case: `buchner_403_exact_extract`
- first_top1: `Michael Addition Reaction`
- cluster: `buchner`
- stage_only_extract_ids: `[559]`
- seed_extract_ids: `[559]`
- core_queryable_count: `3` roles=`['reactant', 'reactant', 'product']`
- recommendation: `review_core_pair_simplification_then_consider_seed_replacement`
- suspicion_flags: `three_or_more_queryable_core_molecules, core_pair_similarity_risk`

#### stage_seed_extracts

- extract_id=559 kind=canonical_overview reactant_smiles=`CCOC(=O)CC(=O)OCC | C=CC(C)=O` product_smiles=`CCOC(=O)C(CCC(C)=O)C(=O)OCC`

### Regitz Diazo Transfer

- changed_cases: **1**
- first_case: `buchner_403_exact_extract`
- first_top1: `Regitz Diazo Transfer`
- cluster: `buchner`
- stage_only_extract_ids: `[475]`
- seed_extract_ids: `[475]`
- core_queryable_count: `2` roles=`['reactant', 'product']`
- recommendation: `review_core_pair_simplification_then_consider_seed_replacement`
- suspicion_flags: `core_pair_similarity_risk`

#### stage_seed_extracts

- extract_id=475 kind=canonical_overview reactant_smiles=`CCOC(=O)CC(=O)OCC` product_smiles=`CCOC(=O)C(=[N+]=[N-])C(=O)OCC`

