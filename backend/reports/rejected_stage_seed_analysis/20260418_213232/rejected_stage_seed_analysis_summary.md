# Rejected stage seed analysis

Generated: 2026-04-18T21:32:32

- Canonical DB: `C:\chemlens\backend\app\labint.db`
- Stage DB: `C:\chemlens\backend\app\labint_v5_stage.db`
- Rejected JSON: `C:\chemlens\backend\reports\v5_selective_merge\20260418_160546\rejected_families.json`
- Diagnose summary: `reports\v5_rejected_diagnose\20260418_201257\rejected_diagnosis_summary.json`
- Apply summary: `reports\gemini_salvage_apply\20260418_201125\gemini_salvage_apply_summary.json`

- Base rejected families: **13**
- Already applied salvage families excluded: **0**
- Remaining rejected families analyzed: **13**

## Family-by-family analysis

### Enyne Metathesis

- Rejected reason: `top1 regression: baseline=1.0000 current=0.9259`
- Diagnose: changed_cases=2, first_case=`barton_radical_rxn_353_exact_extract`, first_top1=`Enyne Metathesis`, cluster=`barton`
- Stage-only extract ids: `[504]`
- Suspicion flags: `organometallic_reagent_present, multiple_queryable_core_molecules`
- Recommendation: `likely_needs_single_smiles_exposure_review_or_seed_replacement`

| role | smiles | queryable | rdkit_parse | sanitize |
|---|---|---:|---:|---:|
| reactant | `C=CCCCCCC` | 1 | True | True |
| reactant | `C#CCCCC` | 1 | True | True |
| product | `CCCC/C=C/C=C/CCCCCC` | 1 | True | True |
| reagent | `Cc1cc(C)c(N2CCN(c3c(C)cc(C)cc3C)[C]2=[Ru]([Cl])([Cl])(=[CH]c2ccccc2)[PH]C(C2CCCCC2)(C2CCCCC2)C2CCCCC2)c(C)c1` | 0 | True | True |

### Hofmann-Loffler-Freytag Reaction

- Rejected reason: `top1 regression: baseline=1.0000 current=0.9259`
- Diagnose: changed_cases=2, first_case=`barton_radical_rxn_353_exact_extract`, first_top1=`Hofmann-Loffler-Freytag Reaction`, cluster=`barton`
- Stage-only extract ids: `[530]`
- Suspicion flags: `explicit_intermediate_species_present`
- Recommendation: `likely_needs_single_smiles_exposure_review_or_seed_replacement`

| role | smiles | queryable | rdkit_parse | sanitize |
|---|---|---:|---:|---:|
| reactant | `CCCCN(C)Cl` | 1 | True | True |
| product | `CN1CCCC1` | 1 | True | True |
| reagent | `O=S(=O)(O)O` | 0 | True | True |
| reagent | `[OH-]` | 0 | True | True |
| intermediate | `[CH2]CCCNC` | 0 | True | True |
| intermediate | `CNCCCCCl` | 0 | True | True |

### Horner-Wadsworth-Emmons Olefination

- Rejected reason: `top1 regression: baseline=1.0000 current=0.9630`
- Diagnose: changed_cases=2, first_case=`buchner_403_exact_extract`, first_top1=`Horner-Wadsworth-Emmons Olefination`, cluster=`buchner`
- Stage-only extract ids: `[532]`
- Suspicion flags: `multiple_queryable_core_molecules`
- Recommendation: `likely_needs_core_pair_simplification_or_seed_replacement`

| role | smiles | queryable | rdkit_parse | sanitize |
|---|---|---:|---:|---:|
| reactant | `CCOC(=O)CP(=O)(OCC)OCC` | 1 | True | True |
| reactant | `O=Cc1ccccc1` | 1 | True | True |
| product | `CCOC(=O)/C=C/c1ccccc1` | 1 | True | True |
| reagent | `[NaH]` | 0 | True | True |

### Krapcho Dealkoxycarbonylation

- Rejected reason: `top1 regression: baseline=1.0000 current=0.9630`
- Diagnose: changed_cases=2, first_case=`buchner_403_exact_extract`, first_top1=`Krapcho Dealkoxycarbonylation`, cluster=`buchner`
- Stage-only extract ids: `[549]`
- Recommendation: `likely_needs_core_pair_simplification_or_seed_replacement`

| role | smiles | queryable | rdkit_parse | sanitize |
|---|---|---:|---:|---:|
| reactant | `CCOC(=O)C(C)(C(=O)OCC)c1ccccc1` | 1 | True | True |
| product | `CCOC(=O)C(C)c1ccccc1` | 1 | True | True |
| reagent | `CS(C)=O` | 0 | True | True |
| reagent | `O` | 0 | True | True |

### Mitsunobu Reaction

- Rejected reason: `top1 regression: baseline=1.0000 current=0.9259`
- Diagnose: changed_cases=2, first_case=`barton_radical_rxn_353_exact_extract`, first_top1=`Mitsunobu Reaction`, cluster=`barton`
- Stage-only extract ids: `[563]`
- Suspicion flags: `explicit_intermediate_species_present, multiple_queryable_core_molecules`
- Recommendation: `likely_needs_single_smiles_exposure_review_or_seed_replacement`

| role | smiles | queryable | rdkit_parse | sanitize |
|---|---|---:|---:|---:|
| reactant | `CCCCCC[C@H](C)O` | 1 | True | True |
| reactant | `O=C(O)c1ccccc1` | 1 | True | True |
| product | `CCCCCC[C@@H](C)OC(=O)c1ccccc1` | 1 | True | True |
| reagent | `c1ccc(P(c2ccccc2)c2ccccc2)cc1` | 0 | True | True |
| reagent | `CCOC(=O)N=NC(=O)OCC` | 0 | True | True |
| intermediate | `CCCCCC[C@H](C)O[P+](c1ccccc1)(c1ccccc1)c1ccccc1` | 0 | True | True |

### Claisen Condensation / Claisen Reaction

- Rejected reason: `top1 regression: baseline=1.0000 current=0.9630`
- Diagnose: changed_cases=1, first_case=`buchner_403_exact_extract`, first_top1=`Claisen Condensation / Claisen Reaction`, cluster=`buchner`
- Stage-only extract ids: `[479]`
- Suspicion flags: `explicit_intermediate_species_present`
- Recommendation: `likely_needs_core_pair_simplification_or_seed_replacement`

| role | smiles | queryable | rdkit_parse | sanitize |
|---|---|---:|---:|---:|
| reactant | `CCOC(C)=O` | 1 | True | True |
| product | `CCOC(=O)CC(C)=O` | 1 | True | True |
| intermediate | `[CH2-]C(=O)OCC` | 0 | True | True |

### Michael Addition Reaction

- Rejected reason: `top1 regression: baseline=1.0000 current=0.9630`
- Diagnose: changed_cases=1, first_case=`buchner_403_exact_extract`, first_top1=`Michael Addition Reaction`, cluster=`buchner`
- Stage-only extract ids: `[559]`
- Suspicion flags: `explicit_intermediate_species_present, multiple_queryable_core_molecules`
- Recommendation: `likely_needs_core_pair_simplification_or_seed_replacement`

| role | smiles | queryable | rdkit_parse | sanitize |
|---|---|---:|---:|---:|
| reactant | `CCOC(=O)CC(=O)OCC` | 1 | True | True |
| reactant | `C=CC(C)=O` | 1 | True | True |
| product | `CCOC(=O)C(CCC(C)=O)C(=O)OCC` | 1 | True | True |
| intermediate | `CCOC(=O)[C-]C(=O)OCC` | 0 | True | True |

### Regitz Diazo Transfer

- Rejected reason: `top1 regression: baseline=1.0000 current=0.9630`
- Diagnose: changed_cases=1, first_case=`buchner_403_exact_extract`, first_top1=`Regitz Diazo Transfer`, cluster=`buchner`
- Stage-only extract ids: `[475]`
- Recommendation: `likely_needs_core_pair_simplification_or_seed_replacement`

| role | smiles | queryable | rdkit_parse | sanitize |
|---|---|---:|---:|---:|
| reactant | `CCOC(=O)CC(=O)OCC` | 1 | True | True |
| product | `CCOC(=O)C(=[N+]=[N-])C(=O)OCC` | 1 | True | True |
| reagent | `Cc1ccc(S(=O)(=O)N=[N+]=[N-])cc1` | 0 | True | True |
| reagent | `CCN(CC)CC` | 0 | True | True |

### Diels-Alder Cycloaddition

- Rejected reason: `top1 regression: baseline=1.0000 current=0.9630`
- Stage-only extract ids: `[467]`
- Suspicion flags: `multiple_queryable_core_molecules`
- Recommendation: `inspect_stage_seed`

| role | smiles | queryable | rdkit_parse | sanitize |
|---|---|---:|---:|---:|
| reactant | `C1=CCC=C1` | 1 | True | True |
| reactant | `O=C1C=CC(=O)O1` | 1 | True | True |
| product | `O=C1OC(=O)[C@H]2[C@@H]1[C@@H]1C=C[C@@H]2C1` | 1 | True | True |

### Finkelstein Reaction

- Rejected reason: `top1 regression: baseline=1.0000 current=0.9259`
- Stage-only extract ids: `[512]`
- Recommendation: `inspect_stage_seed`

| role | smiles | queryable | rdkit_parse | sanitize |
|---|---|---:|---:|---:|
| reactant | `CCCBr` | 1 | True | True |
| product | `CCCI` | 1 | True | True |
| reagent | `CC(C)=O` | 0 | True | True |

### Fries Rearrangement

- Rejected reason: `top1 regression: baseline=1.0000 current=0.9630`
- Stage-only extract ids: `[516]`
- Suspicion flags: `multiple_queryable_core_molecules`
- Recommendation: `inspect_stage_seed`

| role | smiles | queryable | rdkit_parse | sanitize |
|---|---|---:|---:|---:|
| reactant | `CC(=O)Oc1ccccc1` | 1 | True | True |
| product | `CC(=O)c1ccc(O)cc1` | 1 | True | True |
| product | `CC(=O)c1ccccc1O` | 1 | True | True |
| reagent | `[Cl][Al]([Cl])[Cl]` | 0 | True | True |

### Houben-Hoesch Reaction

- Rejected reason: `top1 regression: baseline=1.0000 current=0.9259`
- Stage-only extract ids: `[533]`
- Suspicion flags: `explicit_intermediate_species_present, multiple_queryable_core_molecules`
- Recommendation: `inspect_stage_seed`

| role | smiles | queryable | rdkit_parse | sanitize |
|---|---|---:|---:|---:|
| reactant | `Oc1cc(O)cc(O)c1` | 1 | True | True |
| reactant | `CC#N` | 1 | True | True |
| product | `CC(=O)c1c(O)cc(O)cc1O` | 1 | True | True |
| reagent | `[Cl][Zn][Cl]` | 0 | True | True |
| reagent | `Cl` | 0 | True | True |
| reagent | `O` | 0 | True | True |
| intermediate | `CC(=[NH2+])c1c(O)cc(O)cc1O` | 0 | True | True |

### Hunsdiecker Reaction

- Rejected reason: `top1 regression: baseline=1.0000 current=0.9259`
- Stage-only extract ids: `[534]`
- Recommendation: `inspect_stage_seed`

| role | smiles | queryable | rdkit_parse | sanitize |
|---|---|---:|---:|---:|
| reactant | `CCCC(=O)[O-]` | 1 | True | True |
| product | `CCCBr` | 1 | True | True |
| reagent | `BrBr` | 0 | True | True |
| intermediate | `CCCC(=O)OBr` | 0 | True | True |
