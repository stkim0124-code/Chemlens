# CHEMLENS small benchmark v3 report

## Summary

- total_cases: 35
- reaction_query_cases: 27
- adversarial_noisy_cases: 5
- adversarial_meaningful_cases: 3
- top1_accuracy (reaction): 0.9630
- top3_accuracy (reaction): 1.0000
- disallow_top3_violations: 0
- unique_families: 16
- boundary_cases_with_acceptance_override: 2

## Adversarial — Noisy query robustness

Pass rate: 0/5

| case_id | smiles | no_confident_hit | top1 | top1_score | pass |
|---|---|:---:|---|---:|:---:|
| noisy_acetic_acid | `Acetic acid` | ❌ | Friedel-Crafts Acylation | 0.716 | ❌ |
| noisy_ethanol | `Ethanol` | ❌ | Malonic Ester Synthesis | 0.756 | ❌ |
| noisy_benzene | `Benzene` | ❌ | Friedel-Crafts Acylation | 0.84 | ❌ |
| noisy_acetone | `Acetone` | ❌ | Grignard Reaction | 0.84 | ❌ |
| noisy_toluene | `Toluene` | ❌ | Aza-Wittig Reaction | 1.509 | ❌ |

## Adversarial — Meaningful single SMILES

Top3 pass rate: 2/3

| case_id | expected | top1 | top3 families | top3 scores | pass |
|---|---|---|---|---|:---:|
| meaningful_pentanoic_acid | Barton Radical Decarboxylation Reaction | Barton Radical Decarboxylation Reaction | Barton Radical Decarboxylation Reaction, Malonic Ester Synthesis, Claisen Condensation / Claisen Reaction | 0.891 | 0.431 | 0.3 | ✅ |
| meaningful_cyclohexanone | Baeyer-Villiger Oxidation | Eschenmoser Methenylation | Eschenmoser Methenylation, Enders SAMP/RAMP Hydrazone Alkylation, Corey-Chaykovsky Epoxidation and Cyclopropanation | 1.642 | 1.582 | 1.2 | ❌ |
| meaningful_aniline_aryl_bromide | Buchwald-Hartwig Cross-Coupling | Kumada Cross-Coupling | Kumada Cross-Coupling, Buchwald-Hartwig Cross-Coupling, Lossen Rearrangement | 0.486 | 0.419 | 0.177 | ✅ |

## Family coverage (reaction queries)

| family | cases | top1 | top3 |
|---|---:|---:|---:|
| Aza-Claisen Rearrangement | 2 | 2/2 | 2/2 |
| Baeyer-Villiger Oxidation | 3 | 3/3 | 3/3 |
| Baker-Venkataraman Rearrangement | 2 | 2/2 | 2/2 |
| Balz-Schiemann Reaction | 1 | 1/1 | 1/1 |
| Bartoli Indole Synthesis | 1 | 1/1 | 1/1 |
| Barton Radical Decarboxylation | 2 | 2/2 | 2/2 |
| Barton Radical Decarboxylation Reaction | 1 | 1/1 | 1/1 |
| Barton-Mccombie Radical Deoxygenation Reaction | 1 | 1/1 | 1/1 |
| Benzilic Acid Rearrangement | 2 | 2/2 | 2/2 |
| Brook Rearrangement | 1 | 1/1 | 1/1 |
| Buchner Reaction | 1 | 0/1 | 1/1 |
| Buchwald-Hartwig Cross-Coupling | 3 | 3/3 | 3/3 |
| Castro-Stephens Coupling | 1 | 1/1 | 1/1 |
| Chichibabin Amination Reaction | 2 | 2/2 | 2/2 |
| Claisen Condensation | 2 | 2/2 | 2/2 |
| Claisen Rearrangement | 2 | 2/2 | 2/2 |

## Case table (reaction queries)

| case_id | expected | top1 | top3 families | query types | mismatch pruned | boundary override |
|---|---|---|---|---|---:|---|
| bv_340_exact_extract | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation, Lieben Haloform Reaction, Mannich Reaction | 산화, 탈카복실화, ester/lactone 삽입형 산화 | 2 |  |
| bv_341_exact_extract | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation, Lieben Haloform Reaction, Mannich Reaction | 탈산소화, 탈카복실화 | 10 |  |
| bh_368_exact_extract | Buchwald-Hartwig Cross-Coupling | Buchwald-Hartwig Cross-Coupling | Buchwald-Hartwig Cross-Coupling | 탈산소화 | 0 |  |
| bh_370_exact_extract | Buchwald-Hartwig Cross-Coupling | Buchwald-Hartwig Cross-Coupling | Buchwald-Hartwig Cross-Coupling, Heine Reaction, Danishefsky's Diene Cycloaddition | 커플링, 당/다가산소 재배열 | 0 |  |
| castro_378_exact_extract | Castro-Stephens Coupling | Castro-Stephens Coupling | Castro-Stephens Coupling |  | 0 |  |
| chichibabin_384_exact_extract | Chichibabin Amination Reaction | Chichibabin Amination Reaction | Chichibabin Amination Reaction, Ciamician-Dennstedt Rearrangement |  | 0 |  |
| claisen_387_exact_extract | Claisen Rearrangement | Claisen Rearrangement | Claisen Rearrangement |  | 1 |  |
| benzilic_400_exact_extract | Benzilic Acid Rearrangement | Benzilic Acid Rearrangement | Benzilic Acid Rearrangement, Hajos-Parrish Reaction | 산화, ester/lactone 삽입형 산화 | 1 |  |
| buchner_403_exact_extract | Buchner Reaction | Claisen Condensation / Claisen Reaction | Claisen Condensation / Claisen Reaction, Buchner Reaction, Malonic Ester Synthesis |  | 0 |  |
| aza_claisen_337_exact_extract | Aza-Claisen Rearrangement | Aza-Claisen Rearrangement | Aza-Claisen Rearrangement, Aza-[2,3]-Wittig Rearrangement | 산화 | 0 |  |
| baker_344_exact_extract | Baker-Venkataraman Rearrangement | Baker-Venkataraman Rearrangement | Baker-Venkataraman Rearrangement, Hofmann Rearrangement, Friedel-Crafts Acylation | 산화 | 0 |  |
| balz_351_exact_extract | Balz-Schiemann Reaction | Balz-Schiemann Reaction | Balz-Schiemann Reaction |  | 0 |  |
| bartoli_413_exact_extract | Bartoli Indole Synthesis | Bartoli Indole Synthesis | Bartoli Indole Synthesis, Madelung Indole Synthesis, Fischer Indole Synthesis | 산화 | 0 |  |
| barton_356_exact_extract | Barton Radical Decarboxylation | Barton Radical Decarboxylation | Barton Radical Decarboxylation | 탈산소화, 탈카복실화 | 1 |  |
| brook_416_exact_extract | Brook Rearrangement | Brook Rearrangement | Brook Rearrangement |  | 0 |  |
| chichibabin_386_exact_extract | Chichibabin Amination Reaction | Chichibabin Amination Reaction | Chichibabin Amination Reaction, Minisci Reaction, Friedel-Crafts Alkylation |  | 0 |  |
| claisen_cond_408_exact_extract | Claisen Condensation | Claisen Condensation | Claisen Condensation, Cornforth Rearrangement, Acetoacetic Ester Synthesis | 산화, ester/lactone 삽입형 산화 | 1 |  |
| claisen_389_exact_extract | Claisen Rearrangement | Claisen Rearrangement | Claisen Rearrangement |  | 0 |  |
| aza_claisen_335_exact_extract | Aza-Claisen Rearrangement | Aza-Claisen Rearrangement | Aza-Claisen Rearrangement | 산화 | 0 |  |
| bv_342_exact_extract | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation, Eschenmoser-Tanabe Fragmentation, Bamford-Stevens-Shapiro Olefination | 탈산소화, 탈카복실화 | 12 |  |
| baker_346_exact_extract | Baker-Venkataraman Rearrangement | Baker-Venkataraman Rearrangement | Baker-Venkataraman Rearrangement | 환원 | 1 |  |
| barton_357_exact_extract | Barton Radical Decarboxylation | Barton Radical Decarboxylation | Barton Radical Decarboxylation | 산화, ester/lactone 삽입형 산화 | 1 |  |
| bh_371_exact_extract | Buchwald-Hartwig Cross-Coupling | Buchwald-Hartwig Cross-Coupling | Buchwald-Hartwig Cross-Coupling, Amadori Rearrangement | 커플링, 당/다가산소 재배열 | 0 |  |
| benzilic_402_exact_extract | Benzilic Acid Rearrangement | Claisen-Ireland Rearrangement | Claisen-Ireland Rearrangement, Lieben Haloform Reaction, Cannizzaro Reaction | 탈산소화 | 2 | yes |
| claisen_cond_409_exact_extract | Claisen Condensation | Claisen Condensation | Claisen Condensation, Baker-Venkataraman Rearrangement |  | 1 |  |
| barton_radical_rxn_353_exact_extract | Barton Radical Decarboxylation Reaction | Barton Radical Decarboxylation Reaction | Barton Radical Decarboxylation Reaction, Bamford-Stevens-Shapiro Olefination, Malonic Ester Synthesis |  | 0 |  |
| barton_mccombie_359_boundary_extract | Barton-Mccombie Radical Deoxygenation Reaction | Barton Radical Decarboxylation Reaction | Barton Radical Decarboxylation Reaction, Bamford-Stevens-Shapiro Olefination, Malonic Ester Synthesis |  | 0 | yes |
