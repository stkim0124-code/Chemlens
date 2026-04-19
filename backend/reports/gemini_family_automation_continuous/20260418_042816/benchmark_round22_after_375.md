# CHEMLENS small benchmark v3 report

## Summary

- total_cases: 35
- reaction_query_cases: 27
- adversarial_noisy_cases: 5
- adversarial_meaningful_cases: 3
- top1_accuracy (reaction): 1.0000
- top3_accuracy (reaction): 1.0000
- disallow_top3_violations: 0
- unique_families: 16
- boundary_cases_with_acceptance_override: 2

## Adversarial — Noisy query robustness

Pass rate: 3/5

| case_id | smiles | no_confident_hit | top1 | top1_score | pass |
|---|---|:---:|---|---:|:---:|
| noisy_acetic_acid | `Acetic acid` | ✅ | Cannizzaro Reaction | 0.35 | ✅ |
| noisy_ethanol | `Ethanol` | ✅ | Cannizzaro Reaction | 0.305 | ✅ |
| noisy_benzene | `Benzene` | ❌ | Buchner Reaction | 0.605 | ❌ |
| noisy_acetone | `Acetone` | ✅ | Acetoacetic Ester Synthesis | 0.412 | ✅ |
| noisy_toluene | `Toluene` | ❌ | Aza-Wittig Reaction | 1.509 | ❌ |

## Adversarial — Meaningful single SMILES

Top3 pass rate: 3/3

| case_id | expected | top1 | top3 families | top3 scores | pass |
|---|---|---|---|---|:---:|
| meaningful_pentanoic_acid | Barton Radical Decarboxylation Reaction | Barton Radical Decarboxylation Reaction | Barton Radical Decarboxylation Reaction, Barton-Mccombie Radical Deoxygenation Reaction, Bamford-Stevens-Shapiro Olefination | 0.891 | 0.283 | 0.144 | ✅ |
| meaningful_cyclohexanone | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation/Rearrangement | Baeyer-Villiger Oxidation/Rearrangement | 0.47 | ✅ |
| meaningful_aniline_aryl_bromide | Buchwald-Hartwig Cross-Coupling | Buchwald-Hartwig Cross-Coupling | Buchwald-Hartwig Cross-Coupling, Combes Quinoline Synthesis, Arbuzov Reaction (Michaelis-Arbuzov Reaction) | 0.419 | 0.177 | 0.125 | ✅ |

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
| Buchner Reaction | 1 | 1/1 | 1/1 |
| Buchwald-Hartwig Cross-Coupling | 3 | 3/3 | 3/3 |
| Castro-Stephens Coupling | 1 | 1/1 | 1/1 |
| Chichibabin Amination Reaction | 2 | 2/2 | 2/2 |
| Claisen Condensation | 2 | 2/2 | 2/2 |
| Claisen Rearrangement | 2 | 2/2 | 2/2 |

## Case table (reaction queries)

| case_id | expected | top1 | top3 families | query types | mismatch pruned | boundary override |
|---|---|---|---|---|---:|---|
| bv_340_exact_extract | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation, Clemmensen Reduction | 산화, 탈카복실화, ester/lactone 삽입형 산화 | 2 |  |
| bv_341_exact_extract | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation, Clemmensen Reduction | 탈산소화, 탈카복실화 | 5 |  |
| bh_368_exact_extract | Buchwald-Hartwig Cross-Coupling | Buchwald-Hartwig Cross-Coupling | Buchwald-Hartwig Cross-Coupling | 탈산소화 | 0 |  |
| bh_370_exact_extract | Buchwald-Hartwig Cross-Coupling | Buchwald-Hartwig Cross-Coupling | Buchwald-Hartwig Cross-Coupling | 커플링, 당/다가산소 재배열 | 0 |  |
| castro_378_exact_extract | Castro-Stephens Coupling | Castro-Stephens Coupling | Castro-Stephens Coupling |  | 0 |  |
| chichibabin_384_exact_extract | Chichibabin Amination Reaction | Chichibabin Amination Reaction | Chichibabin Amination Reaction, Ciamician-Dennstedt Rearrangement |  | 0 |  |
| claisen_387_exact_extract | Claisen Rearrangement | Claisen Rearrangement | Claisen Rearrangement |  | 1 |  |
| benzilic_400_exact_extract | Benzilic Acid Rearrangement | Benzilic Acid Rearrangement | Benzilic Acid Rearrangement | 산화, ester/lactone 삽입형 산화 | 1 |  |
| buchner_403_exact_extract | Buchner Reaction | Buchner Reaction | Buchner Reaction, Acetoacetic Ester Synthesis, Clemmensen Reduction |  | 2 |  |
| aza_claisen_337_exact_extract | Aza-Claisen Rearrangement | Aza-Claisen Rearrangement | Aza-Claisen Rearrangement, Aza-[2,3]-Wittig Rearrangement | 산화 | 0 |  |
| baker_344_exact_extract | Baker-Venkataraman Rearrangement | Baker-Venkataraman Rearrangement | Baker-Venkataraman Rearrangement, Clemmensen Reduction, Arbuzov Reaction | 산화 | 0 |  |
| balz_351_exact_extract | Balz-Schiemann Reaction | Balz-Schiemann Reaction | Balz-Schiemann Reaction |  | 0 |  |
| bartoli_413_exact_extract | Bartoli Indole Synthesis | Bartoli Indole Synthesis | Bartoli Indole Synthesis | 산화 | 0 |  |
| barton_356_exact_extract | Barton Radical Decarboxylation | Barton Radical Decarboxylation | Barton Radical Decarboxylation | 탈산소화, 탈카복실화 | 1 |  |
| brook_416_exact_extract | Brook Rearrangement | Brook Rearrangement | Brook Rearrangement |  | 0 |  |
| chichibabin_386_exact_extract | Chichibabin Amination Reaction | Chichibabin Amination Reaction | Chichibabin Amination Reaction, Kahne Glycosidation |  | 0 |  |
| claisen_cond_408_exact_extract | Claisen Condensation | Claisen Condensation | Claisen Condensation, Acetoacetic Ester Synthesis, Acyloin Condensation | 산화, ester/lactone 삽입형 산화 | 1 |  |
| claisen_389_exact_extract | Claisen Rearrangement | Claisen Rearrangement | Claisen Rearrangement |  | 0 |  |
| aza_claisen_335_exact_extract | Aza-Claisen Rearrangement | Aza-Claisen Rearrangement | Aza-Claisen Rearrangement | 산화 | 0 |  |
| bv_342_exact_extract | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation, Bamford-Stevens-Shapiro Olefination, Chichibabin Amination Reaction | 탈산소화, 탈카복실화 | 6 |  |
| baker_346_exact_extract | Baker-Venkataraman Rearrangement | Baker-Venkataraman Rearrangement | Baker-Venkataraman Rearrangement | 환원 | 1 |  |
| barton_357_exact_extract | Barton Radical Decarboxylation | Barton Radical Decarboxylation | Barton Radical Decarboxylation | 산화, ester/lactone 삽입형 산화 | 1 |  |
| bh_371_exact_extract | Buchwald-Hartwig Cross-Coupling | Buchwald-Hartwig Cross-Coupling | Buchwald-Hartwig Cross-Coupling, Amadori Rearrangement | 커플링, 당/다가산소 재배열 | 0 |  |
| benzilic_402_exact_extract | Benzilic Acid Rearrangement | Claisen-Ireland Rearrangement | Claisen-Ireland Rearrangement, Cannizzaro Reaction, Benzilic Acid Rearrangement | 탈산소화 | 2 | yes |
| claisen_cond_409_exact_extract | Claisen Condensation | Claisen Condensation | Claisen Condensation, Baker-Venkataraman Rearrangement |  | 1 |  |
| barton_radical_rxn_353_exact_extract | Barton Radical Decarboxylation Reaction | Barton Radical Decarboxylation Reaction | Barton Radical Decarboxylation Reaction, Bamford-Stevens-Shapiro Olefination, Barton-Mccombie Radical Deoxygenation Reaction |  | 0 |  |
| barton_mccombie_359_boundary_extract | Barton-Mccombie Radical Deoxygenation Reaction | Barton Radical Decarboxylation Reaction | Barton Radical Decarboxylation Reaction, Bamford-Stevens-Shapiro Olefination, Barton-Mccombie Radical Deoxygenation Reaction |  | 0 | yes |
