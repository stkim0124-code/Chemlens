# CHEMLENS benchmark v4 (expanded) report

## Summary

- total_cases: 50
- reaction_query_cases: 33
- adversarial_noisy_cases: 5
- adversarial_meaningful_cases: 12
- top1_accuracy (reaction): 0.3636
- top3_accuracy (reaction): 0.4848
- disallow_top3_violations: 0
- unique_families: 24
- boundary_cases_with_acceptance_override: 2

## Adversarial — Noisy query robustness

Pass rate: 0/5

| case_id | smiles | no_confident_hit | top1 | top1_score | pass |
|---|---|:---:|---|---:|:---:|
| noisy_acetic_acid | `Plain acetic acid: too generic to pin a single family. Expected no confident hit.` | ✅ | Knorr Pyrrole Synthesis | 0.42 | ❌ |
| noisy_ethanol | `Plain ethanol: expected no confident hit.` | ❌ | Malonic Ester Synthesis | 0.756 | ❌ |
| noisy_benzene | `Plain benzene: expected no confident hit (common aromatic).` | ❌ | Buchner Reaction | 0.605 | ❌ |
| noisy_acetone | `Plain acetone: expected no confident hit.` | ✅ | Acetoacetic Ester Synthesis | 0.412 | ❌ |
| noisy_toluene | `Plain toluene: expected no confident hit.` | ❌ | Aza-Wittig Reaction | 1.509 | ❌ |

## Adversarial — Meaningful single SMILES

Top3 pass rate: 11/12

| case_id | expected | top1 | top3 families | top3 scores | pass |
|---|---|---|---|---|:---:|
| meaningful_pentanoic_acid | Barton Radical Decarboxylation Reaction | Barton Radical Decarboxylation Reaction | Barton Radical Decarboxylation Reaction, Malonic Ester Synthesis, Barton-Mccombie Radical Deoxygenation Reaction | 0.891 | 0.431 | 0.283 | ✅ |
| meaningful_cyclohexanone | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation/Rearrangement | Baeyer-Villiger Oxidation/Rearrangement | 0.454 | ✅ |
| meaningful_aniline_aryl_bromide | Buchwald-Hartwig Cross-Coupling | Benzoin and Retro-Benzoin Condensation | Benzoin and Retro-Benzoin Condensation, Aza-Wittig Reaction, Lossen Rearrangement | 3.491 | 3.017 | 2.616 | ❌ |
| arbuzov_new_benzyl_product | Arbuzov Reaction (Michaelis-Arbuzov Reaction) | Arbuzov Reaction (Michaelis-Arbuzov Reaction) | Arbuzov Reaction (Michaelis-Arbuzov Reaction), Clemmensen Reduction, Buchner Reaction | 1.572 | 0.789 | 0.305 | ✅ |
| barbier_new_product | Barbier Coupling Reaction | Acetoacetic Ester Synthesis | Acetoacetic Ester Synthesis, Barbier Coupling Reaction, Cannizzaro Reaction | 0.626 | 0.212 | 0.126 | ✅ |
| combes_new_product | Combes Quinoline Synthesis | Benzoin and Retro-Benzoin Condensation | Benzoin and Retro-Benzoin Condensation, Ciamician-Dennstedt Rearrangement, Combes Quinoline Synthesis | 0.988 | 0.783 | 0.48 | ✅ |
| knorr_pyrrole_new_product | Knorr Pyrrole Synthesis | Buchner Reaction | Buchner Reaction, Knorr Pyrrole Synthesis, Acyloin Condensation | 1.256 | 0.8 | 0.68 | ✅ |
| luche_new_product | Luche Reduction | Luche Reduction | Luche Reduction | 0.84 | ✅ |
| madelung_new_product | Madelung Indole Synthesis | Madelung Indole Synthesis | Madelung Indole Synthesis, Ciamician-Dennstedt Rearrangement, Chichibabin Amination Reaction | 1.2 | 0.608 | 0.335 | ✅ |
| malonic_ester_new_product | Malonic Ester Synthesis | Malonic Ester Synthesis | Malonic Ester Synthesis, Buchner Reaction, Cannizzaro Reaction | 1.069 | 0.316 | 0.233 | ✅ |
| boundary_pentanoic_over_matching | None | Barton Radical Decarboxylation Reaction | Barton Radical Decarboxylation Reaction, Malonic Ester Synthesis, Barton-Mccombie Radical Deoxygenation Reaction | 0.891 | 0.431 | 0.283 | ✅ |
| boundary_chloro_benzyl_chloride | None | Arbuzov Reaction (Michaelis-Arbuzov Reaction) | Arbuzov Reaction (Michaelis-Arbuzov Reaction), Aza-Wittig Reaction, Clemmensen Reduction | 1.572 | 1.015 | 0.868 | ✅ |

## Family coverage (reaction queries)

| family | cases | top1 | top3 |
|---|---:|---:|---:|
| Arbuzov Reaction (Michaelis-Arbuzov Reaction) | 1 | 1/1 | 1/1 |
| Aza-Claisen Rearrangement | 2 | 1/2 | 1/2 |
| Baeyer-Villiger Oxidation | 3 | 1/3 | 2/3 |
| Baeyer-Villiger Oxidation/Rearrangement | 1 | 1/1 | 1/1 |
| Baker-Venkataraman Rearrangement | 2 | 0/2 | 0/2 |
| Balz-Schiemann Reaction | 1 | 0/1 | 0/1 |
| Barbier Coupling Reaction | 1 | 1/1 | 1/1 |
| Bartoli Indole Synthesis | 1 | 0/1 | 0/1 |
| Barton-Mccombie Radical Deoxygenation Reaction | 1 | 0/1 | 0/1 |
| Benzilic Acid Rearrangement | 1 | 0/1 | 0/1 |
| Brook Rearrangement | 1 | 0/1 | 0/1 |
| Buchner Reaction | 1 | 0/1 | 1/1 |
| Buchwald-Hartwig Cross-Coupling | 3 | 0/3 | 0/3 |
| Castro-Stephens Coupling | 1 | 0/1 | 0/1 |
| Chichibabin Amination Reaction | 2 | 0/2 | 1/2 |
| Claisen Condensation | 2 | 0/2 | 0/2 |
| Claisen Rearrangement | 2 | 1/2 | 1/2 |
| Combes Quinoline Synthesis | 1 | 1/1 | 1/1 |
| Knorr Pyrrole Synthesis | 1 | 0/1 | 1/1 |
| Lieben Haloform Reaction | 1 | 1/1 | 1/1 |
| Lossen Rearrangement | 1 | 1/1 | 1/1 |
| Luche Reduction | 1 | 1/1 | 1/1 |
| Madelung Indole Synthesis | 1 | 1/1 | 1/1 |
| Malonic Ester Synthesis | 1 | 1/1 | 1/1 |

## Case table (reaction queries)

| case_id | expected | top1 | top3 families | query types | mismatch pruned | boundary override |
|---|---|---|---|---|---:|---|
| bv_340_exact_extract | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation, Lieben Haloform Reaction, Clemmensen Reduction | 산화, 탈카복실화, ester/lactone 삽입형 산화 | 2 |  |
| bv_341_exact_extract | Baeyer-Villiger Oxidation | Knorr Pyrrole Synthesis | Knorr Pyrrole Synthesis, Baeyer-Villiger Oxidation, Barbier Coupling Reaction | 산화, ester/lactone 삽입형 산화 | 2 |  |
| bh_368_exact_extract | Buchwald-Hartwig Cross-Coupling | Aza-Wittig Reaction | Aza-Wittig Reaction, Chichibabin Amination Reaction, Benzoin and Retro-Benzoin Condensation | 커플링 | 2 |  |
| bh_370_exact_extract | Buchwald-Hartwig Cross-Coupling | Aza-Wittig Reaction | Aza-Wittig Reaction, Benzoin and Retro-Benzoin Condensation, Chichibabin Amination Reaction | 커플링 | 2 |  |
| castro_378_exact_extract | Castro-Stephens Coupling | Aza-Wittig Reaction | Aza-Wittig Reaction, Lossen Rearrangement, Clemmensen Reduction |  | 1 |  |
| chichibabin_384_exact_extract | Chichibabin Amination Reaction | Lossen Rearrangement | Lossen Rearrangement, Baker-Venkataraman Rearrangement, Cannizzaro Reaction |  | 1 |  |
| claisen_387_exact_extract | Claisen Rearrangement | Aza-Cope Rearrangement | Aza-Cope Rearrangement, Aza-Wittig Reaction, Lieben Haloform Reaction | 산화 | 0 |  |
| benzilic_400_exact_extract | Benzilic Acid Rearrangement | Lossen Rearrangement | Lossen Rearrangement, Lieben Haloform Reaction, Aza-Wittig Reaction | 환원 | 9 |  |
| buchner_403_exact_extract | Buchner Reaction | Acetoacetic Ester Synthesis | Acetoacetic Ester Synthesis, Malonic Ester Synthesis, Buchner Reaction |  | 2 |  |
| aza_claisen_337_exact_extract | Aza-Claisen Rearrangement | Aza-[2,3]-Wittig Rearrangement | Aza-[2,3]-Wittig Rearrangement, Aza-Cope Rearrangement, Barbier Coupling Reaction |  | 0 |  |
| baker_344_exact_extract | Baker-Venkataraman Rearrangement | Lieben Haloform Reaction | Lieben Haloform Reaction, Benzoin and Retro-Benzoin Condensation, Lossen Rearrangement |  | 0 |  |
| balz_351_exact_extract | Balz-Schiemann Reaction | Aza-Wittig Reaction | Aza-Wittig Reaction, Lossen Rearrangement, Clemmensen Reduction |  | 1 |  |
| bartoli_413_exact_extract | Bartoli Indole Synthesis | Madelung Indole Synthesis | Madelung Indole Synthesis, Claisen Rearrangement, Chichibabin Amination Reaction |  | 1 |  |
| brook_416_exact_extract | Brook Rearrangement | Balz-Schiemann Reaction | Balz-Schiemann Reaction, Alkyne Metathesis, Aza-Wittig Reaction | 탈산소화 | 2 |  |
| chichibabin_386_exact_extract | Chichibabin Amination Reaction | Lossen Rearrangement | Lossen Rearrangement, Chichibabin Amination Reaction, Cannizzaro Reaction |  | 1 |  |
| claisen_cond_408_exact_extract | Claisen Condensation | Malonic Ester Synthesis | Malonic Ester Synthesis, Acyloin Condensation, Acetoacetic Ester Synthesis |  | 1 |  |
| claisen_389_exact_extract | Claisen Rearrangement | Claisen Rearrangement | Claisen Rearrangement, Chichibabin Amination Reaction, Baker-Venkataraman Rearrangement |  | 1 |  |
| aza_claisen_335_exact_extract | Aza-Claisen Rearrangement | Aza-Claisen Rearrangement | Aza-Claisen Rearrangement | 산화 | 0 |  |
| bv_342_exact_extract | Baeyer-Villiger Oxidation | Baeyer-Villiger Oxidation/Rearrangement | Baeyer-Villiger Oxidation/Rearrangement, Acetoacetic Ester Synthesis, Lieben Haloform Reaction | 산화, ester/lactone 삽입형 산화 | 2 |  |
| baker_346_exact_extract | Baker-Venkataraman Rearrangement | Clemmensen Reduction | Clemmensen Reduction, Lieben Haloform Reaction, Lossen Rearrangement |  | 1 |  |
| bh_371_exact_extract | Buchwald-Hartwig Cross-Coupling | Chichibabin Amination Reaction | Chichibabin Amination Reaction, Claisen Rearrangement, Malonic Ester Synthesis | 커플링 | 2 |  |
| claisen_cond_409_exact_extract | Claisen Condensation | Acyloin Condensation | Acyloin Condensation, Malonic Ester Synthesis, Acetoacetic Ester Synthesis |  | 1 |  |
| barton_mccombie_359_boundary_extract | Barton-Mccombie Radical Deoxygenation Reaction | Cannizzaro Reaction | Cannizzaro Reaction, Acetoacetic Ester Synthesis, Malonic Ester Synthesis |  | 0 | yes |
| arbuzov_new_reaction | Arbuzov Reaction (Michaelis-Arbuzov Reaction) | Arbuzov Reaction (Michaelis-Arbuzov Reaction) | Arbuzov Reaction (Michaelis-Arbuzov Reaction), Clemmensen Reduction, Aza-Wittig Reaction |  | 1 |  |
| bv_rearr_new_reaction | Baeyer-Villiger Oxidation/Rearrangement | Baeyer-Villiger Oxidation/Rearrangement | Baeyer-Villiger Oxidation/Rearrangement, Acetoacetic Ester Synthesis, Lieben Haloform Reaction | 산화, ester/lactone 삽입형 산화 | 2 | yes |
| barbier_new_reaction | Barbier Coupling Reaction | Barbier Coupling Reaction | Barbier Coupling Reaction, Acetoacetic Ester Synthesis, Clemmensen Reduction | 환원 | 1 |  |
| combes_new_reaction | Combes Quinoline Synthesis | Combes Quinoline Synthesis | Combes Quinoline Synthesis, Benzoin and Retro-Benzoin Condensation, Acetoacetic Ester Synthesis | 다성분 축합 | 2 |  |
| knorr_pyrrole_new_reaction | Knorr Pyrrole Synthesis | Acetoacetic Ester Synthesis | Acetoacetic Ester Synthesis, Knorr Pyrrole Synthesis, Combes Quinoline Synthesis | 탈산소화, 탈카복실화, 다성분 축합, 당/다가산소 재배열 | 3 |  |
| lieben_haloform_new_reaction | Lieben Haloform Reaction | Lieben Haloform Reaction | Lieben Haloform Reaction, Lossen Rearrangement, Clemmensen Reduction |  | 1 |  |
| lossen_new_reaction | Lossen Rearrangement | Lossen Rearrangement | Lossen Rearrangement, Lieben Haloform Reaction, Clemmensen Reduction | 환원 | 6 |  |
| luche_new_reaction | Luche Reduction | Luche Reduction | Luche Reduction, Cannizzaro Reaction, Lieben Haloform Reaction | 환원 | 0 |  |
| madelung_new_reaction | Madelung Indole Synthesis | Madelung Indole Synthesis | Madelung Indole Synthesis, Lieben Haloform Reaction, Clemmensen Reduction |  | 1 |  |
| malonic_ester_new_reaction | Malonic Ester Synthesis | Malonic Ester Synthesis | Malonic Ester Synthesis, Acetoacetic Ester Synthesis, Acyloin Condensation | 환원 | 1 |  |
