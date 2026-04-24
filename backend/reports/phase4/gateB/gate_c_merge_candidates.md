# Phase 4 Gate B — Category B Salvage: Gate C Merge-Audit Candidates

**Created:** 2026-04-24
**Cohort tag:** `gate_c_merge_candidates`
**Consumer:** Phase 4 Gate C (task #118 — Family merge audit of 11 greedy predictors)
**Philosophy:** These confused pairs are NOT prediction errors in the structural sense — they are ambiguities introduced by ontology granularity. The remediation is a curated family-merge or canonical-synonym decision at the ontology layer (`final_state_verifier.py` `alias_overrides`), not more SMARTS guards.

---

## Cluster 1 — Macrolactonization triad (2 cases)

| case_id | expected_family | predicted_top1 | reaction_smiles |
|---|---|---|---|
| `adm_keck_macrolactonization_1015` | Keck Macrolactonization | Corey-Nicolaou Macrolactonization | `O=C(O)CCCCCCCCCCO>>O=C1CCCCCCCCCCO1` |
| `adm_yamaguchi_macrolactonization_1399` | Yamaguchi Macrolactonization | Corey-Nicolaou Macrolactonization | `O=C(O)CCCCCCCCCCCO>>O=C1CCCCCCCCCCCO1` |

**Observation.** All three macrolactonization methods (Keck/Yamaguchi/Corey-Nicolaou) produce the identical ω-hydroxy-carboxylic-acid → lactone transformation. SMILES-only input cannot distinguish them — reagent signatures (2-thiopyridyl ester for Corey-Nicolaou, 2,4,6-trichlorobenzoyl chloride for Yamaguchi, DCC/DMAP for Keck) are the only discriminators.

**Recommendation.** Gate C should decide between two options:

1. **Merge under umbrella "Macrolactonization (Keck/Yamaguchi/Corey-Nicolaou)"** — treats the three as one family for admission purposes. `alias_overrides` would canonicalize all three to a shared label.
2. **Keep split, defer to Phase 5 reagent channel** — no Gate B action needed; cases stay under `deferred_reagent_channel` semantics but are NOT listed in Category A because structurally they're siblings, not reagent-distinguished-only.

**Preferred path:** Option 1 (merge). Rationale: the three methods are pedagogically "the same reaction with different activators," and merging aligns with how synthesis textbooks treat them. Benchmark loss is recovered at top-1 the moment the merge ships.

---

## Cluster 2 — Ullmann biaryl-ether vs Ullmann generic (1 case)

| case_id | expected_family | predicted_top1 | reaction_smiles |
|---|---|---|---|
| `adm_ullmann_biaryl_ether_and_biaryl_amine_synthesis_/_condensation_1348` | Ullmann Biaryl Ether and Biaryl Amine Synthesis / Condensation | Ullmann Reaction / Coupling / Biaryl Synthesis | `Ic1ccccc1.Oc1ccccc1>>c1ccc(Oc2ccccc2)cc1` |

**Observation.** The benchmark's "expected" label is the C–O / C–N variant of Ullmann; the predictor's top-1 is the umbrella "Ullmann Reaction / Coupling / Biaryl Synthesis." These are the same mechanism (Cu-mediated aryl nucleophilic substitution) with the phenol vs arylboronic-acid partner being the only discriminator.

**Recommendation.** Gate C merges "Ullmann Biaryl Ether and Biaryl Amine Synthesis" INTO "Ullmann Reaction / Coupling / Biaryl Synthesis" (or vice versa). Add bi-directional `alias_overrides` entry.

**Preferred path:** Collapse the "biaryl ether/amine" specialization into the parent "Ullmann Reaction" family. The phenol/aniline coupling variant is just Ullmann with a heteroatom nucleophile — not a distinct mechanism.

---

## Cluster 3 (observational, NOT for merge — flagged for Gate C awareness only)

Jacobsen-Katsuki / Shi / Prilezhaev epoxidations share the same styrene → styrene-oxide transformation in the benchmark SMILES, but these are **three mechanistically distinct methods** (Mn-salen / fructose-ketone Dioxirane / peracid mCPBA). **Do NOT merge.** These belong in Category A (reagent channel), already parked under `deferred_reagent_channel.json`. Flagged here so Gate C auditors don't accidentally propose a merge during the 11-greedy-predictor sweep.

---

## Summary

| Cluster | Action | Cases moved out of "confused_pairs" on merge |
|---|---|---|
| Macrolactonization triad | Merge under umbrella | 2 |
| Ullmann biaryl-ether → generic Ullmann | Merge into parent | 1 |
| Jacobsen/Shi/Prilezhaev epoxidation | Do NOT merge (reagent channel) | 0 |
| **Expected confused_pairs reduction from Gate C** | | **≥3** |

**Gate C task #118 should incorporate these three clusters into its 11-greedy-predictor sweep.**
