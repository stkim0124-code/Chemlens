# Task #123 — 4 no_confident_hit_cases residual analysis

**Source snapshot:** `reports/phase4/gateB/iterate1_20260424/coverage/corpus_coverage_results.json`
**Date:** 2026-04-24
**Definition:** A coverage case is flagged `no_confident_hit` when no family score in top-5 passes the confidence threshold (≈0.5). The count has been stuck at 4 since Phase 3c-b — this task answers whether those 4 are fixable, and if so by what mechanism.

---

## 1. Per-case analysis

### Case 1 — `cov_barton_nitrite_ester_reaction_1480`
- **expected:** Barton Nitrite Ester Reaction
- **top1:** Barton Nitrite Ester Reaction (score 0.388)
- **hit_at_1:** True
- **Verdict:** **False positive flag.** The top-1 is already correct; the case is flagged only because the absolute score (0.388) falls under the confidence threshold. This is a scoring-calibration artifact, not a family-prediction defect.
- **Action:** None for SMARTS/ontology. Future work could recalibrate the confidence threshold per-family based on score distributions, but this is out of Phase 4 scope.

### Case 2 — `cov_barton_radical_decarboxylation_reaction_357`
- **expected:** Barton Radical Decarboxylation Reaction
- **top1:** Diels-Alder Cycloaddition (0.354)
- **top5:** Diels-Alder | Danheiser Benzannulation | Dieckmann Condensation | Stobbe | Mitsunobu
- **hit_at_1/3/5:** False / False / False — **true miss**
- **Verdict:** Structural false positive. Barton Radical Decarboxylation (RCO2-H → R-H via Barton ester + Bu3SnH) leaves no persistent motif detectable from product SMILES alone. Diels-Alder wins because the product ring system happens to match a cycloadduct SMARTS.
- **Action:** Fixable only via (a) reagent channel (Phase 5: detect Barton ester / NHPI ester reagents) or (b) DB depth (more Barton decarboxylation exemplars to outweigh the Diels-Alder generic pull). Flagged for Phase 5 reagent channel scoping (task #120).

### Case 3 — `cov_barton-mccombie_radical_deoxygenation_reaction_796`
- **expected:** Barton-McCombie Radical Deoxygenation Reaction
- **top1:** Baker-Venkataraman Rearrangement (0.413)
- **top3:** Baker-Venkataraman | Paternò-Büchi | Barton-McCombie (0.049)
- **hit_at_1/3/5:** False / True / True
- **Verdict:** Reagent-channel-invisible. Barton-McCombie is C-OH → C-H via xanthate + Bu3SnH. The xanthate intermediate is not captured in the reaction SMILES (product is R-H with nothing distinctive). Baker-Venkataraman wins because the substrate o-hydroxyacetophenone motif appears in both.
- **Action:** Phase 5 reagent channel — xanthate SMARTS already exists (Gate B G13 Chugaev), but it fires on the intermediate, not in this benchmark's reactant/product SMILES. Detecting R-SC(=S)-O-R requires a different reaction representation. Flagged for task #120.

### Case 4 — `cov_buchwald-hartwig_cross-coupling_368`
- **expected:** Buchwald-Hartwig Cross-Coupling
- **top1:** Simmons-Smith Reaction (0.437)
- **top3:** Simmons-Smith | Ullmann Biaryl Ether/Amine | Buchwald-Hartwig (0.037)
- **hit_at_1/3/5:** False / True / True
- **Verdict:** **Anomaly worth a SMARTS audit.** Buchwald-Hartwig is ArX + HNR2 → ArNR2 (Pd-catalyzed). Simmons-Smith is alkene + CH2I2/Zn → cyclopropane. These should NOT collide structurally. The 0.437 Simmons-Smith score suggests an overly-broad Simmons-Smith SMARTS pattern or cyclopropane-motif false positive. Note: Ullmann biaryl-ether at top2 is expected given Gate C will merge it with Ullmann generic — after Gate C apply this case may move from Buchwald top3 to Ullmann top2, which is still wrong.
- **Action:** Add to Gate B batch — audit Simmons-Smith SMARTS for over-breadth. Small, localized fix. Create a follow-up task.

## 2. Summary verdict

| Case | Kind | Fixable in Phase 4? | Mechanism |
|---|---|---|---|
| Barton Nitrite 1480 | Confidence threshold artifact | No (out of scope) | Score calibration |
| Barton Radical Decarb 357 | True miss, reagent-invisible | No | Phase 5 reagent channel |
| Barton-McCombie 796 | True miss, reagent-invisible | No | Phase 5 reagent channel |
| Buchwald-Hartwig 368 | Anomalous Simmons-Smith pull | **Yes — SMARTS audit** | Gate B follow-on |

**Bottom line:** 3 of 4 cannot be fixed within Phase 4 (need Phase 5 reagent channel or confidence recalibration). 1 of 4 (Buchwald-Hartwig 368) is a Simmons-Smith SMARTS over-breadth — actionable immediately as a Gate B batch item.

## 3. Recommended follow-up

Create a small task: audit Simmons-Smith SMARTS guard in `_family_delta_adjustment`; if over-broad on non-alkene substrates, add a cyclopropane-product requirement. Expected impact: 1 coverage case recovered (Buchwald-Hartwig 368).

Remaining 3 no_confident_hit cases stay parked until Phase 5 reagent channel (task #120) or future score-calibration work.
