# Phase 4 Gate C — Family Merge Audit (task #118)

**Created:** 2026-04-24
**Input:** iterate1 admission snapshot (57 top-1 mismatches, 44 distinct greedy predictors)
**Upstream input:** `reports/phase4/gateB/gate_c_merge_candidates.md` (Category B from Gate B triage)
**Decision scope:** Audit only — no code changes in this task. Produces a merge decision matrix that Phase 4 apply (Gate C execute, a follow-up task) will act on.

---

## 1. Method

A "greedy predictor" is a family that appears as `top1_family` on at least one case where `expected_family` differs. The audit ranks the 15 most-greedy predictors from iterate1 admission, then classifies each into one of four dispositions:

- **MERGE** — predictor is a mechanistic sibling of its victim(s); merging their labels under an umbrella family reclaims top-1 without changing SMARTS behavior.
- **DO NOT MERGE / reagent channel** — predictor and victim(s) map to the same SMILES transform but are mechanistically distinct (different reagents, different stereo selectivity). Already parked under Category A; wait for Phase 5 reagent channel.
- **SMARTS TUNE** — predictor's pull is diffuse (many unrelated victims); merging would collapse too much. Needs further SMARTS guard work, not ontology edit.
- **SINGLETON WATCHLIST** — only 1 victim; insufficient signal for a merge decision. Re-evaluate if the pair recurs after future benchmark changes.

## 2. Greedy predictor table (iterate1 admission, 2026-04-24)

| rank | predictor | count | #victims | top victims | disposition |
|---:|---|---:|---:|---|---|
| 1 | Ley Oxidation | 4 | 4 | Corey-Kim, Dess-Martin, Pfitzner-Moffatt | **DO NOT MERGE / reagent channel** |
| 2 | Aldol Reaction | 4 | 4 | Dakin-West, Mitsunobu, Passerini | **SMARTS TUNE** |
| 3 | Stetter Reaction | 2 | 2 | Aldol, Tishchenko | **SMARTS TUNE** |
| 4 | Schwartz Hydrozirconation | 2 | 2 | Bamford-Stevens-Shapiro, Enyne Metathesis | **DO NOT MERGE / reagent channel** |
| 5 | Pinnick Oxidation | 2 | 2 | HWE, Knoevenagel | **SMARTS TUNE** |
| 6 | Shi Asymmetric Epoxidation | 2 | 2 | Jacobsen-Katsuki, Prilezhaev | **DO NOT MERGE / reagent channel** |
| 7 | Corey-Nicolaou Macrolactonization | 2 | 2 | Keck, Yamaguchi | **MERGE → Macrolactonization (umbrella)** |
| 8 | Malonic Ester Synthesis | 2 | 2 | Michael Addition, Retro-Claisen | **SMARTS TUNE** |
| 9 | Friedel-Crafts Alkylation | 2 | 2 | Sonogashira, Staudinger | **SMARTS TUNE** (diffuse) |
| 10-44 | (1 victim each, 35 families) | 1 ea | 1 ea | varies | **SINGLETON WATCHLIST** |

## 3. Merge decisions

### 3.1 MERGE — Macrolactonization triad
- **Umbrella family:** `Macrolactonization` (new canonical label)
- **Aliased FROM → TO:**
  - `Keck Macrolactonization` → `Macrolactonization`
  - `Yamaguchi Macrolactonization` → `Macrolactonization`
  - `Corey-Nicolaou Macrolactonization` → `Macrolactonization`
- **Rationale:** All three produce the identical ω-hydroxy-acid → lactone transform. Reagent signatures (DCC/DMAP, 2,4,6-trichlorobenzoyl chloride, 2-thiopyridyl ester) are the only discriminators and are invisible in reaction SMILES. Pedagogically treated as one family in synthesis textbooks.
- **Expected impact:** 2 admission cases recovered (Keck 1015, Yamaguchi 1399) + Corey-Nicolaou cases no longer counted as wrong-family when they were expected. Zero regression risk on non-macrolactonization cases.
- **Cases recovered:** `adm_keck_macrolactonization_1015`, `adm_yamaguchi_macrolactonization_1399`

### 3.2 MERGE — Ullmann biaryl-ether/amine into Ullmann generic
- **Umbrella family:** `Ullmann Reaction / Coupling / Biaryl Synthesis`
- **Aliased FROM → TO:**
  - `Ullmann Biaryl Ether and Biaryl Amine Synthesis / Condensation` → `Ullmann Reaction / Coupling / Biaryl Synthesis`
- **Rationale:** Same Cu-mediated mechanism; heteroatom nucleophile (phenol/aniline) vs carbon nucleophile is a substrate difference, not a mechanism difference. Gate B's G12_ullmann_generic guard already treats them as equivalents at the evidence layer.
- **Expected impact:** 1 admission case recovered (1348).
- **Cases recovered:** `adm_ullmann_biaryl_ether_and_biaryl_amine_synthesis_/_condensation_1348`

### 3.3 DO NOT MERGE — mechanistically distinct families

| Cluster | Members | Why kept distinct |
|---|---|---|
| Alcohol oxidation (OH→CHO) | Ley, Corey-Kim, Dess-Martin, Pfitzner-Moffatt, Swern | Different oxidants with different functional-group tolerance (TPAP vs DMSO activation methods). Already in Category A. |
| Styrene epoxidation | Shi, Jacobsen-Katsuki, Prilezhaev | Different catalysts (fructose ketone / Mn-salen / mCPBA peracid). Shi and Jacobsen give opposite enantiomers; Prilezhaev is achiral. Already in Category A. |
| Schwartz Hydrozirconation | (no sister) | Zr-H reagent is mechanistically unique; its confusion with BSS/enyne is a SMARTS issue, not an ontology issue. |

### 3.4 SMARTS TUNE — defer to future Gate B batches

Aldol (rank 2), Stetter (3), Pinnick (5), Malonic Ester (8), Friedel-Crafts (9): each pulls cases from mechanistically unrelated victims (e.g., Aldol→Mitsunobu is motif spill). These need tighter anti-greed guards in `app/evidence_search.py`, **not** a merge.

## 4. Expected Gate C impact

| Metric | iterate1 baseline | Gate C apply (projected) | Δ |
|---|---:|---:|---:|
| adm_top1 | 202/259 = 0.7799 | ≈ 205/259 = 0.7915 | +3 cases (+0.0116) |
| confused_pairs | 57 | ≈ 54 | −3 |

The two merges together recover 3 admission cases (Keck 1015 + Yamaguchi 1399 + Ullmann 1348). No change to coverage/broad gates since these are symbol-level aliases, not SMARTS-level changes.

## 5. Apply path (deferred to follow-up task)

Gate C apply would edit `app/final_state_verifier.py` `alias_overrides`:

```python
alias_overrides = {
    ...existing...
    "Keck Macrolactonization":           "Macrolactonization",
    "Yamaguchi Macrolactonization":      "Macrolactonization",
    "Corey-Nicolaou Macrolactonization": "Macrolactonization",
    "Ullmann Biaryl Ether and Biaryl Amine Synthesis / Condensation":
        "Ullmann Reaction / Coupling / Biaryl Synthesis",
}
```

Then rebuild benchmark JSONs (the `expected_family` entries for these case_ids must be rewritten to use the umbrella label) and re-run the 3-layer benchmark. `scripts/rebuild_benchmarks.py` already handles alias canonicalization.

## 6. Watchlist (no action now)

The 35 singleton greedy predictors represent 1 case each — re-audit after any future benchmark change. If any reappears with ≥3 victims, escalate to new SMARTS guard or merge review.

---

**Audit verdict:** 2 merges recommended (macrolactonization triad + Ullmann biaryl-ether). Expected recovery: 3 admission cases. Zero regression risk on coverage or broad layers. Apply is a separate task (not in #118 scope).
