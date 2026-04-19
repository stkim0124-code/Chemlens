# CHEMLENS Canonical DB Gap Report

- Canonical DB: `C:\chemlens\backend\app\labint.db`
- queryable / tier1 / tier2 / tier3: **443 / 357 / 86 / 1373**
- Manual families covered by queryable evidence: **32 / 251**
- reaction_cards with any direct SMILES: **46 / 11371**
- Benchmark top1/top3: **27/27**, **27/27**

## Highest-priority uncovered families

- Alder (Ene) Reaction (Hydro-Allyl Addition) — pages 2
- Aldol Reaction — pages 2
- Alkene (Olefin) Metathesis — pages 2
- Amadori Reaction / Rearrangement — pages 2
- Appendix — pages 2
- Arbuzov Reaction (Michaelis-Arbuzov Reaction) — pages 2
- Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement) — pages 2
- Baeyer-Villiger Oxidation/Rearrangement — pages 2
- Baldwin's Rules / Guidelines for Ring-Closing Reactions — pages 2
- Barbier Coupling Reaction — pages 2
- Barton-McCombie Radical Deoxygenation Reaction — pages 2
- Baylis-Hillman Reaction — pages 2
- Birch Reduction — pages 2
- Carroll Rearrangement — pages 2
- Claisen Condensation / Claisen Reaction — pages 2
- Combes Quinoline Synthesis — pages 2
- Cope Elimination / Cope Reaction — pages 2
- Cope Rearrangement — pages 2
- Corey-Bakshi-Shibata Reduction (CBS Reduction) — pages 2
- Corey-Chaykovsky Epoxidation and Cyclopropanation — pages 2

## Strongly-covered families

- Benzoin and Retro-Benzoin Condensation — queryable 80 (tier1 80, tier2 0)
- Acyloin Condensation — queryable 50 (tier1 50, tier2 0)
- Acetoacetic Ester Synthesis — queryable 39 (tier1 39, tier2 0)
- Ciamician-Dennstedt Rearrangement — queryable 29 (tier1 29, tier2 0)
- Arndt-Eistert Homologation / Synthesis — queryable 16 (tier1 16, tier2 0)
- Baker-Venkataraman Rearrangement — queryable 15 (tier1 3, tier2 12)
- Barton Radical Decarboxylation Reaction — queryable 12 (tier1 11, tier2 1)
- Benzilic Acid Rearrangement — queryable 12 (tier1 3, tier2 9)
- Bartoli Indole Synthesis — queryable 11 (tier1 0, tier2 11)
- Claisen-Ireland Rearrangement — queryable 11 (tier1 11, tier2 0)
- Buchwald-Hartwig Cross-Coupling — queryable 10 (tier1 3, tier2 7)
- Chichibabin Amination Reaction — queryable 9 (tier1 5, tier2 4)
- Balz-Schiemann Reaction — queryable 8 (tier1 5, tier2 3)
- Castro-Stephens Coupling — queryable 8 (tier1 2, tier2 6)
- Aza-Cope Rearrangement — queryable 6 (tier1 6, tier2 0)
- Brook Rearrangement — queryable 6 (tier1 1, tier2 5)
- Bischler-Napieralski Isoquinoline Synthesis — queryable 5 (tier1 5, tier2 0)
- Claisen Rearrangement — queryable 5 (tier1 5, tier2 0)
- Aza-Wittig Reaction — queryable 4 (tier1 4, tier2 0)
- Bergman Cycloaromatization Reaction — queryable 4 (tier1 4, tier2 0)

## One-source-of-truth policy

- `app/labint.db` is the only canonical baseline.
- `app/labint_round9_bridge_work.db` exists, but must remain disposable (queryable=144).

## Recommended next order

- 1) Treat app/labint.db as the only canonical baseline
- 2) Expand queryable family coverage beyond the current level with structure-bearing pages first
- 3) Expand benchmark from small to medium after each dataization batch
- 4) Only then consider reaction_cards direct SMILES backfill with a reviewed mapping strategy
