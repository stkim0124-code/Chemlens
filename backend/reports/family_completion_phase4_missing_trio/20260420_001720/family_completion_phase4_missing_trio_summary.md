# Family completion phase4 missing-trio summary

- tag: `phase4_family_completion_missing_trio_v1`
- db: `app\labint.db`
- dry_run: `False`

## Before
- queryable family count: 284
- queryable molecule count: 1076

## Alias updates (data tables only)
- reaction_extracts: Alkene (olefin) Metathesis → Alkene (Olefin) Metathesis (0 rows)
- extract_molecules: Alkene (olefin) Metathesis → Alkene (Olefin) Metathesis (0 rows)
- reaction_extracts: Barton-Mccombie Radical Deoxygenation Reaction → Barton-McCombie Radical Deoxygenation Reaction (0 rows)
- extract_molecules: Barton-Mccombie Radical Deoxygenation Reaction → Barton-McCombie Radical Deoxygenation Reaction (0 rows)
- reaction_extracts: Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement → Fries Rearrangement (0 rows)
- extract_molecules: Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement → Fries Rearrangement (0 rows)
- reaction_extracts: Hofmann-Löffler-Freytag Reaction (Remote Functionalization) → Hofmann-Loffler-Freytag Reaction (0 rows)
- extract_molecules: Hofmann-Löffler-Freytag Reaction (Remote Functionalization) → Hofmann-Loffler-Freytag Reaction (0 rows)
- reaction_extracts: Houben-Hoesch Reaction/Synthesis → Houben-Hoesch Reaction (0 rows)
- extract_molecules: Houben-Hoesch Reaction/Synthesis → Houben-Hoesch Reaction (0 rows)

## Inserted seeds
- Fries Rearrangement: inserted | extract_id=798 | page=181 | molecules=2
- Fries Rearrangement: inserted | extract_id=799 | page=181 | molecules=2
- Hofmann-Loffler-Freytag Reaction: inserted | extract_id=800 | page=209 | molecules=2
- Hofmann-Loffler-Freytag Reaction: inserted | extract_id=801 | page=209 | molecules=2
- Houben-Hoesch Reaction: inserted | extract_id=802 | page=217 | molecules=3
- Houben-Hoesch Reaction: inserted | extract_id=803 | page=217 | molecules=3

## Verification
### Fries Rearrangement
- extract_count: 3
- overview_count: 1
- application_count: 2
- extract_with_reactant: 3
- extract_with_product: 3
- extract_with_both: 3
- queryable_reactants: 3
- queryable_products: 3
- unique_queryable_pair_count: 3
- completion_gate_minimum_pass: True
- rich_completion_pass: True

### Hofmann-Loffler-Freytag Reaction
- extract_count: 3
- overview_count: 1
- application_count: 2
- extract_with_reactant: 3
- extract_with_product: 3
- extract_with_both: 3
- queryable_reactants: 3
- queryable_products: 3
- unique_queryable_pair_count: 3
- completion_gate_minimum_pass: True
- rich_completion_pass: True

### Houben-Hoesch Reaction
- extract_count: 3
- overview_count: 1
- application_count: 2
- extract_with_reactant: 3
- extract_with_product: 3
- extract_with_both: 3
- queryable_reactants: 5
- queryable_products: 3
- unique_queryable_pair_count: 3
- completion_gate_minimum_pass: True
- rich_completion_pass: True

## After
- queryable family count: 284
- queryable molecule count: 1090

## Important note
- This patch intentionally leaves legacy alias rows in reaction_family_patterns untouched.
- Continue using final_state_verifier + family_completion_dashboard canonicalization for display/tracking after apply.