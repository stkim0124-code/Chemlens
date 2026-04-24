# Family completion phase3 summary

- tag: `phase3_family_completion_metathesis_barton_v1`
- db: `app\labint.db`
- dry_run: `True`

## Before
- queryable family count: 284
- queryable molecule count: 1063

## Alias updates
- reaction_extracts: Alkene (olefin) Metathesis → Alkene (Olefin) Metathesis (8 rows)
- extract_molecules: Alkene (olefin) Metathesis → Alkene (Olefin) Metathesis (29 rows)
- reaction_extracts: Barton-Mccombie Radical Deoxygenation Reaction → Barton-McCombie Radical Deoxygenation Reaction (5 rows)
- extract_molecules: Barton-Mccombie Radical Deoxygenation Reaction → Barton-McCombie Radical Deoxygenation Reaction (32 rows)

## Inserted seeds
- Alkene (Olefin) Metathesis: inserted | extract_id=792 | page=62 | molecules=2
- Alkene (Olefin) Metathesis: inserted | extract_id=793 | page=63 | molecules=3
- Alkene (Olefin) Metathesis: inserted | extract_id=794 | page=63 | molecules=2
- Barton-McCombie Radical Deoxygenation Reaction: inserted | extract_id=795 | page=98 | molecules=2
- Barton-McCombie Radical Deoxygenation Reaction: inserted | extract_id=796 | page=99 | molecules=2
- Barton-McCombie Radical Deoxygenation Reaction: inserted | extract_id=797 | page=99 | molecules=2

## Verification
### Alkene (Olefin) Metathesis
- extract_count: 11
- overview_count: 5
- application_count: 5
- extract_with_reactant: 4
- extract_with_product: 5
- extract_with_both: 3
- queryable_reactants: 5
- queryable_products: 5
- completion_gate_minimum_pass: True

### Barton-McCombie Radical Deoxygenation Reaction
- extract_count: 8
- overview_count: 2
- application_count: 3
- extract_with_reactant: 5
- extract_with_product: 4
- extract_with_both: 4
- queryable_reactants: 6
- queryable_products: 4
- completion_gate_minimum_pass: True
