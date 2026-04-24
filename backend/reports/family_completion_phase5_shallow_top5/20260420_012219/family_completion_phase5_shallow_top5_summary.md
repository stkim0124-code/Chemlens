# Family completion phase5 shallow-top5 summary

- tag: `phase5_shallow_top5_completion_v1`
- db: `app\labint.db`
- dry_run: `True`

## Before
- queryable family count: 284
- queryable molecule count: 1090

## Alias updates (data tables only)
- reaction_extracts: Alder Ene Reaction → Alder (Ene) Reaction (Hydro-Allyl Addition) (1 rows)
- extract_molecules: Alder Ene Reaction → Alder (Ene) Reaction (Hydro-Allyl Addition) (0 rows)
- reaction_extracts: Alder (ene) Reaction → Alder (Ene) Reaction (Hydro-Allyl Addition) (5 rows)
- extract_molecules: Alder (ene) Reaction → Alder (Ene) Reaction (Hydro-Allyl Addition) (21 rows)
- reaction_extracts: Amadori Rearrangement → Amadori Reaction / Rearrangement (3 rows)
- extract_molecules: Amadori Rearrangement → Amadori Reaction / Rearrangement (25 rows)
- reaction_extracts: Arbuzov Reaction → Arbuzov Reaction (Michaelis-Arbuzov Reaction) (7 rows)
- extract_molecules: Arbuzov Reaction → Arbuzov Reaction (Michaelis-Arbuzov Reaction) (50 rows)
- reaction_extracts: Aza-Claisen Rearrangement → Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement) (6 rows)
- extract_molecules: Aza-Claisen Rearrangement → Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement) (31 rows)

## Inserted seeds
- Alder (Ene) Reaction (Hydro-Allyl Addition): inserted | extract_id=804 | page=60 | molecules=3
- Alder (Ene) Reaction (Hydro-Allyl Addition): inserted | extract_id=805 | page=60 | molecules=3
- Amadori Reaction / Rearrangement: inserted | extract_id=806 | page=67 | molecules=2
- Amadori Reaction / Rearrangement: inserted | extract_id=807 | page=67 | molecules=2
- Arbuzov Reaction (Michaelis-Arbuzov Reaction): inserted | extract_id=808 | page=69 | molecules=4
- Arbuzov Reaction (Michaelis-Arbuzov Reaction): inserted | extract_id=809 | page=69 | molecules=4
- Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement): inserted | extract_id=810 | page=73 | molecules=2
- Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement): inserted | extract_id=811 | page=73 | molecules=2
- Baeyer-Villiger Oxidation/Rearrangement: inserted | extract_id=812 | page=81 | molecules=2
- Baeyer-Villiger Oxidation/Rearrangement: inserted | extract_id=813 | page=81 | molecules=2

## Verification
### Alder (Ene) Reaction (Hydro-Allyl Addition)
- extract_count: 9
- overview_count: 2
- application_count: 6
- extract_with_reactant: 4
- extract_with_product: 4
- extract_with_both: 4
- queryable_reactants: 8
- queryable_products: 4
- unique_queryable_pair_count: 4
- completion_gate_minimum_pass: True
- rich_completion_pass: True

### Amadori Reaction / Rearrangement
- extract_count: 6
- overview_count: 1
- application_count: 4
- extract_with_reactant: 3
- extract_with_product: 3
- extract_with_both: 3
- queryable_reactants: 3
- queryable_products: 3
- unique_queryable_pair_count: 3
- completion_gate_minimum_pass: True
- rich_completion_pass: True

### Arbuzov Reaction (Michaelis-Arbuzov Reaction)
- extract_count: 10
- overview_count: 4
- application_count: 5
- extract_with_reactant: 6
- extract_with_product: 6
- extract_with_both: 6
- queryable_reactants: 12
- queryable_products: 12
- unique_queryable_pair_count: 4
- completion_gate_minimum_pass: True
- rich_completion_pass: True

### Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)
- extract_count: 9
- overview_count: 2
- application_count: 6
- extract_with_reactant: 5
- extract_with_product: 5
- extract_with_both: 5
- queryable_reactants: 5
- queryable_products: 5
- unique_queryable_pair_count: 5
- completion_gate_minimum_pass: True
- rich_completion_pass: True

### Baeyer-Villiger Oxidation/Rearrangement
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


## After
- queryable family count: 280
- queryable molecule count: 1116
