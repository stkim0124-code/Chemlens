# FAMILY COVERAGE PRIORITY REPORT

- manual families (raw): 250
- queryable families (raw): 41
- covered (raw exact match): 32
- covered (normalized match): 34
- uncovered (normalized): 216

## Why this matters

- The previous simple report undercounts covered manual families when naming variants differ.
- Use the normalized count for planning, but keep the raw names unchanged in the DB.

## Top uncovered families to target next

### Aldol Reaction
- priority_score: 54
- manual_rows: 2
- extract_rows: 8
- tier3_rows: 36
- exact_like_score: 22
- sample candidates:
  - [product] beta-hydroxy ketone (count=4)
  - [reactant] aldehyde (count=3)
  - [solvent] DCM (count=2)
  - [intermediate] favored TS, unfavored TS (count=2)
  - [reactant] ketone (count=2)

### Barbier Coupling Reaction
- priority_score: 16
- manual_rows: 2
- extract_rows: 2
- tier3_rows: 0
- exact_like_score: 0

### Baylis-Hillman Reaction
- priority_score: 16
- manual_rows: 2
- extract_rows: 2
- tier3_rows: 0
- exact_like_score: 0

### Birch Reduction
- priority_score: 16
- manual_rows: 2
- extract_rows: 2
- tier3_rows: 0
- exact_like_score: 0

### Carroll Rearrangement
- priority_score: 16
- manual_rows: 2
- extract_rows: 2
- tier3_rows: 0
- exact_like_score: 0

### Alder (Ene) Reaction (Hydro-Allyl Addition)
- priority_score: 10
- manual_rows: 2
- extract_rows: 0
- tier3_rows: 0
- exact_like_score: 0

### Amadori Reaction / Rearrangement
- priority_score: 10
- manual_rows: 2
- extract_rows: 0
- tier3_rows: 0
- exact_like_score: 0

### Arbuzov Reaction (Michaelis-Arbuzov Reaction)
- priority_score: 10
- manual_rows: 2
- extract_rows: 0
- tier3_rows: 0
- exact_like_score: 0

### Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)
- priority_score: 10
- manual_rows: 2
- extract_rows: 0
- tier3_rows: 0
- exact_like_score: 0

### Baeyer-Villiger Oxidation/Rearrangement
- priority_score: 10
- manual_rows: 2
- extract_rows: 0
- tier3_rows: 0
- exact_like_score: 0

### Baldwin's Rules / Guidelines for Ring-Closing Reactions
- priority_score: 10
- manual_rows: 2
- extract_rows: 0
- tier3_rows: 0
- exact_like_score: 0

### Claisen Condensation / Claisen Reaction
- priority_score: 10
- manual_rows: 2
- extract_rows: 0
- tier3_rows: 0
- exact_like_score: 0

### Combes Quinoline Synthesis
- priority_score: 10
- manual_rows: 2
- extract_rows: 0
- tier3_rows: 0
- exact_like_score: 0

### Cope Elimination / Cope Reaction
- priority_score: 10
- manual_rows: 2
- extract_rows: 0
- tier3_rows: 0
- exact_like_score: 0

### Cope Rearrangement
- priority_score: 10
- manual_rows: 2
- extract_rows: 0
- tier3_rows: 0
- exact_like_score: 0

## Reaction cards SMILES audit

- total cards: 11371
- cards with any smiles: 46
- cards with both smiles: 5

## Recommended next action

1. Do NOT mass-backfill reaction_cards yet.
2. Use the top uncovered family list to guide manual dataization from image batches.
3. Prefer families with both extract_rows and exact_like_score > 0 first.