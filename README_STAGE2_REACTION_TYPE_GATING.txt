CHEMLENS Stage 2 Patch: Reaction-Type Gating (coarse, durable version)

What changed
- Keeps Step 1 family aggregation / dedup behavior.
- Adds coarse reaction-type signals derived from reactant/product delta.
- Applies family-level gating so chemically mismatched families are penalized early.
- Adds payload transparency fields:
  - query_reaction_types
  - query_reaction_types_ko
  - query_reaction_type_signals
  - reaction_delta_summary
  - coarse_gate_multiplier / coarse_gate_notes per result

Initial gated families
- Buchner / Buchner Method of Ring Expansion
- Amadori Rearrangement
- Buchwald-Hartwig Cross-Coupling
- Baeyer-Villiger Oxidation
- Beckmann Rearrangement
- Barton Radical Decarboxylation
- Barton-McCombie Radical Deoxygenation
- Biginelli Reaction
- Burgess Dehydration Reaction

Overwrite target
- backend/app/evidence_search.py

Notes
- This file already includes Step 1 family aggregation logic. You can overwrite directly on top of the current project state.
- No frontend overwrite is required for this step.
