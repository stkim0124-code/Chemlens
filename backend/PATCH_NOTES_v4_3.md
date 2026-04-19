CHEMLENS v4.3 patch notes
=========================

1. True deterministic lane
- Deterministic lane no longer calls Gemini.
- It reuses an existing family extract with valid reactant/product SMILES and promotes it as a canonical seed.
- Zero-evidence families go straight to Gemini lane.

2. Lane efficiency
- Removes the double-Gemini pattern (`deterministic failed -> strict Gemini`) for zero-evidence families.
- Plan output now shows `seed=1/0` so reusable candidates are obvious.

3. Merge safety
- Canonical merge now includes `gemini_auto_seed`, `deterministic_gemini_seed`, and `deterministic_seed_from_existing`.
- Only reaction_extracts linked to those selected molecules are merged.

4. Diagnostic benchmark cleanup
- Removed three invalid Barton exact-extract cases that crashed the expanded benchmark runner.
