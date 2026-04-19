# PATCH_NOTES_v4_4.md

## v4.4 patch summary

This patch is intentionally narrow and addresses the two concrete failures seen in v4.3:

1. **Covered-family misclassification**
   - Fixed coverage lookup to use runtime `_norm(family_name)` instead of trusting legacy `reaction_family_patterns.family_name_norm` values.
   - Effect: previously covered families should stop reappearing as `covered=0` in plan-only mode.

2. **Deterministic seed reuse failure**
   - Deterministic lane now clones role-resolved molecules from `extract_molecules` and carries over required extract metadata such as `scheme_candidate_id`.
   - Effect: `seed=1` candidates should no longer fail simply because legacy text fields are NULL or pipe-delimited.

## Operational policy preserved
- family_target = 250
- deterministic first, Gemini when needed
- expanded diagnostic benchmark kept
- Gemini model pinned to gemini-2.5-pro
- disk budget = 10GB
- no candidate_backup files
