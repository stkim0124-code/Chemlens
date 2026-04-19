# Gemini family automation run

- run_id: 20260418_042044
- canonical_db: C:\chemlens\backend\app\labint.db
- stage_db: C:\chemlens\backend\app\labint_gemini_autorun_safe.db
- family_target: 60
- inserted_count: 3
- created_pattern_count: 0
- final_family_coverage: 44

## Decisions

| family | status | decision | target_family | model | created_pattern | note |
|---|---|---|---|---|---:|---|
| Baylis-Hillman Reaction | inserted | merge_existing | Baylis-Hillman Reaction | gemini-2.5-pro | False | The candidate is a canonical overview of the Baylis-Hillman reaction and perfectly matches the existing family of the same name, justifying a merge decision. |
| Birch Reduction | inserted | merge_existing | Birch Reduction | gemini-2.5-pro | False | This is a canonical overview page for the Birch reduction. The decision is to merge with the existing 'Birch Reduction' family due to the exact name and chemistry match. The example with anisole illustrates the regioselectivity for electron-donating groups. |
| Carroll Rearrangement | inserted | merge_existing | Carroll Rearrangement | gemini-2.5-pro | False | The candidate page is a canonical overview of the Carroll Rearrangement, which already exists in the database. The chemistry, name, and scope are identical, so merging is appropriate. |
