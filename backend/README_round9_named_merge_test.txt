
ROUND9 NAMED REACTIONS TEST PATCH

Goal
- Merge named reactions staging tables into app/labint.db
- Keep reaction_cards promote forbidden
- Add /api/named-reactions/search and /api/named-reactions/summary
- Make named reactions searchable in the React UI via a dedicated tab

Recommended order
1) Overwrite backend/ and frontend/ files from this patch
2) Run backend/run_merge_round9_staging_into_labint.bat
3) Start backend and frontend
4) Open the new '🧪 Named Reactions' tab and test queries

Suggested test queries
- Aldol Reaction
- Claisen Rearrangement
- Buchwald-Hartwig
- Clemmensen Reduction
- Grubbs catalyst
- gamma-diketone

Important policy
- This is evidence-layer merge only
- page_images / scheme_candidates / reaction_extracts are merged
- reaction_cards automatic promote is still forbidden
