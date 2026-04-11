CHEMLENS DB SCHEMA FRONTMATTER BATCH13 PATCH
Date: 2026-04-11

This cumulative backend patch extends named-reaction frontmatter coverage with ten additional pages.

Families covered in batch13:
- Chugaev Elimination Reaction
- Ciamician-Dennstedt Rearrangement
- Claisen Condensation / Claisen Reaction
- Claisen Rearrangement
- Claisen-Ireland Rearrangement

Batch13 additions:
- manual_page_knowledge: +10
- manual_page_entities: +30
- abbreviation_aliases: updated with batch13 seed entries
- reaction_family_patterns: 5 family rows updated/seeded

Current cumulative counts in bundled labint.db:
- reaction_family_patterns: 292
- abbreviation_aliases: 560
- manual_page_knowledge: 130
- manual_page_entities: 990

Batch13 page kinds:
[('application_example', 5), ('canonical_overview', 5)]

Files added in this patch:
- app/labint_frontmatter_batch13.py
- upgrade_labint_frontmatter_batch13.py
- run_db_frontmatter_batch13_upgrade.bat
- seed_templates/frontmatter_batch13/*
- bundled SQLite DB files updated
