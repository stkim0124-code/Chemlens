CHEMLENS DB SCHEMA FRONTMATTER BATCH8 PATCH (2026-04-11)

This cumulative backend patch extends the named-reaction knowledge layer through batch8.

New families covered in batch8:
- Baldwin's Rules / Guidelines for Ring-Closing Reactions
- Balz-Schiemann Reaction (Schiemann Reaction)
- Bamford-Stevens-Shapiro Olefination
- Barbier Coupling Reaction
- Bartoli Indole Synthesis

What batch8 adds:
- 10 manual_page_knowledge rows (pages 32-41)
- 30 manual_page_entities rows
- 15 abbreviation_aliases rows
- 5 reaction_family_patterns seeded/updated
- Applies to both app/labint.db and app/labint_round9_bridge_work.db

Apply:
- Overwrite your backend folder with this ZIP contents
- Run: run_db_frontmatter_batch8_upgrade.bat

Notes:
- This ZIP is cumulative and retains prior intel/frontmatter files from earlier batches.
- node_modules, .vite, __pycache__, and virtual environments are excluded.
