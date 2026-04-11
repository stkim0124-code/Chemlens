CHEMLENS backend cumulative patch - DB schema/frontmatter batch3 (2026-04-11)

What this cumulative patch adds on top of batch2
- Adds manual page knowledge for pages xxxiv-xliii (named reactions_34.jpg to named reactions_43.jpg)
- Seeds abbreviation_aliases with additional glossary aliases/reagents/bases/solvents/ligands/protecting groups
- Adds explicit metathesis-family glossary seeds: RCAM, RCM, ROM, ROMP
- Adds manual_page_entities for the new glossary pages

Files added
- app/labint_frontmatter_batch3.py
- upgrade_labint_frontmatter_batch3.py
- run_db_frontmatter_batch3_upgrade.bat
- seed_templates/frontmatter_batch3/frontmatter_batch3_manual_pages.csv
- seed_templates/frontmatter_batch3/frontmatter_batch3_abbreviation_seed.csv

How to apply
1) Extract this ZIP
2) Overwrite your backend folder with the extracted files
3) Restart backend
4) If needed, run run_db_frontmatter_batch3_upgrade.bat

Notes
- This ZIP is cumulative and includes prior batch1/batch2 structure files and updated SQLite DBs.
- This batch mainly strengthens abbreviation/alias normalization and glossary-driven family hints rather than direct reaction-example cards.
