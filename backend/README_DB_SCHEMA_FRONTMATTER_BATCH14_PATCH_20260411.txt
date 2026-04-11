CHEMLENS DB Schema Frontmatter Batch14 Patch (2026-04-11)

Scope:
- Clemmensen Reduction final named-reaction pages from named reaction.zip
- p92 Importance/Mechanism and p93 Synthetic Applications

Assets included:
- app/labint.db
- app/labint_round9_bridge_work.db
- app/labint_frontmatter_batch14.py
- upgrade_labint_frontmatter_batch14.py
- run_db_frontmatter_batch14_upgrade.bat
- seed_templates/frontmatter_batch14/*

Batch14 deltas:
- manual_page_knowledge: +2 pages
- manual_page_entities: +6 entities
- abbreviation_aliases: +5 source rows (net increase may differ if aliases already existed)
- reaction_family_patterns: Clemmensen Reduction updated

Current totals after batch14:
- reaction_family_patterns: 292
- abbreviation_aliases: 565
- manual_page_knowledge: 132
- manual_page_entities: 996
- Clemmensen Reduction counts (overview/application/mechanism): 2/5/3
