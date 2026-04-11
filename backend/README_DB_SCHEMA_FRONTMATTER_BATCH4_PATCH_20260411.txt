CHEMLENS backend cumulative patch: DB schema + frontmatter batch4

This patch is cumulative.
You can overwrite the backend folder directly without applying older frontmatter patches first.

What this batch adds
- named reactions_44.jpg:
  - final abbreviation glossary page (TPS to Z/Cbz)
- named reactions_45.jpg ~ named reactions_52.jpg:
  - alphabetical named-reaction index pages
- named reactions_53.jpg:
  - section cover page for 'VII. Named Organic Reactions in Alphabetical Order'

Main additions
- app/labint_frontmatter_batch4.py
- upgrade_labint_frontmatter_batch4.py
- run_db_frontmatter_batch4_upgrade.bat
- seed_templates/frontmatter_batch4/frontmatter_batch4_manual_pages.csv
- seed_templates/frontmatter_batch4/frontmatter_batch4_abbreviation_seed.csv
- seed_templates/frontmatter_batch4/frontmatter_batch4_family_seed.csv

Data model impact
- manual_page_knowledge: +10 batch4 pages
- manual_page_entities: reaction-family index entries + abbreviation entries
- abbreviation_aliases: xliv glossary additions
- reaction_family_patterns: alphabetical named-reaction index family seeds

Apply
1) Overwrite your backend folder with this patch.
2) Restart backend.
3) If needed, run:
   run_db_frontmatter_batch4_upgrade.bat

Notes
- This batch intentionally seeds many family names from the named-reaction index pages.
- Some family names preserve the textbook's exact wording, including variant spellings or parenthetical aliases, to maximize recall during later search/ranking work.
