CHEMLENS backend cumulative patch: DB schema + front matter batch2
Date: 2026-04-11

This patch is cumulative over the previous intel + frontmatter batch1 patch.
You can overwrite your backend folder directly with this ZIP.

What this batch adds
- Manual front-matter page knowledge for pages xxiv-xxxiii (10 pages)
- 176 new abbreviation_aliases rows from the uploaded glossary pages
- 180 new manual_page_entities rows linked to those pages
- 2 new reaction_family_patterns seeds:
  - Horner-Wadsworth-Emmons
  - Intramolecular Nitrile Oxide Cycloaddition

Updated cumulative totals in both DB files
- manual_page_knowledge: 20
- manual_page_entities: 280
- abbreviation_aliases: 292
- reaction_family_patterns: 58
- family_references: 8

Files added in this patch
- app/labint_frontmatter_batch2.py
- upgrade_labint_frontmatter_batch2.py
- run_db_frontmatter_batch2_upgrade.bat
- seed_templates/frontmatter_batch2/frontmatter_batch2_manual_pages.csv
- seed_templates/frontmatter_batch2/frontmatter_batch2_abbreviation_seed.csv

Representative newly added aliases
- DEAD, DIAD, DIB, DIBAL-H, DIPEA, DMAP, DMF, DMSO, DMTMM
- DPPA, dppf, dppp, EDCI, ee, Fmoc, HATU, HFIP, HOBt, HMDS
- IBX, imidazole (Im), KHMDS, LAH, LDA, LiHMDS, L-selectride
- m-CPBA, MEM, MOM, NMP, PMB

How to apply
1. Unzip this patch.
2. Overwrite your backend folder with the contents.
3. Restart backend.
4. If you want to re-run the DB update manually, run:
   - run_db_frontmatter_batch2_upgrade.bat

Notes
- This batch is mainly glossary/alias normalization data, not core reaction-example data.
- It improves search-time interpretation of reagents, ligands, protecting groups, condition terms, and explicit family abbreviations.
- Some attempted alias rows were deduplicated automatically where an equivalent row already existed.
