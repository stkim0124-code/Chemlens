CHEMLENS backend DB schema frontmatter batch7 cumulative patch
Date: 2026-04-11

Scope of this cumulative patch
- Preserves the prior cumulative state from intel patch + batch1~batch6
- Adds named-reaction frontmatter batch7 pages (p22~p31)
- Seeds five additional reaction-family knowledge entries:
  1) Aza-Cope Rearrangement
  2) Aza-Wittig Reaction
  3) Aza-[2,3]-Wittig Rearrangement
  4) Baeyer-Villiger Oxidation/Rearrangement
  5) Baker-Venkataraman Rearrangement
- Adds curated abbreviations/reagent aliases and manual page entities for the uploaded image set

Important operational note
- This ZIP is intended as a drop-in overwrite patch.
- Excluded on purpose: node_modules, .vite, virtual environments, __pycache__.

Main files added in this patch
- app/labint_frontmatter_batch7.py
- upgrade_labint_frontmatter_batch7.py
- run_db_frontmatter_batch7_upgrade.bat
- seed_templates/frontmatter_batch7/*

DB targets updated
- app/labint.db
- app/labint_round9_bridge_work.db

Page bundle covered
- named reactions_74.jpg ~ named reactions_83.jpg
