This patch updates the long-form family shells so they behave as aliases of the canonical short-form families.

Targets:
- Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement -> Fries Rearrangement
- Hofmann-Löffler-Freytag Reaction (Remote Functionalization) -> Hofmann-Loffler-Freytag Reaction
- Houben-Hoesch Reaction/Synthesis -> Houben-Hoesch Reaction

What it changes:
- Adds the long-form names to short-form synonym_names
- Marks the long-form rows as alias shells in description_short
- Inserts/updates abbreviation_aliases rows with entity_type='reaction_family'
- Backs up each touched DB into reports/family_alias_cleanup/<timestamp>/

Databases touched if present:
- app/labint.db
- app/labint_round9_bridge_work.db
- app/labint_v5_stage.db

Run:
  conda activate chemlens
  cd /d C:\chemlens\backend
  run_family_alias_ontology_cleanup.bat
