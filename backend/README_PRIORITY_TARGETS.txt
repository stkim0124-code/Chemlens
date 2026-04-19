
This patch adds one script:
- prioritize_family_coverage_targets.py

Run:
  conda activate chemlens
  cd /d C:\chemlensackend
  python prioritize_family_coverage_targets.py

Outputs:
- FAMILY_COVERAGE_PRIORITY_REPORT.json
- FAMILY_COVERAGE_PRIORITY_REPORT.md
- REACTION_CARDS_SMILES_AUDIT.json

Use this before any new manual dataization batch.
The goal is to target uncovered families that still have usable exact-like tier3 candidates,
while avoiding a risky mass backfill of reaction_cards.
