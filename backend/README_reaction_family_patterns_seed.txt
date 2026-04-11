ROUND9 reaction_family_patterns seed patch

This patch adds the semantics seed layer without touching the sealed baseline.

Included:
- reaction_family_patterns_seed.csv (49 families)
- install_reaction_family_patterns.py
- run_install_reaction_family_patterns.bat
- app/labint_round9_bridge_work.db (pre-populated)
- app/evidence_search.py (stats endpoint now reports pattern counts when present)

Recommended use:
1) Overwrite into C:\chemlens\backend
2) Run run_install_reaction_family_patterns.bat
3) Start bridge backend as usual
4) Open http://127.0.0.1:8000/api/search/structure-evidence/stats
5) Confirm reaction_family_patterns == 49

Note:
- This patch seeds the family-pattern layer only.
- It does NOT yet consume the table for reaction semantics ranking.
- The next patch should implement FG / transformation matching against this table.
