This patch solves the "bridge_work is not the baseline" limitation first.

What it changes:
- build_round9_structure_evidence_bridge.py now treats app/labint.db as the canonical baseline.
- The default disposable output is app/labint_bridge_work.db, not app/labint_round9_bridge_work.db.
- report_remaining_limitations.py generates a fresh gap report from your local canonical DB.

Recommended use:
1) Copy these files into C:\chemlens\backend
2) Run:
   conda activate chemlens
   cd /d C:\chemlens\backend
   python report_remaining_limitations.py
3) Read:
   - CANONICAL_DB_GAP_REPORT.md
   - BRIDGE_WORK_STATUS.json

This patch does NOT rewrite your canonical DB.
It only clarifies one-source-of-truth and generates an actionable audit for the next limitation.
