from __future__ import annotations

import argparse
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.labint_v2 import ensure_labint_v2_schema, get_labint_v2_counts, migrate_reaction_cards_to_v2


def main() -> int:
    ap = argparse.ArgumentParser(description="Initialize LabInt v2 schema and migrate legacy reaction_cards.")
    ap.add_argument("--db", default=str(BACKEND_DIR / "app" / "labint.db"), help="Path to labint.db")
    ap.add_argument("--limit", type=int, default=0, help="0 means migrate all unmigrated cards")
    ap.add_argument("--since-id", type=int, default=0, help="Only migrate reaction_cards with id > since_id")
    args = ap.parse_args()

    db_path = Path(args.db)
    ensure_labint_v2_schema(db_path)
    before = get_labint_v2_counts(db_path)
    res = migrate_reaction_cards_to_v2(db_path, limit=args.limit, since_id=args.since_id)
    after = get_labint_v2_counts(db_path)

    print({"before": before, "migration": res, "after": after})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
