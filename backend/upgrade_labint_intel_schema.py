from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.labint_intel import backfill_labint_intel, export_seed_templates


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply LabInt intelligence schema upgrade/backfill")
    parser.add_argument("--db", action="append", required=True, help="Target SQLite DB path. Repeatable.")
    parser.add_argument("--export-templates-dir", default="seed_templates", help="Where to export CSV seed templates")
    args = parser.parse_args()

    export_seed_templates(Path(args.export_templates_dir))
    results = {}
    for db in args.db:
        path = Path(db)
        if not path.exists():
            raise SystemExit(f"DB not found: {path}")
        results[str(path)] = backfill_labint_intel(path)
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
