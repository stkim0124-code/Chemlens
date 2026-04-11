from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.labint_frontmatter_batch3 import apply_frontmatter_batch3, export_frontmatter_batch3_seed_templates


def main() -> None:
    parser = argparse.ArgumentParser(description='Apply manual front-matter batch3 schema/data upgrade')
    parser.add_argument('--db', action='append', required=True, help='Target SQLite DB path. Repeatable.')
    parser.add_argument('--export-templates-dir', default='seed_templates', help='Where to export CSV seed templates')
    args = parser.parse_args()
    export_frontmatter_batch3_seed_templates(Path(args.export_templates_dir) / 'frontmatter_batch3')
    results = {}
    for db in args.db:
        path = Path(db)
        if not path.exists():
            raise SystemExit(f'DB not found: {path}')
        results[str(path)] = apply_frontmatter_batch3(path)
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
