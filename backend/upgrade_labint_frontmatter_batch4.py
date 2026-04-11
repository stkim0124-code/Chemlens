from __future__ import annotations

import json
from pathlib import Path

from app.labint_frontmatter_batch4 import apply_frontmatter_batch4


def main() -> None:
    backend_root = Path(__file__).resolve().parent
    targets = [
        backend_root / 'app' / 'labint.db',
        backend_root / 'app' / 'labint_round9_bridge_work.db',
    ]
    out = {}
    for db_path in targets:
        if db_path.exists():
            out[str(db_path)] = apply_frontmatter_batch4(db_path)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
