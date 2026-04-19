from __future__ import annotations

import sys
from pathlib import Path

from app.frontmatter_repair import repair_frontmatter_db
from app.labint_frontmatter_batch11 import apply_frontmatter_batch11
from app.labint_frontmatter_batch12 import apply_frontmatter_batch12
from app.labint_frontmatter_batch13 import apply_frontmatter_batch13
from app.labint_frontmatter_batch14 import apply_frontmatter_batch14

DEFAULT_TARGETS = [
    Path(__file__).resolve().parent / 'app' / 'labint.db',
    Path(__file__).resolve().parent / 'app' / 'labint_round9_bridge_work.db',
]


def main(argv: list[str]) -> int:
    targets = [Path(x) for x in argv] if argv else [p for p in DEFAULT_TARGETS if p.exists()]
    if not targets:
        print('No target DB found. Pass an explicit db path.')
        return 1
    for db in targets:
        print(f'[frontmatter-repair] repairing {db}')
        print(repair_frontmatter_db(db))
        print(f'[frontmatter-repair] applying batch11 -> {db}')
        print(apply_frontmatter_batch11(db))
        print(f'[frontmatter-repair] applying batch12 -> {db}')
        print(apply_frontmatter_batch12(db))
        print(f'[frontmatter-repair] applying batch13 -> {db}')
        print(apply_frontmatter_batch13(db))
        print(f'[frontmatter-repair] applying batch14 -> {db}')
        print(apply_frontmatter_batch14(db))
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
