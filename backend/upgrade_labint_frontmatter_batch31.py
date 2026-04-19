from __future__ import annotations
import sys
from pathlib import Path
from app.labint_frontmatter_batch31 import apply_frontmatter_batch31
DEFAULT_TARGETS = [Path(__file__).resolve().parent / 'app' / 'labint.db', Path(__file__).resolve().parent / 'app' / 'labint_round9_bridge_work.db']
def main(argv: list[str]) -> int:
    targets = [Path(x) for x in argv] if argv else [p for p in DEFAULT_TARGETS if p.exists()]
    if not targets:
        print('No target DB found. Pass an explicit db path.')
        return 1
    for db in targets:
        print(f'[batch31] applying to {db}')
        print(apply_frontmatter_batch31(db))
    return 0
if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
