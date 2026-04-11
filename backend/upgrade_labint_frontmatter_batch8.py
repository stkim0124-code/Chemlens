from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.labint_frontmatter_batch8 import apply_frontmatter_batch8


def main() -> None:
    for name in ['app/labint.db', 'app/labint_round9_bridge_work.db']:
        p = ROOT / name
        if p.exists():
            print(name, apply_frontmatter_batch8(p))
        else:
            print(name, 'missing')


if __name__ == '__main__':
    main()
