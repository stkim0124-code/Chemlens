from pathlib import Path
from app.labint_frontmatter_batch9 import apply_frontmatter_batch9

if __name__ == '__main__':
    root = Path(__file__).resolve().parent
    for name in ['labint.db','labint_round9_bridge_work.db']:
        print(name, apply_frontmatter_batch9(root / 'app' / name))
