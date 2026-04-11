from app.labint_frontmatter_batch5 import apply_frontmatter_batch5
from pathlib import Path

base = Path(__file__).resolve().parent / 'app'
for db_name in ['labint.db', 'labint_round9_bridge_work.db']:
    db_path = base / db_name
    if db_path.exists():
        print(db_name, apply_frontmatter_batch5(db_path))
