from pathlib import Path
from app.labint_frontmatter_batch10 import apply_frontmatter_batch10
print(apply_frontmatter_batch10(Path(__file__).resolve().parent / 'app' / 'labint.db'))
print(apply_frontmatter_batch10(Path(__file__).resolve().parent / 'app' / 'labint_round9_bridge_work.db'))
