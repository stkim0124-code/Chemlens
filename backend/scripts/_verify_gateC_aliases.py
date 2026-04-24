"""Verify Gate C alias edits: syntax + canonical collapse for macrolact triad + ullmann pair."""
import sys, sqlite3, ast
from pathlib import Path

here = Path(r'C:\chemlens\backend')
sys.path.insert(0, str(here))

# AST syntax check
src = (here / 'final_state_verifier.py').read_text(encoding='utf-8')
ast.parse(src)
print('AST_OK')

import final_state_verifier as fsv

conn = fsv.connect_db(here / 'app' / 'labint.db')
raw_names = fsv.distinct_pattern_names(conn)
print(f'raw_names_count={len(raw_names)}')

# Check our 5 target raws
targets = [
    'Keck Macrolactonization',
    'Yamaguchi Macrolactonization',
    'Corey-Nicolaou Macrolactonization',
    'Ullmann Reaction / Coupling / Biaryl Synthesis',
    'Ullmann Biaryl Ether and Biaryl Amine Synthesis / Condensation',
]
for t in targets:
    in_raw = t in raw_names
    canon = fsv.canonicalize_name(t, raw_names)
    print(f'  [{in_raw}]  {t!r:80s}  ->  {canon!r}')

# Alias groups after canonicalization
groups, collapse = fsv.build_alias_groups(raw_names)
for canon in sorted(groups.keys()):
    if 'acrolacton' in canon.lower() or 'llmann' in canon.lower():
        print(f'\nGROUP[{canon}] raws={sorted(groups[canon])}')

# Rich-family summary check
for canon in ['Corey-Nicolaou Macrolactonization', 'Ullmann Reaction / Coupling / Biaryl Synthesis']:
    if canon not in groups:
        print(f'SKIP {canon} (not in groups)')
        continue
    pair_map = fsv.pair_map_from_extract_molecules(conn)
    summary = fsv.summarize_canonical_family(conn, canon, groups[canon], pair_map)
    print(f'\nSUMMARY[{canon}] bucket={summary.get("completion_bucket")} overview_count={summary.get("overview_count")} app_ex_count={summary.get("application_example_count")}')
