import json
from pathlib import Path

for p in [
    r'C:\chemlens\backend\reports\phase4\gateB\iterate1_20260424\admission\family_admission_results.json',
    r'C:\chemlens\backend\reports\phase4\gateC\apply_20260424_204941\admission\family_admission_results.json',
    r'C:\chemlens\backend\reports\phase4\gateB\iterate1_20260424\coverage\corpus_coverage_results.json',
    r'C:\chemlens\backend\reports\phase4\gateC\apply_20260424_204941\coverage\corpus_coverage_results.json',
]:
    print(f'\n=== {p} ===')
    try:
        d = json.loads(Path(p).read_text(encoding='utf-8-sig'))
    except Exception as e:
        print(f'  ERR {e}')
        continue
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(v, (int, float, str, bool)) or v is None:
                print(f'  {k}: {v}')
            elif isinstance(v, list):
                print(f'  {k}: list({len(v)})')
                if v and isinstance(v[0], dict):
                    print(f'    sample case keys: {sorted(v[0].keys())}')
            elif isinstance(v, dict):
                print(f'  {k}: dict({list(v.keys())})')
    else:
        print(f'  root type: {type(d).__name__}')
