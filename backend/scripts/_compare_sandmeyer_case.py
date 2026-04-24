"""Compare Sandmeyer 1237 and Schmidt 1240 benchmark spec between iter1 and gateC."""
import json
from pathlib import Path

ITER1 = Path(r'C:\chemlens\backend\reports\phase4\gateB\iterate1_20260424')
GATEC = Path(r'C:\chemlens\backend\reports\phase4\gateC\apply_20260424_204941')

# Gate C benchmark JSONs (post-rebuild) live in benchmark_backup/*.post.json
GATEC_BENCH = GATEC / 'benchmark_backup'
ITER1_BENCH = ITER1 / 'benchmark_backup'
# iterate1 didn't store benchmark_backup in same layout. Try original benchmark/ dir timestamps.

# Check results JSON rows for cases of interest
def find_row(p, cid):
    d = json.loads(Path(p).read_text(encoding='utf-8-sig'))
    for r in d.get('rows', []):
        if r['case_id'] == cid:
            return r
    return None

for cid in ['adm_sandmeyer_reaction_1237', 'adm_schmidt_reaction_1240']:
    print(f'\n=== {cid} ===')
    r1 = find_row(ITER1 / 'admission' / 'family_admission_results.json', cid)
    r2 = find_row(GATEC / 'admission' / 'family_admission_results.json', cid)
    for k in ('expected_family', 'source_extract_id', 'top1_family', 'top3_families', 'top3_scores', 'error'):
        v1 = r1.get(k) if r1 else None
        v2 = r2.get(k) if r2 else None
        print(f'  {k}:')
        print(f'    iter1: {v1!r}')
        print(f'    gateC: {v2!r}')

# Also pull reaction_smiles from the benchmark SPEC (not results)
print('\n\n=== reaction_smiles from SPEC ===')
for nm in ('family_admission_benchmark.pre.json', 'family_admission_benchmark.post.json'):
    pass  # pre doesn't exist

# We saved benchmark_backup/family_admission_benchmark.json (pre-rebuild, 259 cases)
# and post.json after rebuild
spec_iter1 = Path(r'C:\chemlens\backend\reports\phase4\gateC\apply_20260424_204941\benchmark_backup\family_admission_benchmark.json')
spec_gatec = Path(r'C:\chemlens\backend\reports\phase4\gateC\apply_20260424_204941\benchmark_backup\family_admission_benchmark.post.json')
if spec_iter1.exists():
    s1 = json.loads(spec_iter1.read_text(encoding='utf-8'))
    # find cases by id
    for cid in ['adm_sandmeyer_reaction_1237', 'adm_schmidt_reaction_1240']:
        for c in s1.get('cases', []):
            if c['case_id'] == cid:
                print(f'\n  SPEC-PRE {cid}:')
                print(f'    reaction_smiles={c.get("reaction_smiles")!r}')
                print(f'    expected_family={c.get("expected_family")!r}')
                print(f'    acceptable_top1={c.get("acceptable_top1")}')
                print(f'    raw_family_aliases={c.get("raw_family_aliases")}')
                break
if spec_gatec.exists():
    s2 = json.loads(spec_gatec.read_text(encoding='utf-8'))
    for cid in ['adm_sandmeyer_reaction_1237', 'adm_schmidt_reaction_1240']:
        for c in s2.get('cases', []):
            if c['case_id'] == cid:
                print(f'\n  SPEC-POST {cid}:')
                print(f'    reaction_smiles={c.get("reaction_smiles")!r}')
                print(f'    expected_family={c.get("expected_family")!r}')
                print(f'    acceptable_top1={c.get("acceptable_top1")}')
                print(f'    raw_family_aliases={c.get("raw_family_aliases")}')
                break
