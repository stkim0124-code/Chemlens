"""Gate C REDO vs iter2 per-case diff after evidence_search.py restore."""
import json
from pathlib import Path

ITER2 = Path(r'C:\chemlens\backend\reports\phase4\gateB\iter2_20260424_203319')
REDO  = Path(r'C:\chemlens\backend\reports\phase4\gateC\apply_20260424_210600_redo')

def load(p): return json.loads(Path(p).read_text(encoding='utf-8-sig'))

# === ADMISSION ===
a1 = load(ITER2 / 'admission' / 'family_admission_results.json')
a2 = load(REDO  / 'admission' / 'family_admission_results.json')
print('=== ADMISSION ===')
s1 = a1.get('summary', {}); s2 = a2.get('summary', {})
for k in ('total_cases','top1_correct','top1_accuracy','top3_correct','top3_accuracy','confused_pairs'):
    print(f'  {k}: iter2={s1.get(k)} -> redo={s2.get(k)}')

def cm(data):
    return {r['case_id']: {'expected': r.get('expected_family'),
                            'top1_family': r.get('top1_family'),
                            'top1_pass': bool(r.get('top1_correct', False)),
                            'top3_pass': bool(r.get('top3_correct', False)),
                            'top3_families': r.get('top3_families', [])} for r in data.get('rows', [])}
m1 = cm(a1); m2 = cm(a2)
only1 = set(m1)-set(m2); only2 = set(m2)-set(m1); both = set(m1)&set(m2)
print(f'\n  dropped (merged away): {len(only1)}')
for cid in sorted(only1):
    print(f'    {cid} [top1_pass_iter2={m1[cid]["top1_pass"]} top3_pass_iter2={m1[cid]["top3_pass"]}]')
print(f'\n  added (new merged canonical): {len(only2)}')
for cid in sorted(only2):
    print(f'    {cid} [top1_pass={m2[cid]["top1_pass"]} top3_pass={m2[cid]["top3_pass"]}]')

p2f1 = []; f2p1 = []; p2f3 = []; f2p3 = []
for cid in both:
    if m1[cid]['top1_pass'] and not m2[cid]['top1_pass']: p2f1.append(cid)
    if not m1[cid]['top1_pass'] and m2[cid]['top1_pass']: f2p1.append(cid)
    if m1[cid]['top3_pass'] and not m2[cid]['top3_pass']: p2f3.append(cid)
    if not m1[cid]['top3_pass'] and m2[cid]['top3_pass']: f2p3.append(cid)
print(f'\n  top1 PASS->FAIL: {len(p2f1)}')
for cid in sorted(p2f1):
    print(f'    {cid} expected={m1[cid]["expected"]!r} iter2={m1[cid]["top1_family"]!r} redo={m2[cid]["top1_family"]!r}')
print(f'  top1 FAIL->PASS: {len(f2p1)}')
for cid in sorted(f2p1):
    print(f'    {cid} expected={m1[cid]["expected"]!r} iter2={m1[cid]["top1_family"]!r} redo={m2[cid]["top1_family"]!r}')
print(f'\n  top3 PASS->FAIL: {len(p2f3)}')
for cid in sorted(p2f3):
    print(f'    {cid} expected={m1[cid]["expected"]!r} iter2_top3={m1[cid]["top3_families"]} redo_top3={m2[cid]["top3_families"]}')
print(f'  top3 FAIL->PASS: {len(f2p3)}')
for cid in sorted(f2p3):
    print(f'    {cid} expected={m1[cid]["expected"]!r} iter2_top3={m1[cid]["top3_families"]} redo_top3={m2[cid]["top3_families"]}')

# Direct check of Category A cases
print('\n  Category A verify:')
for cid in ('adm_sandmeyer_reaction_1237','adm_schmidt_reaction_1240'):
    r = m2.get(cid)
    if r: print(f'    {cid} top1={r["top1_family"]!r} pass={r["top1_pass"]}')
    else: print(f'    {cid} MISSING')

# === COVERAGE ===
print('\n\n=== COVERAGE ===')
c1 = load(ITER2 / 'coverage' / 'corpus_coverage_results.json')
c2 = load(REDO  / 'coverage' / 'corpus_coverage_results.json')
sc1 = c1.get('summary', {}); sc2 = c2.get('summary', {})
for k in ('total_cases','recall_at_1','recall_at_3','recall_at_5','hit_at_1','hit_at_3','hit_at_5','no_confident_hit_cases'):
    print(f'  {k}: iter2={sc1.get(k)} -> redo={sc2.get(k)}')

def cvm(data):
    return {r['case_id']: {'expected': r.get('expected_family'),
                            'h1': bool(r.get('hit_at_1', False)),
                            'h3': bool(r.get('hit_at_3', False)),
                            'h5': bool(r.get('hit_at_5', False)),
                            'top5': r.get('top5_families', [])} for r in data.get('rows', [])}
n1 = cvm(c1); n2 = cvm(c2)
only1 = set(n1)-set(n2); only2 = set(n2)-set(n1); both = set(n1)&set(n2)
print(f'\n  dropped: {len(only1)}  added: {len(only2)}')
for cid in sorted(only1)[:10]:
    print(f'    DROP {cid} [h1={n1[cid]["h1"]} h5={n1[cid]["h5"]}]')
for cid in sorted(only2)[:10]:
    print(f'    ADD  {cid} [h1={n2[cid]["h1"]} h5={n2[cid]["h5"]}]')

for layer in ('h1','h3','h5'):
    p2f = [cid for cid in both if n1[cid][layer] and not n2[cid][layer]]
    f2p = [cid for cid in both if not n1[cid][layer] and n2[cid][layer]]
    print(f'\n  {layer}: PASS->FAIL: {len(p2f)} / FAIL->PASS: {len(f2p)}')
    for cid in sorted(p2f)[:10]:
        print(f'    P2F {cid} expected={n1[cid]["expected"]!r}')
    for cid in sorted(f2p)[:10]:
        print(f'    F2P {cid} expected={n1[cid]["expected"]!r}')

print('\n\n=== HARD GATE ===')
adm_ok = (s2.get('top1_accuracy', 0) or 0) >= 0.745
cov_ok = (sc2.get('recall_at_5', 0) or 0) >= 0.875
print(f'  adm_top1 {s2.get("top1_accuracy"):.4f} >= 0.745: {adm_ok}')
print(f'  cov_r5   {sc2.get("recall_at_5"):.4f} >= 0.875: {cov_ok}')
print(f'  Gate pass: {adm_ok and cov_ok}')
