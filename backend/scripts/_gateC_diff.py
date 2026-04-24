"""Gate C diff — compare iterate1 (259-case) vs Gate C (256-case) admission + coverage."""
import json
from pathlib import Path

ITER1 = Path(r'C:\chemlens\backend\reports\phase4\gateB\iterate1_20260424')
GATEC = Path(r'C:\chemlens\backend\reports\phase4\gateC\apply_20260424_204941')

def load(p):
    return json.loads(Path(p).read_text(encoding='utf-8-sig'))

# ============ ADMISSION ============
a1 = load(ITER1 / 'admission' / 'family_admission_results.json')
a2 = load(GATEC / 'admission' / 'family_admission_results.json')
print('=== ADMISSION ===')
s1 = a1.get('summary', {}); s2 = a2.get('summary', {})
for k in ('total_cases','top1_correct','top1_accuracy','top3_correct','top3_accuracy','confused_pairs'):
    print(f'  {k}: {s1.get(k)} -> {s2.get(k)}')

# Per-case diff (match on case_id; many will be missing because of merges)
def case_pass_map(data):
    out = {}
    for c in data.get('rows', []):
        cid = c['case_id']
        out[cid] = {
            'expected': c.get('expected_family'),
            'top1_family': c.get('top1_family'),
            'top1_pass': bool(c.get('top1_correct', False)),
            'top3_pass': bool(c.get('top3_correct', False)),
            'top3_families': c.get('top3_families', []),
            'raw_aliases': c.get('raw_family_aliases', []),
        }
    return out

m1 = case_pass_map(a1)
m2 = case_pass_map(a2)

only_in_1 = set(m1) - set(m2)
only_in_2 = set(m2) - set(m1)
both = set(m1) & set(m2)

print(f'\n  case_ids only in iterate1: {len(only_in_1)}')
for cid in sorted(only_in_1):
    r = m1[cid]
    print(f'    DROP {cid} [top1_pass={r["top1_pass"]}] expected={r["expected"]} got={r["top1_family"]}')

print(f'\n  case_ids only in gateC: {len(only_in_2)}')
for cid in sorted(only_in_2):
    r = m2[cid]
    print(f'    NEW  {cid} [top1_pass={r["top1_pass"]}] expected={r["expected"]} got={r["top1_family"]}')

# Flips
flipped_p2f = []
flipped_f2p = []
for cid in sorted(both):
    p1 = m1[cid]['top1_pass']
    p2 = m2[cid]['top1_pass']
    if p1 and not p2:
        flipped_p2f.append((cid, m1[cid], m2[cid]))
    elif not p1 and p2:
        flipped_f2p.append((cid, m1[cid], m2[cid]))

print(f'\n  top1 flipped PASS->FAIL: {len(flipped_p2f)}')
for cid, r1, r2 in flipped_p2f:
    print(f'    {cid}')
    print(f'      iter1: got={r1["top1_family"]!r}, expected={r1["expected"]!r}')
    print(f'      gateC: got={r2["top1_family"]!r}, expected={r2["expected"]!r}')

print(f'\n  top1 flipped FAIL->PASS: {len(flipped_f2p)}')
for cid, r1, r2 in flipped_f2p:
    print(f'    {cid}')
    print(f'      iter1: got={r1["top1_family"]!r}, expected={r1["expected"]!r}')
    print(f'      gateC: got={r2["top1_family"]!r}, expected={r2["expected"]!r}')

# top3 flips
flipped_p2f_t3 = []
flipped_f2p_t3 = []
for cid in sorted(both):
    p1 = m1[cid]['top3_pass']
    p2 = m2[cid]['top3_pass']
    if p1 and not p2:
        flipped_p2f_t3.append((cid, m1[cid], m2[cid]))
    elif not p1 and p2:
        flipped_f2p_t3.append((cid, m1[cid], m2[cid]))
print(f'\n  top3 flipped PASS->FAIL: {len(flipped_p2f_t3)}')
for cid, r1, r2 in flipped_p2f_t3:
    print(f'    {cid} expected={r2["expected"]!r}')
    print(f'      iter1 top3: {r1["top3_families"]}')
    print(f'      gateC top3: {r2["top3_families"]}')
print(f'  top3 flipped FAIL->PASS: {len(flipped_f2p_t3)}')
for cid, r1, r2 in flipped_f2p_t3:
    print(f'    {cid} expected={r2["expected"]!r}')
    print(f'      iter1 top3: {r1["top3_families"]}')
    print(f'      gateC top3: {r2["top3_families"]}')

# ============ COVERAGE ============
print('\n\n=== COVERAGE ===')
c1 = load(ITER1 / 'coverage' / 'corpus_coverage_results.json')
c2 = load(GATEC / 'coverage' / 'corpus_coverage_results.json')
sc1 = c1.get('summary', {}); sc2 = c2.get('summary', {})
for k in ('total_cases','recall_at_1','recall_at_3','recall_at_5','hit_at_1','hit_at_3','hit_at_5','no_confident_hit_cases'):
    print(f'  {k}: {sc1.get(k)} -> {sc2.get(k)}')

def cov_case_map(data):
    out = {}
    for c in data.get('rows', []):
        cid = c['case_id']
        out[cid] = {
            'expected': c.get('expected_family'),
            'hit_at_1': bool(c.get('hit_at_1', False)),
            'hit_at_5': bool(c.get('hit_at_5', False)),
            'top1_family': c.get('top1_family'),
            'top5_families': c.get('top5_families', []),
        }
    return out

n1 = cov_case_map(c1)
n2 = cov_case_map(c2)
only_in_1 = set(n1) - set(n2)
only_in_2 = set(n2) - set(n1)
both = set(n1) & set(n2)

print(f'\n  cov cases only in iterate1: {len(only_in_1)} (expected 6 due to macrolact+ullmann drops)')
for cid in sorted(only_in_1):
    r = n1[cid]
    print(f'    DROP {cid} [h@1={r["hit_at_1"]} h@5={r["hit_at_5"]}] expected={r["expected"]}')
print(f'\n  cov cases only in gateC: {len(only_in_2)}')
for cid in sorted(only_in_2):
    r = n2[cid]
    print(f'    NEW  {cid} [h@1={r["hit_at_1"]} h@5={r["hit_at_5"]}] expected={r["expected"]}')

cov_p2f_h1 = []
cov_f2p_h1 = []
cov_p2f_h5 = []
cov_f2p_h5 = []
for cid in both:
    if n1[cid]['hit_at_1'] and not n2[cid]['hit_at_1']:
        cov_p2f_h1.append(cid)
    if not n1[cid]['hit_at_1'] and n2[cid]['hit_at_1']:
        cov_f2p_h1.append(cid)
    if n1[cid]['hit_at_5'] and not n2[cid]['hit_at_5']:
        cov_p2f_h5.append(cid)
    if not n1[cid]['hit_at_5'] and n2[cid]['hit_at_5']:
        cov_f2p_h5.append(cid)

print(f'\n  cov hit@1 PASS->FAIL: {len(cov_p2f_h1)} / FAIL->PASS: {len(cov_f2p_h1)}')
for cid in sorted(cov_p2f_h1)[:10]:
    print(f'    P2F h@1: {cid} expected={n1[cid]["expected"]}')
for cid in sorted(cov_f2p_h1)[:10]:
    print(f'    F2P h@1: {cid} expected={n1[cid]["expected"]}')

print(f'\n  cov hit@5 PASS->FAIL: {len(cov_p2f_h5)} / FAIL->PASS: {len(cov_f2p_h5)}')
for cid in sorted(cov_p2f_h5)[:20]:
    print(f'    P2F h@5: {cid} expected={n1[cid]["expected"]}')
for cid in sorted(cov_f2p_h5)[:20]:
    print(f'    F2P h@5: {cid} expected={n1[cid]["expected"]}')

# Verdict
print('\n\n=== Gate C verdict ===')
adm_pass1 = (s2.get('top1_accuracy', 0) or 0) >= 0.745
cov_pass5 = (sc2.get('recall_at_5', 0) or 0) >= 0.875
print(f'  adm_top1 {s2.get("top1_accuracy"):.4f} >= 0.745: {adm_pass1}')
print(f'  cov_r5   {sc2.get("recall_at_5"):.4f} >= 0.875: {cov_pass5}')
print(f'  Gate pass (hard thresholds): {adm_pass1 and cov_pass5}')
