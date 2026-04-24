"""Step 3 — Cross-match PDF canonical vs CHEMLENS queryable + compute coverage.

Reads:
  _audit_pdf_canonical.json     — 250 PDF Section VII L3 entries
  _audit_chemlens_families.json — union of expected/surfaced families from Gate C bench
  final_state_verifier.py       — MANUAL_ALIAS_OVERRIDES dict

Writes:
  _audit_coverage.json — summary + per-entry tier classification

Tiers:
  A_deep       — PDF entry maps to a CHEMLENS family that has test cases (expected)
  B_surfaced   — PDF entry maps to a candidate that only surfaces in top-N (no cases)
  C_alias_only — PDF entry only hits through an alias-override mapping
  D_missing    — no mapping at all
"""
import json, re, unicodedata
from pathlib import Path

PDF_J = Path(r'C:\chemlens\backend\scripts\_audit_pdf_canonical.json')
CHE_J = Path(r'C:\chemlens\backend\scripts\_audit_chemlens_families.json')
FSV_P = Path(r'C:\chemlens\backend\final_state_verifier.py')
OUT   = Path(r'C:\chemlens\backend\scripts\_audit_coverage.json')


def parse_alias_map(text):
    m = re.search(r'MANUAL_ALIAS_OVERRIDES\s*=\s*\{(.*?)^\}', text, re.S | re.M)
    body = m.group(1)
    out = {}
    for line in body.splitlines():
        s = line.split('#', 1)[0].strip()
        if not s or not s.startswith(("'", '"')):
            continue
        mm = re.match(r"""['"](.+?)['"]\s*:\s*['"](.+?)['"]""", s)
        if mm:
            out[mm.group(1)] = mm.group(2)
    return out


def _norm(s):
    s = unicodedata.normalize('NFKC', s)
    s = s.replace('\u2019', "'").replace('\u2018', "'")
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _keyify(s):
    s = _norm(s).lower()
    s = re.sub(r'\([^)]*\)', '', s)
    s = re.sub(r'[^a-z0-9]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _alt_keys(name):
    name = _norm(name)
    yield _keyify(name)
    for splitter in [r'\s*\(', r'\s+-\s+', r'\s+/\s+', r'\s*&\s*']:
        parts = re.split(splitter, name, maxsplit=1)
        if len(parts) > 1 and len(parts[0]) > 3:
            yield _keyify(parts[0])
    m = re.search(r'\(([^()]+)\)', name)
    if m:
        yield _keyify(m.group(1))
    trimmed = re.sub(
        r'\s*(reaction|synthesis|rearrangement|condensation|reduction|oxidation|coupling|olefination|elimination|cycloaddition)\b\.?\s*$',
        '', name, flags=re.I,
    )
    if trimmed != name and len(trimmed) > 3:
        yield _keyify(trimmed)


def main():
    pdf = json.load(PDF_J.open(encoding='utf-8'))
    che = json.load(CHE_J.open(encoding='utf-8'))
    fsv_text = FSV_P.read_text(encoding='utf-8')
    alias_map = parse_alias_map(fsv_text)
    print(f'alias_map entries: {len(alias_map)}')

    expected = set(che['expected_families'])
    surfaced = set(che['surfaced_families'])
    union = expected | surfaced

    che_all = set(union)
    for k, v in alias_map.items():
        che_all.add(k)
        che_all.add(v)

    # Index: key -> (canonical, flavor)
    order = {'expected': 3, 'surfaced': 2, 'alias_only': 1}
    che_index = {}
    for name in sorted(che_all):
        canonical = alias_map.get(name, name)
        if canonical in expected:
            flavor = 'expected'
        elif canonical in surfaced:
            flavor = 'surfaced'
        else:
            flavor = 'alias_only'
        for k in _alt_keys(name):
            if not k:
                continue
            if k in che_index:
                if order[flavor] > order[che_index[k][1]]:
                    che_index[k] = (canonical, flavor)
            else:
                che_index[k] = (canonical, flavor)

    results = []
    tier_counts = {'A_deep': 0, 'B_surfaced': 0, 'C_alias_only': 0, 'D_missing': 0}
    for e in pdf['entries']:
        pdf_name = e['clean']
        hit = None
        hit_key = None
        for k in _alt_keys(pdf_name):
            if k in che_index:
                hit = che_index[k]
                hit_key = k
                break
        if not hit:
            for alias in e['aliases']:
                for k in _alt_keys(alias):
                    if k in che_index:
                        hit = che_index[k]
                        hit_key = k
                        break
                if hit:
                    break
        if hit:
            canonical, flavor = hit
            tier = {'expected': 'A_deep', 'surfaced': 'B_surfaced', 'alias_only': 'C_alias_only'}[flavor]
        else:
            canonical = None
            tier = 'D_missing'
        tier_counts[tier] += 1
        results.append({
            'pdf_name': pdf_name,
            'pdf_page': e['page'],
            'chemlens_canonical': canonical,
            'match_key': hit_key,
            'tier': tier,
        })

    total = len(results)
    summary = {
        'pdf_total_entries': total,
        'tier_A_deep_queryable': tier_counts['A_deep'],
        'tier_B_surfaced_candidate_only': tier_counts['B_surfaced'],
        'tier_C_alias_only_mapping': tier_counts['C_alias_only'],
        'tier_D_missing': tier_counts['D_missing'],
        'coverage_strict_pct': round(100 * tier_counts['A_deep'] / total, 2),
        'coverage_broad_pct': round(100 * (tier_counts['A_deep'] + tier_counts['B_surfaced']) / total, 2),
        'coverage_including_aliases_pct': round(
            100 * (tier_counts['A_deep'] + tier_counts['B_surfaced'] + tier_counts['C_alias_only']) / total, 2,
        ),
    }
    OUT.write_text(json.dumps({'summary': summary, 'results': results}, ensure_ascii=False, indent=2), encoding='utf-8')
    print('\n=== COVERAGE SUMMARY ===')
    for k, v in summary.items():
        print(f'  {k}: {v}')
    print('\n=== Missing (tier D) — all ===')
    for r in results:
        if r['tier'] == 'D_missing':
            print(f"  p{r['pdf_page']:>4}  {r['pdf_name']}")


if __name__ == '__main__':
    main()
