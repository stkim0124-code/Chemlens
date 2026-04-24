"""Re-audit PDF coverage vs CHEMLENS post-Option F. Same method as
`_audit_coverage_v2.py` but reads Option F bench outputs.
"""
from __future__ import annotations
import json, re, unicodedata
from pathlib import Path

ROOT   = Path(r'C:\chemlens\backend')
SNAPF  = Path((ROOT / 'scripts' / '_optionF_snapdir.txt').read_text(encoding='utf-8').strip())
PDF_J  = ROOT / 'scripts' / '_audit_pdf_canonical.json'
FSV_P  = ROOT / 'final_state_verifier.py'
CHE_J  = ROOT / 'scripts' / '_audit_chemlens_families_optionF.json'
ADM_J  = SNAPF / 'admission' / 'family_admission_results.json'
BRD_J  = SNAPF / 'broad'     / 'broad_results.json'
CVB_J  = SNAPF / 'coverage'  / 'corpus_coverage_results.json'
OUT_NM = ROOT / 'scripts' / '_audit_coverage_optionF_name.json'
OUT_PR = ROOT / 'scripts' / '_audit_coverage_optionF_practical.json'

PDF_TYPO_FIX = {
    'Bischler-Napieralski Isoqinoline Synthesis': 'Bischler-Napieralski Isoquinoline Synthesis',
    'Paterno-BÜChi Reaction': 'Paternò-Büchi Reaction',
}


def parse_alias_map(text):
    m = re.search(r'MANUAL_ALIAS_OVERRIDES\s*=\s*\{(.*?)^\}', text, re.S | re.M)
    out = {}
    for line in m.group(1).splitlines():
        s = line.split('#', 1)[0].strip()
        if not s or not s.startswith(("'", '"')): continue
        mm = re.match(r"""['"](.+?)['"]\s*:\s*['"](.+?)['"]""", s)
        if mm: out[mm.group(1)] = mm.group(2)
    return out


def _norm(s):
    s = unicodedata.normalize('NFKC', s)
    s = s.replace('\u2019', "'").replace('\u2018', "'")
    return re.sub(r'\s+', ' ', s).strip()


def _keyify(s):
    s = _norm(s).lower()
    s = re.sub(r'\([^)]*\)', '', s)
    s = re.sub(r'[^a-z0-9]+', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


def _alt_keys(name):
    name = _norm(name)
    yield _keyify(name)
    for splitter in [r'\s*\(', r'\s+-\s+', r'\s+/\s+', r'\s*&\s*', r'\s+–\s+']:
        parts = re.split(splitter, name, maxsplit=1)
        if len(parts) > 1 and len(parts[0]) > 3: yield _keyify(parts[0])
    m = re.search(r'\(([^()]+)\)', name)
    if m: yield _keyify(m.group(1))
    trimmed = re.sub(r'\s*(reaction|synthesis|rearrangement|condensation|reduction|oxidation|coupling|olefination|elimination|cycloaddition|modification)\b\.?\s*$', '', name, flags=re.I)
    if trimmed != name and len(trimmed) > 3: yield _keyify(trimmed)


def main():
    pdf = json.load(PDF_J.open(encoding='utf-8'))
    che = json.load(CHE_J.open(encoding='utf-8'))
    alias = parse_alias_map(FSV_P.read_text(encoding='utf-8'))
    print(f'alias_map entries: {len(alias)}')

    expected = set(che['expected_families'])
    surfaced = set(che['surfaced_families'])

    che_all = set(expected | surfaced)
    for k, v in alias.items():
        che_all.add(k); che_all.add(v)

    order = {'expected': 3, 'surfaced': 2, 'alias_only': 1}
    che_index = {}
    for name in sorted(che_all):
        canonical = alias.get(name, name)
        flavor = 'expected' if canonical in expected else ('surfaced' if canonical in surfaced else 'alias_only')
        for k in _alt_keys(name):
            if not k: continue
            if k in che_index:
                if order[flavor] > order[che_index[k][1]]: che_index[k] = (canonical, flavor)
            else:
                che_index[k] = (canonical, flavor)

    # Name-level tier
    name_rows = []
    ncnt = {'A_deep': 0, 'B_surfaced': 0, 'C_alias_only': 0, 'D_missing': 0}
    for e in pdf['entries']:
        pdf_name = e['clean']
        hit, hit_key = None, None
        for k in _alt_keys(pdf_name):
            if k in che_index:
                hit = che_index[k]; hit_key = k; break
        if not hit:
            for alias_name in e['aliases']:
                for k in _alt_keys(alias_name):
                    if k in che_index:
                        hit = che_index[k]; hit_key = k; break
                if hit: break
        # typo patch
        if not hit and pdf_name in PDF_TYPO_FIX:
            for k in _alt_keys(PDF_TYPO_FIX[pdf_name]):
                if k in che_index:
                    hit = che_index[k]; hit_key = k; break
        if hit:
            canon, flavor = hit
            tier = {'expected':'A_deep','surfaced':'B_surfaced','alias_only':'C_alias_only'}[flavor]
        else:
            canon = None; tier = 'D_missing'
        ncnt[tier] += 1
        name_rows.append({'pdf_name': pdf_name, 'pdf_page': e['page'], 'chemlens_canonical': canon, 'tier': tier, 'match_key': hit_key})

    n_total = len(name_rows)
    n_summary = {
        'pdf_total': n_total,
        'A_deep_queryable': ncnt['A_deep'],
        'B_surfaced_candidate': ncnt['B_surfaced'],
        'C_alias_only': ncnt['C_alias_only'],
        'D_missing': ncnt['D_missing'],
        'coverage_strict_pct': round(100 * ncnt['A_deep'] / n_total, 2),
        'coverage_incl_aliases_pct': round(100 * (ncnt['A_deep'] + ncnt['B_surfaced'] + ncnt['C_alias_only']) / n_total, 2),
    }
    OUT_NM.write_text(json.dumps({'summary': n_summary, 'results': name_rows}, ensure_ascii=False, indent=2), encoding='utf-8')

    # Practical tier
    adm = json.load(ADM_J.open(encoding='utf-8'))
    brd = json.load(BRD_J.open(encoding='utf-8'))
    cvb = json.load(CVB_J.open(encoding='utf-8'))

    pdf2canon = {r['pdf_name']: r['chemlens_canonical'] for r in name_rows}
    for bad, good in PDF_TYPO_FIX.items():
        if pdf2canon.get(bad) is None: pdf2canon[bad] = good

    fam_bench = {}
    def _init(fam):
        return fam_bench.setdefault(fam, {
            'adm_cases': 0, 'adm_top1_hits': 0, 'adm_top3_hits': 0,
            'cov_cases': 0, 'cov_h1_hits': 0, 'cov_h3_hits': 0, 'cov_h5_hits': 0,
            'brd_cases': 0, 'brd_top1_hits': 0, 'brd_top3_hits': 0,
        })
    for r in adm['rows']:
        d = _init(r['expected_family']); d['adm_cases'] += 1
        if r.get('top1_correct'): d['adm_top1_hits'] += 1
        if r.get('top3_correct'): d['adm_top3_hits'] += 1
    for r in cvb['rows']:
        d = _init(r['expected_family']); d['cov_cases'] += 1
        if r.get('hit_at_1'): d['cov_h1_hits'] += 1
        if r.get('hit_at_3'): d['cov_h3_hits'] += 1
        if r.get('hit_at_5'): d['cov_h5_hits'] += 1
    for r in brd['rows']:
        d = _init(r['expected_family']); d['brd_cases'] += 1
        if r.get('top1_correct'): d['brd_top1_hits'] += 1
        if r.get('top3_correct'): d['brd_top3_hits'] += 1

    rows = []
    tcnt = {'T1_adm_top1':0,'T2_adm_top3':0,'T3_cov_h1':0,'T4_cov_h5':0,'T5_name_only':0,'T6_missing':0}
    for e in pdf['entries']:
        pdf_name = e['clean']; canon = pdf2canon.get(pdf_name); fb = fam_bench.get(canon) if canon else None
        if fb:
            if fb['adm_top1_hits'] > 0: tier = 'T1_adm_top1'
            elif fb['adm_top3_hits'] > 0: tier = 'T2_adm_top3'
            elif fb['cov_h1_hits'] > 0: tier = 'T3_cov_h1'
            elif fb['cov_h5_hits'] > 0: tier = 'T4_cov_h5'
            else: tier = 'T5_name_only'
        elif canon: tier = 'T5_name_only'
        else: tier = 'T6_missing'
        tcnt[tier] += 1
        rows.append({'pdf_name': pdf_name, 'pdf_page': e['page'], 'chemlens_canonical': canon, 'tier': tier, 'bench_stats': fb})

    total = len(rows)
    p_summary = {
        'pdf_total': total,
        'typo_fixes_applied': len(PDF_TYPO_FIX),
        'T1_admission_top1': tcnt['T1_adm_top1'],
        'T2_admission_top3_only': tcnt['T2_adm_top3'],
        'T3_coverage_h1_only': tcnt['T3_cov_h1'],
        'T4_coverage_h5_only': tcnt['T4_cov_h5'],
        'T5_name_only_fail': tcnt['T5_name_only'],
        'T6_missing': tcnt['T6_missing'],
        'pct_strict_top1': round(100*tcnt['T1_adm_top1']/total,2),
        'pct_top3_or_better': round(100*(tcnt['T1_adm_top1']+tcnt['T2_adm_top3'])/total,2),
        'pct_top5_or_better': round(100*sum(tcnt[k] for k in ['T1_adm_top1','T2_adm_top3','T3_cov_h1','T4_cov_h5'])/total,2),
        'pct_name_known': round(100*(total-tcnt['T6_missing'])/total,2),
    }
    OUT_PR.write_text(json.dumps({'summary': p_summary, 'rows': rows}, ensure_ascii=False, indent=2), encoding='utf-8')

    print('\n=== NAME-LEVEL COVERAGE (Option F) ===')
    for k, v in n_summary.items(): print(f'  {k}: {v}')
    print('\n=== PRACTICAL COVERAGE (Option F) ===')
    for k, v in p_summary.items(): print(f'  {k}: {v}')
    print('\n=== Residual T5 (still failing) ===')
    for r in rows:
        if r['tier'] == 'T5_name_only':
            b = r['bench_stats'] or {}
            print(f"  p{r['pdf_page']:>4}  {r['pdf_name']}  -> {r['chemlens_canonical']}  adm={b.get('adm_top1_hits',0)}/{b.get('adm_cases',0)}  cov_h5={b.get('cov_h5_hits',0)}/{b.get('cov_cases',0)}")


if __name__ == '__main__':
    main()
