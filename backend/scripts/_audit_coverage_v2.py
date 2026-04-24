"""Step 4 — 'Practical coverage': join PDF canonical to Gate C benchmark per-case
PASS/FAIL to measure what % of the PDF's named reactions CHEMLENS actually
recognizes correctly (not just has in the name list).

Reads:
  _audit_pdf_canonical.json
  _audit_coverage.json (tier A/B/C/D mapping)
  reports/phase4/gateC/apply_20260424_210600_redo/
      admission/family_admission_results.json  (256 rows, top1/top3)
      coverage/corpus_coverage_results.json    (512 rows, h@1/h@3/h@5)
      broad/broad_results.json                 (256 rows, top1/top3)

Coverage tiers (stacked, most strict first):
  T1 admission_top1  : PDF entry has ≥1 admission case where top1 = expected
  T2 admission_top3  : PDF entry has ≥1 admission case where top3 hit
  T3 coverage_h1     : PDF entry has ≥1 coverage case where h@1 = True
  T4 coverage_h5     : PDF entry has ≥1 coverage case where h@5 = True
  T5 name_known      : PDF name (or typo-corrected) is in CHEMLENS name pool
  T6 not_covered     : totally absent / unmappable
"""
import json, re, unicodedata
from pathlib import Path

PDF_J  = Path(r'C:\chemlens\backend\scripts\_audit_pdf_canonical.json')
COV_J  = Path(r'C:\chemlens\backend\scripts\_audit_coverage.json')
ADM_J  = Path(r'C:\chemlens\backend\reports\phase4\gateC\apply_20260424_210600_redo\admission\family_admission_results.json')
CVB_J  = Path(r'C:\chemlens\backend\reports\phase4\gateC\apply_20260424_210600_redo\coverage\corpus_coverage_results.json')
BRD_J  = Path(r'C:\chemlens\backend\reports\phase4\gateC\apply_20260424_210600_redo\broad\broad_results.json')
OUT    = Path(r'C:\chemlens\backend\scripts\_audit_coverage_practical.json')

# Manual typo patches for 2 missing PDF entries that match CHEMLENS after fix
PDF_TYPO_FIX = {
    'Bischler-Napieralski Isoqinoline Synthesis': 'Bischler-Napieralski Isoquinoline Synthesis',
    'Paterno-BÜChi Reaction': 'Paternò-Büchi Reaction',
}


def _norm(s):
    s = unicodedata.normalize('NFKC', s)
    s = s.replace('\u2019', "'").replace('\u2018', "'")
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def main():
    pdf = json.load(PDF_J.open(encoding='utf-8'))
    cov = json.load(COV_J.open(encoding='utf-8'))
    adm = json.load(ADM_J.open(encoding='utf-8'))
    cvb = json.load(CVB_J.open(encoding='utf-8'))
    brd = json.load(BRD_J.open(encoding='utf-8'))

    # Map pdf_name -> chemlens_canonical (from Step 3)
    pdf2canon = {r['pdf_name']: r['chemlens_canonical'] for r in cov['results']}
    # Apply typo fixes (these resolve the 2 D_missing rows)
    for bad, good in PDF_TYPO_FIX.items():
        if pdf2canon.get(bad) is None:
            pdf2canon[bad] = good

    # Aggregate benchmark per canonical family name
    fam_bench = {}  # canonical -> flags
    def _bump(fam, **kv):
        d = fam_bench.setdefault(fam, {
            'adm_cases': 0, 'adm_top1_hits': 0, 'adm_top3_hits': 0,
            'cov_cases': 0, 'cov_h1_hits': 0, 'cov_h3_hits': 0, 'cov_h5_hits': 0,
            'brd_cases': 0, 'brd_top1_hits': 0, 'brd_top3_hits': 0,
        })
        for k, v in kv.items():
            d[k] = d[k] + (1 if v else 0) if k.endswith('_hits') else d[k] + 1 if k.endswith('_cases') else v

    for r in adm['rows']:
        fam = r['expected_family']
        d = fam_bench.setdefault(fam, {
            'adm_cases': 0, 'adm_top1_hits': 0, 'adm_top3_hits': 0,
            'cov_cases': 0, 'cov_h1_hits': 0, 'cov_h3_hits': 0, 'cov_h5_hits': 0,
            'brd_cases': 0, 'brd_top1_hits': 0, 'brd_top3_hits': 0,
        })
        d['adm_cases'] += 1
        if r.get('top1_correct'): d['adm_top1_hits'] += 1
        if r.get('top3_correct'): d['adm_top3_hits'] += 1

    for r in cvb['rows']:
        fam = r['expected_family']
        d = fam_bench.setdefault(fam, {
            'adm_cases': 0, 'adm_top1_hits': 0, 'adm_top3_hits': 0,
            'cov_cases': 0, 'cov_h1_hits': 0, 'cov_h3_hits': 0, 'cov_h5_hits': 0,
            'brd_cases': 0, 'brd_top1_hits': 0, 'brd_top3_hits': 0,
        })
        d['cov_cases'] += 1
        if r.get('hit_at_1'): d['cov_h1_hits'] += 1
        if r.get('hit_at_3'): d['cov_h3_hits'] += 1
        if r.get('hit_at_5'): d['cov_h5_hits'] += 1

    for r in brd['rows']:
        fam = r['expected_family']
        d = fam_bench.setdefault(fam, {
            'adm_cases': 0, 'adm_top1_hits': 0, 'adm_top3_hits': 0,
            'cov_cases': 0, 'cov_h1_hits': 0, 'cov_h3_hits': 0, 'cov_h5_hits': 0,
            'brd_cases': 0, 'brd_top1_hits': 0, 'brd_top3_hits': 0,
        })
        d['brd_cases'] += 1
        if r.get('top1_correct'): d['brd_top1_hits'] += 1
        if r.get('top3_correct'): d['brd_top3_hits'] += 1

    # Classify each PDF entry
    rows = []
    tiers = {'T1_adm_top1': 0, 'T2_adm_top3': 0, 'T3_cov_h1': 0,
             'T4_cov_h5': 0, 'T5_name_only': 0, 'T6_missing': 0}
    for e in pdf['entries']:
        pdf_name = e['clean']
        canon = pdf2canon.get(pdf_name)
        fb = fam_bench.get(canon) if canon else None
        tier = None
        if fb:
            if fb['adm_top1_hits'] > 0: tier = 'T1_adm_top1'
            elif fb['adm_top3_hits'] > 0: tier = 'T2_adm_top3'
            elif fb['cov_h1_hits'] > 0: tier = 'T3_cov_h1'
            elif fb['cov_h5_hits'] > 0: tier = 'T4_cov_h5'
            elif (fb['adm_cases'] + fb['cov_cases'] + fb['brd_cases']) > 0:
                tier = 'T5_name_only'  # has cases but never scored
            else:
                tier = 'T5_name_only'
        elif canon:
            tier = 'T5_name_only'
        else:
            tier = 'T6_missing'
        tiers[tier] += 1
        rows.append({
            'pdf_name': pdf_name,
            'pdf_page': e['page'],
            'chemlens_canonical': canon,
            'tier': tier,
            'bench_stats': fb,
        })

    total = len(rows)
    summary = {
        'pdf_total_entries': total,
        'typo_fixes_applied': len(PDF_TYPO_FIX),
        'T1_admission_top1_PASS':     tiers['T1_adm_top1'],
        'T2_admission_top3_only':     tiers['T2_adm_top3'],
        'T3_coverage_h1_only':        tiers['T3_cov_h1'],
        'T4_coverage_h5_only':        tiers['T4_cov_h5'],
        'T5_name_present_only':       tiers['T5_name_only'],
        'T6_not_in_chemlens':         tiers['T6_missing'],
        'pct_strict_top1':            round(100 * tiers['T1_adm_top1'] / total, 2),
        'pct_top3_or_better':         round(100 * (tiers['T1_adm_top1'] + tiers['T2_adm_top3']) / total, 2),
        'pct_top5_or_better':         round(100 * sum(tiers[k] for k in ['T1_adm_top1','T2_adm_top3','T3_cov_h1','T4_cov_h5']) / total, 2),
        'pct_name_known':             round(100 * (total - tiers['T6_missing']) / total, 2),
    }
    OUT.write_text(json.dumps({'summary': summary, 'rows': rows}, ensure_ascii=False, indent=2), encoding='utf-8')
    print('=== PRACTICAL COVERAGE SUMMARY ===')
    for k, v in summary.items():
        print(f'  {k}: {v}')
    print('\n=== Per-tier detail ===')
    for tier_name in ['T5_name_only', 'T6_missing', 'T4_cov_h5', 'T3_cov_h1']:
        sub = [r for r in rows if r['tier'] == tier_name]
        if sub:
            print(f'\n{tier_name} — {len(sub)} entries:')
            for r in sub[:30]:
                b = r['bench_stats'] or {}
                print(f"  p{r['pdf_page']:>4}  {r['pdf_name']}  -> {r['chemlens_canonical']}  adm={b.get('adm_top1_hits',0)}/{b.get('adm_cases',0)}  cov_h5={b.get('cov_h5_hits',0)}/{b.get('cov_cases',0)}")


if __name__ == '__main__':
    main()
