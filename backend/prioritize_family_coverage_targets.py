
import sqlite3, json, re, unicodedata, os
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DB = ROOT / 'app' / 'labint.db'
OUT_JSON = ROOT / 'FAMILY_COVERAGE_PRIORITY_REPORT.json'
OUT_MD = ROOT / 'FAMILY_COVERAGE_PRIORITY_REPORT.md'
OUT_CARDS = ROOT / 'REACTION_CARDS_SMILES_AUDIT.json'

SKIP_FAMILIES = {'Appendix'}
SKIP_RE = re.compile(r'^VIII\.|^IX\.|^X\.|^XI\.|^XII\.|^XIII\.', re.I)
GENERIC_RE = re.compile(r'\b(reactant|product|intermediate|compound|derivative|adduct|favored|unfavored|solvent|catalyst|reagent|acid or base|steps|temperature|heat|hv|mixture|salt)\b', re.I)
PLACEHOLDER_RE = re.compile(r'\b(R\d*|Ar|Het|EWG|PG|LG|X|Y|Z)\b|\[\*', re.I)


def norm(s: str) -> str:
    s = unicodedata.normalize('NFKD', s or '').lower()
    s = s.replace('&', ' and ')
    s = re.sub(r'[^a-z0-9]+', ' ', s)
    return ' '.join(s.split())


def is_manual_family(s: str) -> bool:
    s = (s or '').strip()
    return bool(s) and s not in SKIP_FAMILIES and not SKIP_RE.search(s)


def looks_like_exact_candidate(text: str) -> bool:
    t = (text or '').strip()
    if not t or ';' in t or ',' in t or len(t) > 80:
        return False
    if GENERIC_RE.search(t):
        return False
    if PLACEHOLDER_RE.search(t):
        return False
    words = t.split()
    return 1 <= len(words) <= 5


conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

manual_families = [r['reference_family_name'] for r in conn.execute(
    "select distinct reference_family_name from manual_page_knowledge where trim(coalesce(reference_family_name,''))<>''"
) if is_manual_family(r['reference_family_name'])]

queryable_families = [r['reaction_family_name'] for r in conn.execute(
    "select distinct rx.reaction_family_name from extract_molecules em join reaction_extracts rx on rx.id=em.extract_id where em.queryable=1 and trim(coalesce(rx.reaction_family_name,''))<>''"
)]

raw_overlap = len(set(manual_families) & set(queryable_families))
queryable_norm = {norm(x): x for x in queryable_families}
normalized_overlap = 0
uncovered = []
for fam in manual_families:
    if norm(fam) in queryable_norm:
        normalized_overlap += 1
    else:
        uncovered.append(fam)

priority_rows = []
for fam in uncovered:
    manual_rows = conn.execute("select count(*) from manual_page_knowledge where reference_family_name=?", (fam,)).fetchone()[0]
    extract_rows = conn.execute("select count(*) from reaction_extracts where reaction_family_name=?", (fam,)).fetchone()[0]
    tier3_rows = conn.execute("""
        select count(*) from extract_molecules em
        join reaction_extracts rx on rx.id=em.extract_id
        where rx.reaction_family_name=? and em.queryable=0 and em.quality_tier=3
    """, (fam,)).fetchone()[0]
    candidates = []
    exact_like_score = 0
    for r in conn.execute("""
        select em.normalized_text txt, em.role, count(*) c
        from extract_molecules em
        join reaction_extracts rx on rx.id=em.extract_id
        where rx.reaction_family_name=? and em.queryable=0 and em.quality_tier=3 and trim(coalesce(em.normalized_text,''))<>''
        group by txt, role order by c desc
    """, (fam,)):
        txt = (r['txt'] or '').strip()
        if len(candidates) < 8:
            candidates.append({'text': txt, 'role': r['role'], 'count': r['c']})
        if looks_like_exact_candidate(txt):
            exact_like_score += r['c']
    priority_score = manual_rows * 5 + extract_rows * 3 + min(exact_like_score, 10) * 2
    priority_rows.append({
        'family': fam,
        'manual_rows': manual_rows,
        'extract_rows': extract_rows,
        'tier3_rows': tier3_rows,
        'exact_like_score': exact_like_score,
        'priority_score': priority_score,
        'sample_candidates': candidates,
    })

priority_rows.sort(key=lambda x: (-x['priority_score'], -x['exact_like_score'], -x['extract_rows'], x['family']))

audit_cards = conn.execute("""
    select count(*) total,
           sum(case when trim(coalesce(substrate_smiles,''))<>'' or trim(coalesce(product_smiles,''))<>'' then 1 else 0 end) with_any_smiles,
           sum(case when trim(coalesce(substrate_smiles,''))<>'' and trim(coalesce(product_smiles,''))<>'' then 1 else 0 end) with_both
    from reaction_cards
""").fetchone()

card_sources = [tuple(r) for r in conn.execute("select coalesce(source,'NULL'), count(*) from reaction_cards group by coalesce(source,'NULL') order by 2 desc")]
card_examples = [dict(r) for r in conn.execute("""
    select id, title, transformation, substrate_smiles, product_smiles, source
    from reaction_cards
    where trim(coalesce(substrate_smiles,''))<>'' or trim(coalesce(product_smiles,''))<>''
    limit 20
""")]

report = {
    'db': str(DB),
    'manual_family_total_raw': len(manual_families),
    'queryable_family_total_raw': len(queryable_families),
    'manual_family_covered_raw_exact': raw_overlap,
    'manual_family_covered_normalized': normalized_overlap,
    'manual_family_uncovered_normalized': len(uncovered),
    'top_priority_uncovered_families': priority_rows[:25],
}

cards_report = {
    'reaction_cards_total': audit_cards['total'],
    'reaction_cards_with_any_smiles': audit_cards['with_any_smiles'],
    'reaction_cards_with_both_smiles': audit_cards['with_both'],
    'reaction_cards_source_counts': card_sources,
    'example_smiles_cards': card_examples,
}

OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
OUT_CARDS.write_text(json.dumps(cards_report, ensure_ascii=False, indent=2), encoding='utf-8')

lines = []
lines.append('# FAMILY COVERAGE PRIORITY REPORT')
lines.append('')
lines.append(f'- manual families (raw): {len(manual_families)}')
lines.append(f'- queryable families (raw): {len(queryable_families)}')
lines.append(f'- covered (raw exact match): {raw_overlap}')
lines.append(f'- covered (normalized match): {normalized_overlap}')
lines.append(f'- uncovered (normalized): {len(uncovered)}')
lines.append('')
lines.append('## Why this matters')
lines.append('')
lines.append('- The previous simple report undercounts covered manual families when naming variants differ.')
lines.append('- Use the normalized count for planning, but keep the raw names unchanged in the DB.')
lines.append('')
lines.append('## Top uncovered families to target next')
lines.append('')
for item in priority_rows[:15]:
    lines.append(f"### {item['family']}")
    lines.append(f"- priority_score: {item['priority_score']}")
    lines.append(f"- manual_rows: {item['manual_rows']}")
    lines.append(f"- extract_rows: {item['extract_rows']}")
    lines.append(f"- tier3_rows: {item['tier3_rows']}")
    lines.append(f"- exact_like_score: {item['exact_like_score']}")
    if item['sample_candidates']:
        lines.append('- sample candidates:')
        for c in item['sample_candidates'][:5]:
            lines.append(f"  - [{c['role']}] {c['text']} (count={c['count']})")
    lines.append('')
lines.append('## Reaction cards SMILES audit')
lines.append('')
lines.append(f"- total cards: {audit_cards['total']}")
lines.append(f"- cards with any smiles: {audit_cards['with_any_smiles']}")
lines.append(f"- cards with both smiles: {audit_cards['with_both']}")
lines.append('')
lines.append('## Recommended next action')
lines.append('')
lines.append('1. Do NOT mass-backfill reaction_cards yet.')
lines.append('2. Use the top uncovered family list to guide manual dataization from image batches.')
lines.append('3. Prefer families with both extract_rows and exact_like_score > 0 first.')
OUT_MD.write_text('\n'.join(lines), encoding='utf-8')

print(json.dumps({
    'report_json': str(OUT_JSON),
    'report_md': str(OUT_MD),
    'cards_audit_json': str(OUT_CARDS),
    'manual_family_covered_raw_exact': raw_overlap,
    'manual_family_covered_normalized': normalized_overlap,
    'manual_family_total_raw': len(manual_families),
    'next_best_targets': [x['family'] for x in priority_rows[:6]],
    'reaction_cards_with_any_smiles': audit_cards['with_any_smiles'],
    'reaction_cards_total': audit_cards['total'],
}, ensure_ascii=False, indent=2))
