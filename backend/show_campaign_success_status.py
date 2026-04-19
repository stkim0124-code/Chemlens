#!/usr/bin/env python
from __future__ import annotations
import argparse, json, sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / 'app' / 'labint.db'
CAMPAIGN_FAMILIES = [
    'Diels-Alder Cycloaddition',
    'Fries Rearrangement',
    'Houben-Hoesch Reaction',
    'Finkelstein Reaction',
    'Hunsdiecker Reaction',
    'Claisen Condensation / Claisen Reaction',
    'Horner-Wadsworth-Emmons Olefination',
    'Krapcho Dealkoxycarbonylation',
    'Michael Addition Reaction',
    'Regitz Diazo Transfer',
    'Enyne Metathesis',
    'Hofmann-Loffler-Freytag Reaction',
    'Mitsunobu Reaction',
]

def get_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f'PRAGMA table_info({table})').fetchall()}

def pick(cols: set[str], *names: str) -> str | None:
    for n in names:
        if n in cols:
            return n
    return None

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default=str(DEFAULT_DB))
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    db = Path(args.db)
    conn = sqlite3.connect(str(db))
    try:
        em_cols = get_cols(conn, 'extract_molecules')
        re_cols = get_cols(conn, 'reaction_extracts')
        fam_col = pick(re_cols, 'reaction_family_name', 'family_name', 'family')
        query_col = pick(em_cols, 'queryable', 'is_queryable')
        source_col = pick(em_cols, 'structure_source') or 'structure_source'
        rows = []
        for fam in CAMPAIGN_FAMILIES:
            r = conn.execute(
                f"""
                SELECT COUNT(DISTINCT re.id),
                       SUM(CASE WHEN em.{query_col}=1 THEN 1 ELSE 0 END),
                       SUM(CASE WHEN em.{source_col}='gemini_salvage_seed' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN em.{source_col}='gemini_rebuild_seed' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN em.{source_col}='gemini_auto_seed' THEN 1 ELSE 0 END)
                FROM reaction_extracts re
                LEFT JOIN extract_molecules em ON em.extract_id=re.id
                WHERE re.{fam_col}=?
                """,
                (fam,),
            ).fetchone()
            rows.append({
                'family': fam,
                'extract_count': int(r[0] or 0),
                'queryable_molecule_count': int(r[1] or 0),
                'salvage_seed_count': int(r[2] or 0),
                'rebuild_seed_count': int(r[3] or 0),
                'auto_seed_count': int(r[4] or 0),
                'resolved': int(r[1] or 0) > 0,
            })
        payload = {
            'db': str(db),
            'resolved_count': sum(1 for x in rows if x['resolved']),
            'total_campaign_families': len(rows),
            'families': rows,
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print('=' * 72)
            print('CAMPAIGN SUCCESS STATUS')
            print('=' * 72)
            print(f"db: {db}")
            print(f"resolved: {payload['resolved_count']}/{payload['total_campaign_families']}")
            for x in rows:
                tag = 'RESOLVED' if x['resolved'] else 'UNRESOLVED'
                print(f"- {tag:10s} | {x['family']} | extracts={x['extract_count']} | queryable={x['queryable_molecule_count']} | salvage={x['salvage_seed_count']} | rebuild={x['rebuild_seed_count']} | auto={x['auto_seed_count']}")
            print('=' * 72)
    finally:
        conn.close()
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
