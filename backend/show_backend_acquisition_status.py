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


def scalar(conn: sqlite3.Connection, sql: str, params: tuple = ()):
    return conn.execute(sql, params).fetchone()[0]


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
    conn.row_factory = sqlite3.Row
    try:
        em_cols = get_cols(conn, 'extract_molecules')
        re_cols = get_cols(conn, 'reaction_extracts')
        fp_cols = get_cols(conn, 'reaction_family_patterns') if scalar(conn, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='reaction_family_patterns'") else set()

        fam_col = pick(re_cols, 'reaction_family_name', 'family_name', 'family')
        query_col = pick(em_cols, 'queryable', 'is_queryable')
        tier_col = pick(em_cols, 'quality_tier', 'structure_tier')
        source_col = pick(em_cols, 'structure_source')

        total_registry_families = scalar(conn, 'SELECT COUNT(*) FROM reaction_family_patterns') if fp_cols else 0
        total_extracts = scalar(conn, 'SELECT COUNT(*) FROM reaction_extracts')
        total_molecules = scalar(conn, 'SELECT COUNT(*) FROM extract_molecules')
        queryable = scalar(conn, f'SELECT COUNT(*) FROM extract_molecules WHERE {query_col}=1') if query_col else None
        tier1 = scalar(conn, f"SELECT COUNT(*) FROM extract_molecules WHERE {tier_col} IN (1, '1', 'tier1')") if tier_col else None
        tier2 = scalar(conn, f"SELECT COUNT(*) FROM extract_molecules WHERE {tier_col} IN (2, '2', 'tier2')") if tier_col else None
        tier3 = scalar(conn, f"SELECT COUNT(*) FROM extract_molecules WHERE {tier_col} IN (3, '3', 'tier3')") if tier_col else None
        family_coverage = scalar(conn, f"SELECT COUNT(DISTINCT re.{fam_col}) FROM reaction_extracts re WHERE EXISTS (SELECT 1 FROM extract_molecules em WHERE em.extract_id=re.id AND em.{query_col}=1)") if fam_col and query_col else None

        structure_source_counts = []
        if source_col:
            structure_source_counts = [dict(source=r[0], count=r[1]) for r in conn.execute(
                f"SELECT COALESCE({source_col}, 'NULL'), COUNT(*) FROM extract_molecules GROUP BY COALESCE({source_col}, 'NULL') ORDER BY COUNT(*) DESC"
            ).fetchall()]

        campaign_rows = []
        if fam_col:
            for fam in CAMPAIGN_FAMILIES:
                row = conn.execute(
                    f"""
                    SELECT COUNT(DISTINCT re.id),
                           SUM(CASE WHEN em.{query_col}=1 THEN 1 ELSE 0 END),
                           SUM(CASE WHEN em.{source_col}='gemini_salvage_seed' THEN 1 ELSE 0 END) AS salvage_cnt,
                           SUM(CASE WHEN em.{source_col}='gemini_rebuild_seed' THEN 1 ELSE 0 END) AS rebuild_cnt,
                           SUM(CASE WHEN em.{source_col}='gemini_auto_seed' THEN 1 ELSE 0 END) AS auto_cnt
                    FROM reaction_extracts re
                    LEFT JOIN extract_molecules em ON em.extract_id=re.id
                    WHERE re.{fam_col}=?
                    """,
                    (fam,),
                ).fetchone()
                campaign_rows.append({
                    'family': fam,
                    'extract_count': int(row[0] or 0),
                    'queryable_molecule_count': int(row[1] or 0),
                    'salvage_seed_count': int(row[2] or 0),
                    'rebuild_seed_count': int(row[3] or 0),
                    'auto_seed_count': int(row[4] or 0),
                    'resolved': int(row[1] or 0) > 0,
                })

        payload = {
            'db': str(db),
            'metrics': {
                'total_registry_families': total_registry_families,
                'queryable_family_coverage': family_coverage,
                'reaction_extracts': total_extracts,
                'extract_molecules_total': total_molecules,
                'queryable': queryable,
                'tier1': tier1,
                'tier2': tier2,
                'tier3': tier3,
            },
            'structure_source_counts': structure_source_counts,
            'campaign': {
                'resolved_count': sum(1 for x in campaign_rows if x['resolved']),
                'total_campaign_families': len(campaign_rows),
                'families': campaign_rows,
            },
        }

        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0

        print('=' * 80)
        print('CHEMLENS BACKEND ACQUISITION STATUS')
        print('=' * 80)
        print(f"db: {db}")
        m = payload['metrics']
        print(f"total_registry_families   : {m['total_registry_families']}")
        print(f"queryable_family_coverage : {m['queryable_family_coverage']}")
        print(f"reaction_extracts         : {m['reaction_extracts']}")
        print(f"extract_molecules_total   : {m['extract_molecules_total']}")
        print(f"queryable                 : {m['queryable']}")
        print(f"tier1 / tier2 / tier3     : {m['tier1']} / {m['tier2']} / {m['tier3']}")
        print('-' * 80)
        print(f"campaign_resolved         : {payload['campaign']['resolved_count']}/{payload['campaign']['total_campaign_families']}")
        for x in campaign_rows:
            tag = 'RESOLVED' if x['resolved'] else 'UNRESOLVED'
            print(f"- {tag:10s} | {x['family']} | extracts={x['extract_count']} | queryable={x['queryable_molecule_count']} | salvage={x['salvage_seed_count']} | rebuild={x['rebuild_seed_count']} | auto={x['auto_seed_count']}")
        print('-' * 80)
        print('structure_source_counts')
        for item in structure_source_counts:
            print(f"- {item['source']}: {item['count']}")
        print('=' * 80)
    finally:
        conn.close()
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
