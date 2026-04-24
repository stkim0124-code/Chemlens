import argparse
import datetime as dt
import json
import sqlite3
from pathlib import Path

FAMILIES = [
    'Fries Rearrangement',
    'Hofmann-Loffler-Freytag Reaction',
    'Houben-Hoesch Reaction',
]
ALIASES = [
    'Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement',
    'Hofmann-Löffler-Freytag Reaction (Remote Functionalization)',
    'Houben-Hoesch Reaction/Synthesis',
]
TAG = 'phase4_family_completion_missing_trio_v1'


def gather_family(conn, family):
    row = conn.execute(
        '''
        SELECT
          COUNT(*) AS extract_count,
          SUM(CASE WHEN extract_kind='canonical_overview' THEN 1 ELSE 0 END) AS overview_count,
          SUM(CASE WHEN extract_kind='application_example' THEN 1 ELSE 0 END) AS application_count,
          SUM(CASE WHEN COALESCE(reactant_smiles,'')<>'' THEN 1 ELSE 0 END) AS extract_with_reactant,
          SUM(CASE WHEN COALESCE(product_smiles,'')<>'' THEN 1 ELSE 0 END) AS extract_with_product,
          SUM(CASE WHEN COALESCE(reactant_smiles,'')<>'' AND COALESCE(product_smiles,'')<>'' THEN 1 ELSE 0 END) AS extract_with_both
        FROM reaction_extracts
        WHERE reaction_family_name=?
        ''',
        (family,),
    ).fetchone()
    mol = conn.execute(
        '''
        SELECT
          SUM(CASE WHEN role='reactant' AND queryable=1 AND smiles IS NOT NULL AND smiles<>'' THEN 1 ELSE 0 END) AS queryable_reactants,
          SUM(CASE WHEN role='product' AND queryable=1 AND smiles IS NOT NULL AND smiles<>'' THEN 1 ELSE 0 END) AS queryable_products,
          COUNT(*) AS molecule_rows
        FROM extract_molecules
        WHERE reaction_family_name=?
        ''',
        (family,),
    ).fetchone()
    pair_count = conn.execute(
        '''
        SELECT COUNT(DISTINCT COALESCE(reactant_smiles,'') || ' || ' || COALESCE(product_smiles,''))
        FROM reaction_extracts
        WHERE reaction_family_name=? AND COALESCE(reactant_smiles,'')<>'' AND COALESCE(product_smiles,'')<>''
        ''',
        (family,),
    ).fetchone()[0]
    curated = conn.execute(
        '''
        SELECT id, extract_kind, transformation_text, reactants_text, products_text
        FROM reaction_extracts
        WHERE reaction_family_name=? AND notes_text LIKE ?
        ORDER BY id
        ''',
        (family, '%' + TAG + '%'),
    ).fetchall()
    return {
        'family': family,
        **dict(row),
        **dict(mol),
        'unique_queryable_pair_count': int(pair_count or 0),
        'completion_gate_minimum_pass': (
            int((row['overview_count'] or 0)) >= 1
            and int((row['application_count'] or 0)) >= 2
            and int((mol['queryable_reactants'] or 0)) >= 1
            and int((mol['queryable_products'] or 0)) >= 1
            and int(pair_count or 0) >= 1
        ),
        'rich_completion_pass': (
            int((row['overview_count'] or 0)) >= 1
            and int((row['application_count'] or 0)) >= 2
            and int((mol['queryable_reactants'] or 0)) >= 3
            and int((mol['queryable_products'] or 0)) >= 3
            and int(pair_count or 0) >= 3
        ),
        'curated_extract_ids': [int(r['id']) for r in curated],
        'curated_extract_summaries': [dict(r) for r in curated],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default='app/labint.db')
    ap.add_argument('--report-dir', default='reports/family_completion_phase4_missing_trio_verify')
    args = ap.parse_args()
    report_dir = Path(args.report_dir) / dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    report_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        payload = {
            'families': [gather_family(conn, fam) for fam in FAMILIES],
            'alias_residue': {
                'reaction_extracts': conn.execute(
                    f"SELECT reaction_family_name, COUNT(*) c FROM reaction_extracts WHERE reaction_family_name IN ({','.join('?' for _ in ALIASES)}) GROUP BY reaction_family_name ORDER BY reaction_family_name",
                    ALIASES,
                ).fetchall(),
                'extract_molecules': conn.execute(
                    f"SELECT reaction_family_name, COUNT(*) c FROM extract_molecules WHERE reaction_family_name IN ({','.join('?' for _ in ALIASES)}) GROUP BY reaction_family_name ORDER BY reaction_family_name",
                    ALIASES,
                ).fetchall(),
            },
        }
        payload['alias_residue'] = {
            k: [dict(r) for r in v] for k, v in payload['alias_residue'].items()
        }
        (report_dir / 'family_completion_phase4_missing_trio_verify.json').write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8')
        md = ['# Family completion phase4 missing-trio verify', '']
        for item in payload['families']:
            md.append(f"## {item['family']}")
            for key in ['extract_count','overview_count','application_count','extract_with_reactant','extract_with_product','extract_with_both','queryable_reactants','queryable_products','unique_queryable_pair_count','completion_gate_minimum_pass','rich_completion_pass']:
                md.append(f'- {key}: {item[key]}')
            if item['curated_extract_summaries']:
                md.append('- curated_extract_summaries:')
                for row in item['curated_extract_summaries']:
                    md.append(f"  - [{row['id']}] {row['extract_kind']} :: {row['transformation_text']}")
            md.append('')
        md.append('## Alias residue')
        for table, rows in payload['alias_residue'].items():
            if rows:
                for row in rows:
                    md.append(f"- {table}: {row['reaction_family_name']} ({row['c']} rows)")
            else:
                md.append(f'- {table}: none')
        (report_dir / 'family_completion_phase4_missing_trio_verify.md').write_text('\n'.join(md), encoding='utf-8')
        print('verify json:', report_dir / 'family_completion_phase4_missing_trio_verify.json')
        print('verify md:  ', report_dir / 'family_completion_phase4_missing_trio_verify.md')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
