import argparse
import datetime as dt
import json
import sqlite3
from pathlib import Path

FAMILIES = [
    'Alkene (Olefin) Metathesis',
    'Barton-McCombie Radical Deoxygenation Reaction',
]
ALIASES = [
    'Alkene (olefin) Metathesis',
    'Barton-Mccombie Radical Deoxygenation Reaction',
]


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
          SUM(CASE WHEN role='reactant' AND queryable=1 AND smiles IS NOT NULL THEN 1 ELSE 0 END) AS queryable_reactants,
          SUM(CASE WHEN role='product' AND queryable=1 AND smiles IS NOT NULL THEN 1 ELSE 0 END) AS queryable_products,
          COUNT(*) AS molecule_rows
        FROM extract_molecules
        WHERE reaction_family_name=?
        ''',
        (family,),
    ).fetchone()
    curated = conn.execute(
        '''
        SELECT id, extract_kind, transformation_text, reactants_text, products_text, notes_text
        FROM reaction_extracts
        WHERE reaction_family_name=? AND notes_text LIKE '%phase3_family_completion_metathesis_barton_v1%'
        ORDER BY id
        ''' ,
        (family,),
    ).fetchall()
    return {
        'family': family,
        **dict(row),
        **dict(mol),
        'curated_extract_ids': [r[0] for r in curated],
        'curated_extract_summaries': [
            {
                'id': r[0],
                'extract_kind': r[1],
                'transformation_text': r[2],
                'reactants_text': r[3],
                'products_text': r[4],
            } for r in curated
        ],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default='app/labint.db')
    ap.add_argument('--report-dir', default='reports/family_completion_phase3_verify')
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
                    f"SELECT reaction_family_name, COUNT(*) c FROM reaction_extracts WHERE reaction_family_name IN ({','.join('?'*len(ALIASES))}) GROUP BY reaction_family_name",
                    ALIASES,
                ).fetchall(),
                'extract_molecules': conn.execute(
                    f"SELECT reaction_family_name, COUNT(*) c FROM extract_molecules WHERE reaction_family_name IN ({','.join('?'*len(ALIASES))}) GROUP BY reaction_family_name",
                    ALIASES,
                ).fetchall(),
            },
        }
        # sqlite Row not serializable
        payload['alias_residue'] = {k: [dict(r) for r in v] for k,v in payload['alias_residue'].items()}
        (report_dir / 'family_completion_phase3_verify.json').write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8')
        lines = ['# Family completion phase3 verify', '']
        for fam in payload['families']:
            lines.append(f"## {fam['family']}")
            for k in ['extract_count','overview_count','application_count','extract_with_reactant','extract_with_product','extract_with_both','queryable_reactants','queryable_products','molecule_rows']:
                lines.append(f'- {k}: {fam[k]}')
            lines.append(f"- curated_extract_ids: {fam['curated_extract_ids']}")
            lines.append('')
        lines.append('## Alias residue')
        lines.append(f"- reaction_extracts: {payload['alias_residue']['reaction_extracts']}")
        lines.append(f"- extract_molecules: {payload['alias_residue']['extract_molecules']}")
        (report_dir / 'family_completion_phase3_verify.md').write_text('\n'.join(lines), encoding='utf-8')
        print('verify json:', report_dir / 'family_completion_phase3_verify.json')
        print('verify md:  ', report_dir / 'family_completion_phase3_verify.md')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
