
import argparse, datetime as dt, json, sqlite3
from pathlib import Path

BATCH_FAMILIES = {'a': ['Glaser Coupling', 'Grignard Reaction', 'Heck Reaction', 'Hell-Volhard-Zelinsky Reaction', 'Henry Reaction'], 'b': ['Hantzsch Dihydropyridine Synthesis', 'Hetero Diels-Alder Cycloaddition (HDA)', 'Hofmann Elimination', 'Hofmann Rearrangement', 'Horner-Wadsworth-Emmons Olefination']}
ALIAS_RESIDUES = {'Glaser Coupling': ['Glaser Reaction'], 'Grignard Reaction': [], 'Heck Reaction': [], 'Hell-Volhard-Zelinsky Reaction': ['Hell Volhard Zelinsky Reaction', 'HVZ Reaction'], 'Henry Reaction': [], 'Hantzsch Dihydropyridine Synthesis': ['Hantzsch Synthesis'], 'Hetero Diels-Alder Cycloaddition (HDA)': ['Hetero Diels-Alder Cycloaddition', 'Hetero Diels Alder Cycloaddition'], 'Hofmann Elimination': ['Hofmann Elimination Reaction'], 'Hofmann Rearrangement': ['Hofmann Rearrangement Reaction'], 'Horner-Wadsworth-Emmons Olefination': ['Horner-Wadsworth-Emmons', 'Horner-Wadsworth-Emmons Reaction']}
TAG = 'phase13_shallow_top10_completion_'

def connect(path: Path) -> sqlite3.Connection:
    conn=sqlite3.connect(str(path)); conn.row_factory=sqlite3.Row; return conn

def alias_residue(conn: sqlite3.Connection, family: str):
    residues=ALIAS_RESIDUES.get(family, [])
    out={'reaction_extracts': [], 'extract_molecules': []}
    for alias in residues:
        if conn.execute('SELECT 1 FROM reaction_extracts WHERE reaction_family_name=? LIMIT 1',(alias,)).fetchone(): out['reaction_extracts'].append(alias)
        if conn.execute('SELECT 1 FROM extract_molecules WHERE reaction_family_name=? LIMIT 1',(alias,)).fetchone(): out['extract_molecules'].append(alias)
    return out

def verify_family(conn: sqlite3.Connection, family: str):
    row=conn.execute("""
        SELECT COUNT(*) AS extract_count,
               SUM(CASE WHEN extract_kind='canonical_overview' THEN 1 ELSE 0 END) AS overview_count,
               SUM(CASE WHEN extract_kind='application_example' THEN 1 ELSE 0 END) AS application_count,
               SUM(CASE WHEN COALESCE(reactant_smiles,'')<>'' THEN 1 ELSE 0 END) AS extract_with_reactant,
               SUM(CASE WHEN COALESCE(product_smiles,'')<>'' THEN 1 ELSE 0 END) AS extract_with_product,
               SUM(CASE WHEN COALESCE(reactant_smiles,'')<>'' AND COALESCE(product_smiles,'')<>'' THEN 1 ELSE 0 END) AS extract_with_both
        FROM reaction_extracts WHERE reaction_family_name=?
    """,(family,)).fetchone()
    mol=conn.execute("""
        SELECT SUM(CASE WHEN role='reactant' AND queryable=1 AND smiles IS NOT NULL AND smiles<>'' THEN 1 ELSE 0 END) AS queryable_reactants,
               SUM(CASE WHEN role='product' AND queryable=1 AND smiles IS NOT NULL AND smiles<>'' THEN 1 ELSE 0 END) AS queryable_products,
               COUNT(*) AS molecule_rows
        FROM extract_molecules WHERE reaction_family_name=?
    """,(family,)).fetchone()
    pair_count=conn.execute("""
        SELECT COUNT(DISTINCT COALESCE(reactant_smiles,'') || ' || ' || COALESCE(product_smiles,''))
        FROM reaction_extracts WHERE reaction_family_name=? AND COALESCE(reactant_smiles,'')<>'' AND COALESCE(product_smiles,'')<>''
    """,(family,)).fetchone()[0]
    curated=conn.execute('SELECT id, extract_kind, transformation_text, reactants_text, products_text FROM reaction_extracts WHERE reaction_family_name=? AND notes_text LIKE ? ORDER BY id',(family, '%' + TAG + '%')).fetchall()
    return {'family':family, **dict(row), **dict(mol), 'unique_queryable_pair_count': int(pair_count or 0), 'completion_minimum_pass': int((row['overview_count'] or 0))>=1 and int((row['application_count'] or 0))>=2 and int((mol['queryable_reactants'] or 0))>=1 and int((mol['queryable_products'] or 0))>=1 and int(pair_count or 0)>=1, 'rich_completion_pass': int((row['overview_count'] or 0))>=1 and int((row['application_count'] or 0))>=2 and int((mol['queryable_reactants'] or 0))>=3 and int((mol['queryable_products'] or 0))>=3 and int(pair_count or 0)>=3, 'curated_extract_ids':[int(r['id']) for r in curated], 'curated_extract_summaries':[dict(r) for r in curated], 'alias_residue':alias_residue(conn,family)}

def main() -> int:
    ap=argparse.ArgumentParser(description='Verify shallow-family sprint phase13 top10 in 5+5 batches.')
    ap.add_argument('--db', default='app/labint.db')
    ap.add_argument('--report-dir', default='reports/family_completion_phase13_shallow_top10_verify')
    ap.add_argument('--batch', choices=['a','b','all'], default='all')
    args=ap.parse_args()
    families=BATCH_FAMILIES['a']+BATCH_FAMILIES['b'] if args.batch=='all' else BATCH_FAMILIES[args.batch]
    db_path=Path(args.db)
    report_dir=Path(args.report_dir)/dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    report_dir.mkdir(parents=True, exist_ok=True)
    conn=connect(db_path)
    try:
        payload={'db':str(db_path),'batch':args.batch,'verify':[verify_family(conn,fam) for fam in families]}
        suffix=f'phase13_shallow_top10_{args.batch}'
        jpath=report_dir/f'{suffix}_verify.json'
        mpath=report_dir/f'{suffix}_verify.md'
        jpath.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8')
        lines=[f'# Family completion {suffix} verify','',f'- db: `{db_path}`',f'- batch: `{args.batch}`','']
        for item in payload['verify']:
            lines.append(f"## {item['family']}")
            for k in ['extract_count','overview_count','application_count','extract_with_reactant','extract_with_product','extract_with_both','queryable_reactants','queryable_products','unique_queryable_pair_count','completion_minimum_pass','rich_completion_pass']:
                lines.append(f'- {k}: {item[k]}')
            lines.append(f"- alias_residue.reaction_extracts: {item['alias_residue']['reaction_extracts']}")
            lines.append(f"- alias_residue.extract_molecules: {item['alias_residue']['extract_molecules']}")
            lines.append('')
        mpath.write_text('\n'.join(lines)+'\n', encoding='utf-8')
        print(f'verify json: {jpath}')
        print(f'verify md:   {mpath}')
        return 0
    finally:
        conn.close()

if __name__=='__main__':
    raise SystemExit(main())
