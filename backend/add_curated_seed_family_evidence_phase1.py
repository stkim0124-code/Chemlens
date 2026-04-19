import argparse, datetime as dt, json, sqlite3, sys
from pathlib import Path

try:
    from rdkit import Chem
    from rdkit.Chem import AllChem
except Exception:
    Chem = None
    AllChem = None

TAG = 'phase1_manual_curated_seed_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

SEEDS = [
    {
        'family': 'Aza-Wittig Reaction',
        'anchor_image': 'named reactions_76.jpg',
        'anchor_section_role': 'canonical_overview',
        'page_no_expected': 76,
        'extract_kind': 'canonical_overview',
        'transformation_text': 'Curated representative seed: organic azide plus triphenylphosphine plus aldehyde condensed to an imine (aza-Wittig via iminophosphorane).',
        'reactants_text': 'phenyl azide | benzaldehyde',
        'products_text': 'N-benzylideneaniline',
        'reagents_text': 'triphenylphosphine',
        'conditions_text': 'curated representative seed from textbook canonical overview',
        'notes_text': 'Manual curated seed built from the canonical overview page to provide concrete queryable structure evidence for the family. Simplified representative example, not claimed to be the exact textbook substrate.',
        'molecules': [
            ('reactant', '[N-]=[N+]=Nc1ccccc1', 'phenyl azide', 'reactants_text'),
            ('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text'),
            ('product', 'N=C(c1ccccc1)c1ccccc1', 'N-benzylideneaniline', 'products_text'),
            ('reagent', 'c1ccc(P(c2ccccc2)c2ccccc2)cc1', 'triphenylphosphine', 'reagents_text'),
        ],
    },
    {
        'family': 'Bamford-Stevens-Shapiro Olefination',
        'anchor_image': 'named reactions_88.jpg',
        'anchor_section_role': 'canonical_overview',
        'page_no_expected': 88,
        'extract_kind': 'canonical_overview',
        'transformation_text': 'Curated representative seed: cyclohexanone tosylhydrazone undergoes base-promoted Bamford-Stevens/Shapiro olefination to cyclohexene.',
        'reactants_text': 'cyclohexanone tosylhydrazone',
        'products_text': 'cyclohexene',
        'reagents_text': 'n-butyllithium',
        'conditions_text': 'curated representative seed from textbook canonical overview',
        'notes_text': 'Manual curated seed built from the canonical overview page to provide concrete queryable structure evidence for the family. Simplified representative substrate capturing the tosylhydrazone-to-alkene logic.',
        'molecules': [
            ('reactant', 'Cc1ccc(S(=O)(=O)NN=C2CCCCC2)cc1', 'cyclohexanone tosylhydrazone', 'reactants_text'),
            ('product', 'C1=CCCCC1', 'cyclohexene', 'products_text'),
            ('reagent', '[Li][CH2]CCC', 'n-butyllithium', 'reagents_text'),
        ],
    },
    {
        'family': 'Barton Nitrite Ester Reaction',
        'anchor_image': 'named reactions_94.jpg',
        'anchor_section_role': 'canonical_overview',
        'page_no_expected': 94,
        'extract_kind': 'canonical_overview',
        'transformation_text': 'Curated representative seed: a primary alkyl nitrite undergoes photolysis and remote oxidation to a gamma-hydroxy oxime (Barton nitrite ester reaction).',
        'reactants_text': '2-methylbutyl nitrite',
        'products_text': '4-hydroxy-2-methylbutanal oxime',
        'reagents_text': 'hν',
        'conditions_text': 'curated representative seed from textbook canonical overview',
        'notes_text': 'Manual curated seed built from the canonical overview page to provide concrete queryable structure evidence for the family. Simplified representative substrate capturing the nitrite-ester-to-gamma-hydroxy-oxime logic.',
        'molecules': [
            ('reactant', 'CCC(C)CON=O', '2-methylbutyl nitrite', 'reactants_text'),
            ('product', 'CC(C=NO)CCO', '4-hydroxy-2-methylbutanal oxime', 'products_text'),
        ],
    },
]


def canon(smiles: str) -> str:
    if Chem is None:
        return smiles
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        raise ValueError(f'RDKit failed to parse SMILES: {smiles}')
    return Chem.MolToSmiles(mol)


def family_coverage(conn):
    return conn.execute("SELECT COUNT(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1").fetchone()[0]


def queryable_count(conn):
    return conn.execute("SELECT COUNT(*) FROM extract_molecules WHERE queryable=1").fetchone()[0]


def find_anchor(conn, family, anchor_image, anchor_section_role, page_no_expected):
    row = conn.execute(
        '''
        SELECT sc.id, pi.page_no, pi.image_filename
        FROM scheme_candidates sc
        JOIN page_images pi ON pi.id = sc.page_image_id
        WHERE pi.image_filename = ? AND (sc.scheme_role = ? OR sc.section_type = 'overview')
        ORDER BY CASE WHEN sc.scheme_role = ? THEN 0 ELSE 1 END, sc.id
        LIMIT 1
        ''',
        (anchor_image, anchor_section_role, anchor_section_role),
    ).fetchone()
    if row:
        return row
    row = conn.execute(
        '''
        SELECT sc.id, pi.page_no, pi.image_filename
        FROM scheme_candidates sc
        JOIN page_images pi ON pi.id = sc.page_image_id
        JOIN reaction_extracts re ON re.scheme_candidate_id = sc.id
        WHERE re.reaction_family_name = ?
        ORDER BY sc.id
        LIMIT 1
        ''',
        (family,),
    ).fetchone()
    if row:
        return row
    row = conn.execute(
        '''
        SELECT sc.id, pi.page_no, pi.image_filename
        FROM scheme_candidates sc
        JOIN page_images pi ON pi.id = sc.page_image_id
        WHERE pi.page_no = ?
        ORDER BY sc.id
        LIMIT 1
        ''',
        (page_no_expected,),
    ).fetchone()
    return row


def existing_seed_extract(conn, family):
    row = conn.execute(
        "SELECT id FROM reaction_extracts WHERE reaction_family_name = ? AND notes_text LIKE ? LIMIT 1",
        (family, f'%{TAG}%'),
    ).fetchone()
    return row[0] if row else None


def insert_seed(conn, seed):
    fam = seed['family']
    anchor = find_anchor(conn, fam, seed['anchor_image'], seed['anchor_section_role'], seed['page_no_expected'])
    if not anchor:
        raise RuntimeError(f'Could not find anchor scheme_candidate for {fam}')
    sc_id, page_no, image_filename = anchor

    existing = existing_seed_extract(conn, fam)
    if existing:
        return {'family': fam, 'status': 'skipped_existing', 'extract_id': existing, 'page_no': page_no, 'image_filename': image_filename, 'inserted_molecules': 0}

    cur = conn.execute(
        '''
        INSERT INTO reaction_extracts (
            scheme_candidate_id, reaction_family_name, reaction_family_name_norm, extract_kind,
            transformation_text, reactants_text, products_text, reagents_text,
            conditions_text, notes_text, smiles_confidence, extraction_confidence,
            parse_status, promote_decision, extractor_model, extractor_prompt_version,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            sc_id, fam, fam.lower(), seed['extract_kind'],
            seed['transformation_text'], seed['reactants_text'], seed['products_text'], seed['reagents_text'],
            seed['conditions_text'], seed['notes_text'] + f' [{TAG}]', 0.99, 0.99,
            'curated', 'promote', 'gpt-5.4-thinking', TAG,
            NOW, NOW,
        ),
    )
    extract_id = cur.lastrowid

    inserted = 0
    for role, smiles, name, source_field in seed['molecules']:
        s = canon(smiles)
        dup = conn.execute(
            '''SELECT id FROM extract_molecules
               WHERE extract_id = ? AND role = ? AND smiles = ?''',
            (extract_id, role, s),
        ).fetchone()
        if dup:
            continue
        conn.execute(
            '''
            INSERT INTO extract_molecules (
                extract_id, role, smiles, smiles_kind, quality_tier, reaction_family_name,
                source_zip, page_no, queryable, note_text, normalized_text,
                source_field, structure_source, role_confidence, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                extract_id, role, s, 'smiles', 1, fam,
                'named reactions.zip', page_no, 1,
                f'{TAG} | {name} | anchor={image_filename}', name,
                source_field, 'manual_curated_seed', 0.98, NOW,
            ),
        )
        inserted += 1

    # Also backfill direct smiles fields for the extract itself when possible.
    reactants = [canon(sm) for role, sm, _nm, _sf in seed['molecules'] if role == 'reactant']
    products = [canon(sm) for role, sm, _nm, _sf in seed['molecules'] if role == 'product']
    conn.execute(
        'UPDATE reaction_extracts SET reactant_smiles=?, product_smiles=? WHERE id=?',
        (' | '.join(reactants) if reactants else None, ' | '.join(products) if products else None, extract_id),
    )

    return {'family': fam, 'status': 'inserted', 'extract_id': extract_id, 'page_no': page_no, 'image_filename': image_filename, 'inserted_molecules': inserted}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default='app/labint.db')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    db = Path(args.db)
    if not db.exists():
        print(json.dumps({'error': f'DB not found: {db}'}, ensure_ascii=False, indent=2))
        sys.exit(1)

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    try:
        before = {
            'queryable': queryable_count(conn),
            'family_coverage': family_coverage(conn),
        }
        results = []
        if args.dry_run:
            conn.execute('BEGIN')
        for seed in SEEDS:
            results.append(insert_seed(conn, seed))
        after = {
            'queryable': queryable_count(conn),
            'family_coverage': family_coverage(conn),
        }
        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()
    finally:
        conn.close()

    print(json.dumps({
        'db': str(db.resolve()),
        'dry_run': args.dry_run,
        'tag': TAG,
        'before': before,
        'after': after,
        'delta': {
            'queryable': after['queryable'] - before['queryable'],
            'family_coverage': after['family_coverage'] - before['family_coverage'],
        },
        'results': results,
    }, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()
