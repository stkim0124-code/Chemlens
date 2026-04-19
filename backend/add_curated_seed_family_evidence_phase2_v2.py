import argparse, datetime as dt, json, sqlite3, sys
from pathlib import Path

try:
    from rdkit import Chem
except Exception:
    Chem = None

TAG = 'phase2_manual_curated_seed_v2'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

SEEDS = [
    {
        'family': 'Cannizzaro Reaction',
        'anchor_image': 'named reactions_74.jpg',
        'page_no_expected': 74,
        'extract_kind': 'canonical_overview',
        'transformation_text': 'Curated representative seed: non-enolizable pivaldehyde undergoes Cannizzaro disproportionation under strong base to neopentyl alcohol and pivalic acid.',
        'reactants_text': 'pivaldehyde',
        'products_text': 'neopentyl alcohol | pivalic acid',
        'reagents_text': 'sodium hydroxide',
        'conditions_text': 'curated representative seed from textbook canonical overview',
        'notes_text': 'Manual curated seed built from the canonical overview page to provide concrete queryable structure evidence for the family. Swapped from aromatic benzaldehyde to aliphatic non-enolizable pivaldehyde to reduce false positives against arene-rich rearrangement families. [' + TAG + ']',
        'molecules': [
            ('reactant', 'CC(C)(C)C=O', 'pivaldehyde', 'reactants_text'),
            ('product', 'CC(C)(C)CO', 'neopentyl alcohol', 'products_text'),
            ('product', 'CC(C)(C)C(=O)O', 'pivalic acid', 'products_text'),
            ('reagent', '[Na+].[OH-]', 'sodium hydroxide', 'reagents_text'),
        ],
    },
    {
        'family': 'Clemmensen Reduction',
        'anchor_image': 'named reactions_92.jpg',
        'page_no_expected': 92,
        'extract_kind': 'canonical_overview',
        'transformation_text': 'Curated representative seed: acetophenone undergoes Clemmensen reduction under Zn(Hg)/HCl conditions to ethylbenzene.',
        'reactants_text': 'acetophenone',
        'products_text': 'ethylbenzene',
        'reagents_text': 'zinc | hydrochloric acid',
        'conditions_text': 'curated representative seed from textbook canonical overview',
        'notes_text': 'Manual curated seed built from the canonical overview page to provide concrete queryable structure evidence for the family. Simplified representative substrate capturing carbonyl deoxygenation under strongly acidic zinc conditions. [' + TAG + ']',
        'molecules': [
            ('reactant', 'CC(=O)c1ccccc1', 'acetophenone', 'reactants_text'),
            ('product', 'CCc1ccccc1', 'ethylbenzene', 'products_text'),
            ('reagent', '[Zn]', 'zinc', 'reagents_text'),
            ('reagent', 'Cl', 'hydrochloric acid surrogate', 'reagents_text'),
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


def find_anchor(conn, anchor_image, page_no_expected):
    row = conn.execute('''
        SELECT sc.id, pi.page_no, pi.image_filename
        FROM scheme_candidates sc
        JOIN page_images pi ON pi.id = sc.page_image_id
        WHERE pi.image_filename = ?
        ORDER BY sc.id
        LIMIT 1
    ''', (anchor_image,)).fetchone()
    if row:
        return row
    row = conn.execute('''
        SELECT sc.id, pi.page_no, pi.image_filename
        FROM scheme_candidates sc
        JOIN page_images pi ON pi.id = sc.page_image_id
        WHERE pi.page_no = ?
        ORDER BY sc.id
        LIMIT 1
    ''', (page_no_expected,)).fetchone()
    return row


def existing_seed_extract(conn, family):
    row = conn.execute(
        "SELECT id FROM reaction_extracts WHERE reaction_family_name = ? AND notes_text LIKE ? LIMIT 1",
        (family, f'%{TAG}%'),
    ).fetchone()
    return row[0] if row else None


def insert_seed(conn, seed):
    fam = seed['family']
    anchor = find_anchor(conn, seed['anchor_image'], seed['page_no_expected'])
    if not anchor:
        raise RuntimeError(f'Could not find anchor scheme_candidate for {fam}')
    sc_id, page_no, image_filename = anchor

    existing = existing_seed_extract(conn, fam)
    if existing:
        return {'family': fam, 'status': 'skipped_existing', 'extract_id': existing, 'page_no': page_no, 'image_filename': image_filename, 'inserted_molecules': 0}

    cur = conn.execute('''
        INSERT INTO reaction_extracts (
            scheme_candidate_id, reaction_family_name, reaction_family_name_norm, extract_kind,
            transformation_text, reactants_text, products_text, reagents_text,
            conditions_text, notes_text, smiles_confidence, extraction_confidence,
            parse_status, promote_decision, extractor_model, extractor_prompt_version,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        sc_id, fam, fam.lower(), seed['extract_kind'],
        seed['transformation_text'], seed['reactants_text'], seed['products_text'], seed['reagents_text'],
        seed['conditions_text'], seed['notes_text'], 0.99, 0.99,
        'curated', 'promote', 'gpt-5.4-thinking', TAG, NOW, NOW,
    ))
    extract_id = cur.lastrowid

    inserted = 0
    for role, smiles, name, source_field in seed['molecules']:
        s = canon(smiles)
        dup = conn.execute('''SELECT id FROM extract_molecules WHERE extract_id = ? AND role = ? AND smiles = ?''', (extract_id, role, s)).fetchone()
        if dup:
            continue
        conn.execute('''
            INSERT INTO extract_molecules (
                extract_id, role, smiles, smiles_kind, quality_tier, reaction_family_name,
                source_zip, page_no, queryable, note_text, normalized_text,
                source_field, structure_source, role_confidence, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            extract_id, role, s, 'smiles', 1, fam,
            'named reactions.zip', page_no, 1,
            f'{TAG} | {name} | anchor={image_filename}', name,
            source_field, 'manual_curated_seed', 0.98, NOW,
        ))
        inserted += 1

    reactants = [canon(sm) for role, sm, _nm, _sf in seed['molecules'] if role == 'reactant']
    products = [canon(sm) for role, sm, _nm, _sf in seed['molecules'] if role == 'product']
    conn.execute('UPDATE reaction_extracts SET reactant_smiles=?, product_smiles=? WHERE id=?', (
        ' | '.join(reactants) if reactants else None,
        ' | '.join(products) if products else None,
        extract_id,
    ))

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
    try:
        before = {'queryable': queryable_count(conn), 'family_coverage': family_coverage(conn)}
        if args.dry_run:
            conn.execute('BEGIN')
        results = [insert_seed(conn, seed) for seed in SEEDS]
        after = {'queryable': queryable_count(conn), 'family_coverage': family_coverage(conn)}
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
        'delta': {'queryable': after['queryable'] - before['queryable'], 'family_coverage': after['family_coverage'] - before['family_coverage']},
        'results': results,
    }, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
