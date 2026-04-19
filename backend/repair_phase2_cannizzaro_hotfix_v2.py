import argparse, datetime as dt, json, sqlite3, sys
from pathlib import Path

try:
    from rdkit import Chem
except Exception:
    Chem = None

OLD_TAG = 'phase2_manual_curated_seed_v1'
NEW_TAG = 'phase2_manual_curated_seed_v2'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
FAMILY = 'Cannizzaro Reaction'

NEW_SEED = {
    'transformation_text': 'Curated representative seed: non-enolizable pivaldehyde undergoes Cannizzaro disproportionation under strong base to neopentyl alcohol and pivalic acid.',
    'reactants_text': 'pivaldehyde',
    'products_text': 'neopentyl alcohol | pivalic acid',
    'reagents_text': 'sodium hydroxide',
    'conditions_text': 'curated representative seed from textbook canonical overview',
    'notes_text': 'Manual curated seed built from the canonical overview page to provide concrete queryable structure evidence for the family. Swapped from aromatic benzaldehyde to aliphatic non-enolizable pivaldehyde to reduce false positives against arene-rich rearrangement families. [' + NEW_TAG + ']',
    'molecules': [
        ('reactant', 'CC(C)(C)C=O', 'pivaldehyde', 'reactants_text'),
        ('product', 'CC(C)(C)CO', 'neopentyl alcohol', 'products_text'),
        ('product', 'CC(C)(C)C(=O)O', 'pivalic acid', 'products_text'),
        ('reagent', '[Na+].[OH-]', 'sodium hydroxide', 'reagents_text'),
    ],
}


def canon(smiles: str) -> str:
    if Chem is None:
        return smiles
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        raise ValueError(f'RDKit failed to parse SMILES: {smiles}')
    return Chem.MolToSmiles(mol)


def queryable_count(conn):
    return conn.execute('SELECT COUNT(*) FROM extract_molecules WHERE queryable=1').fetchone()[0]


def family_coverage(conn):
    return conn.execute('SELECT COUNT(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1').fetchone()[0]


def find_state(conn):
    v2 = conn.execute(
        'SELECT id FROM reaction_extracts WHERE reaction_family_name=? AND notes_text LIKE ? LIMIT 1',
        (FAMILY, f'%{NEW_TAG}%')
    ).fetchone()
    if v2:
        return 'already_v2', v2[0]
    v1 = conn.execute(
        'SELECT id, scheme_candidate_id FROM reaction_extracts WHERE reaction_family_name=? AND notes_text LIKE ? LIMIT 1',
        (FAMILY, f'%{OLD_TAG}%')
    ).fetchone()
    if v1:
        return 'upgrade_v1', v1[0]
    return 'not_found', None


def apply_upgrade(conn, extract_id: int):
    reactants = [canon(sm) for role, sm, _nm, _sf in NEW_SEED['molecules'] if role == 'reactant']
    products = [canon(sm) for role, sm, _nm, _sf in NEW_SEED['molecules'] if role == 'product']

    page_no_row = conn.execute('SELECT page_no FROM extract_molecules WHERE extract_id=? LIMIT 1', (extract_id,)).fetchone()
    page_no = page_no_row[0] if page_no_row else None

    conn.execute('DELETE FROM extract_molecules WHERE extract_id=?', (extract_id,))
    conn.execute('''
        UPDATE reaction_extracts
        SET transformation_text=?, reactants_text=?, products_text=?, reagents_text=?,
            conditions_text=?, notes_text=?, reactant_smiles=?, product_smiles=?,
            extractor_prompt_version=?, updated_at=?
        WHERE id=?
    ''', (
        NEW_SEED['transformation_text'], NEW_SEED['reactants_text'], NEW_SEED['products_text'], NEW_SEED['reagents_text'],
        NEW_SEED['conditions_text'], NEW_SEED['notes_text'],
        ' | '.join(reactants) if reactants else None,
        ' | '.join(products) if products else None,
        NEW_TAG, NOW, extract_id,
    ))

    inserted = 0
    for role, smiles, name, source_field in NEW_SEED['molecules']:
        s = canon(smiles)
        conn.execute('''
            INSERT INTO extract_molecules (
                extract_id, role, smiles, smiles_kind, quality_tier, reaction_family_name,
                source_zip, page_no, queryable, note_text, normalized_text,
                source_field, structure_source, role_confidence, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            extract_id, role, s, 'smiles', 1, FAMILY,
            'named reactions.zip', page_no, 1,
            f'{NEW_TAG} | {name}', name,
            source_field, 'manual_curated_seed', 0.98, NOW,
        ))
        inserted += 1
    return inserted


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
        state, extract_id = find_state(conn)
        if state == 'already_v2':
            result = {'status': 'already_v2', 'extract_id': extract_id}
        elif state == 'upgrade_v1':
            inserted = apply_upgrade(conn, extract_id)
            result = {'status': 'upgraded_v1_to_v2', 'extract_id': extract_id, 'inserted_molecules': inserted}
        else:
            result = {'status': 'not_found', 'message': 'No phase2 v1 Cannizzaro seed found. Nothing changed.'}
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
        'before': before,
        'after': after,
        'delta': {
            'queryable': after['queryable'] - before['queryable'],
            'family_coverage': after['family_coverage'] - before['family_coverage'],
        },
        'result': result,
    }, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()
