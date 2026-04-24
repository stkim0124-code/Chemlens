import argparse
import datetime as dt
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from rdkit import Chem
    from rdkit.Chem import AllChem
except Exception:
    Chem = None
    AllChem = None

TAG = 'phase3_family_completion_metathesis_barton_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

ALIASES = {
    'Alkene (olefin) Metathesis': 'Alkene (Olefin) Metathesis',
    'Barton-Mccombie Radical Deoxygenation Reaction': 'Barton-McCombie Radical Deoxygenation Reaction',
}

SEEDS: List[Dict[str, Any]] = [
    {
        'family': 'Alkene (Olefin) Metathesis',
        'anchor_image': 'named reactions_62.jpg',
        'anchor_scheme_role': 'canonical_overview',
        'page_no_expected': 62,
        'extract_kind': 'canonical_overview',
        'transformation_text': 'Curated representative seed: ring-closing metathesis of 1,7-octadiene to cyclohexene.',
        'reactants_text': '1,7-octadiene',
        'products_text': 'cyclohexene',
        'reagents_text': 'Grubbs catalyst',
        'conditions_text': 'curated representative seed from textbook overview page; captures the ring-closing metathesis subclass',
        'notes_text': 'Manual curated seed built from the alkene metathesis overview page to provide an explicit queryable RCM pair for the official family. Simplified representative substrate, not claimed to be the exact textbook substrate. [' + TAG + ']',
        'molecules': [
            ('reactant', 'C=CCCCCC=C', '1,7-octadiene', 'reactants_text', 1),
            ('product', 'C1=CCCCC1', 'cyclohexene', 'products_text', 1),
        ],
    },
    {
        'family': 'Alkene (Olefin) Metathesis',
        'anchor_image': 'named reactions_63.jpg',
        'anchor_scheme_role': 'application_example',
        'page_no_expected': 63,
        'extract_kind': 'application_example',
        'transformation_text': 'Curated representative seed: cross-metathesis of styrene with methyl acrylate to methyl cinnamate.',
        'reactants_text': 'styrene | methyl acrylate',
        'products_text': 'methyl cinnamate',
        'reagents_text': 'Grubbs catalyst',
        'conditions_text': 'curated application-class seed to ensure the family has an explicit cross-metathesis evidence pair; captures the CM subclass that is described on the alkene metathesis family pages',
        'notes_text': 'Manual curated application-class seed for the official alkene metathesis family. Added because the page-level extraction retained the named natural-product applications unevenly, while the completion sprint requires a clean explicit CM pair for search and family-depth purposes. [' + TAG + ']',
        'molecules': [
            ('reactant', 'C=CC1=CC=CC=C1', 'styrene', 'reactants_text', 1),
            ('reactant', 'C=CC(=O)OC', 'methyl acrylate', 'reactants_text', 1),
            ('product', 'COC(=O)/C=C/c1ccccc1', 'methyl cinnamate', 'products_text', 1),
        ],
    },
    {
        'family': 'Alkene (Olefin) Metathesis',
        'anchor_image': 'named reactions_63.jpg',
        'anchor_scheme_role': 'application_example',
        'page_no_expected': 63,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: ring-closing metathesis toward the (+)-Prelaureatin precursor.',
        'reactants_text': 'C=C[C@H](O[Si](C)(C)C(C)(C)C)[C@H](O[Si](C)(C)C(C)(C)C)[C@@H](C=C)OCc1ccccc1',
        'products_text': 'c1ccccc1CO[C@@H]2[C@H](O[Si](C)(C)C(C)(C)C)[C@H](O[Si](C)(C)C(C)(C)C)/C=C\\C2',
        'reagents_text': 'Grubbs catalyst',
        'conditions_text': 'DCM (0.005 M), 95%',
        'notes_text': 'Promoted from existing page-level extraction on the alkene metathesis synthetic applications page. Uses the explicit substrate/product SMILES that were already recovered from the page-level vision JSON. [' + TAG + ']',
        'molecules': [
            ('reactant', 'C=C[C@H](O[Si](C)(C)C(C)(C)C)[C@H](O[Si](C)(C)C(C)(C)C)[C@@H](C=C)OCc1ccccc1', 'prelaureatin metathesis precursor', 'reactants_text', 1),
            ('product', 'c1ccccc1CO[C@@H]2[C@H](O[Si](C)(C)C(C)(C)C)[C@H](O[Si](C)(C)C(C)(C)C)/C=C\\C2', 'ring-closed metathesis product', 'products_text', 1),
        ],
    },
    {
        'family': 'Barton-McCombie Radical Deoxygenation Reaction',
        'anchor_image': 'named reactions_98.jpg',
        'anchor_scheme_role': 'canonical_overview',
        'page_no_expected': 98,
        'extract_kind': 'canonical_overview',
        'transformation_text': 'Curated representative seed: 1-phenylethanol is deoxygenated to ethylbenzene via a xanthate intermediate under Barton-McCombie conditions.',
        'reactants_text': '1-phenylethanol',
        'products_text': 'ethylbenzene',
        'intermediates_text': '1-phenylethyl xanthate',
        'reagents_text': 'NaH | CS2 | MeI | (n-Bu)3SnH | AIBN',
        'conditions_text': 'toluene, reflux; curated representative seed from textbook overview page capturing alcohol-to-alkane deoxygenation through a thiocarbonyl derivative',
        'notes_text': 'Manual curated seed built from the Barton-McCombie overview page to provide an explicit searchable alcohol-to-alkane deoxygenation pair while preserving the xanthate intermediate in text provenance. [' + TAG + ']',
        'molecules': [
            ('reactant', 'CC(O)c1ccccc1', '1-phenylethanol', 'reactants_text', 1),
            ('product', 'CCc1ccccc1', 'ethylbenzene', 'products_text', 1),
        ],
    },
    {
        'family': 'Barton-McCombie Radical Deoxygenation Reaction',
        'anchor_image': 'named reactions_99.jpg',
        'anchor_scheme_role': None,
        'page_no_expected': 99,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Barton-McCombie deoxygenation in the synthesis of (±)-Δ9(12)-Capnellene.',
        'reactants_text': 'CSC(=S)O[C@H]1C[C@H]2C3C[C@H](C(C)(C)O3)[C@@H]12',
        'products_text': 'CC1(C)O[C@H]2CC3[C@H]4C[C@@H]3C421',
        'reagents_text': '(n-Bu)3SnH | AIBN',
        'conditions_text': 'toluene, 76%',
        'notes_text': 'Promoted from the multi-step page-level extraction on the Barton-McCombie synthetic applications page. This seed isolates the actual xanthate-to-hydrocarbon deoxygenation step rather than the surrounding route steps. [' + TAG + ']',
        'molecules': [
            ('reactant', 'CSC(=S)O[C@H]1C[C@H]2C3C[C@H](C(C)(C)O3)[C@@H]12', 'capnellene xanthate intermediate', 'reactants_text', 1),
            ('product', 'CC1(C)O[C@H]2CC3[C@H]4C[C@@H]3C421', 'capnellene deoxygenation product', 'products_text', 1),
        ],
    },
    {
        'family': 'Barton-McCombie Radical Deoxygenation Reaction',
        'anchor_image': 'named reactions_99.jpg',
        'anchor_scheme_role': None,
        'page_no_expected': 99,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Barton-McCombie deoxygenation in the synthesis of the octenoic acid side chain of zaragozic acid.',
        'reactants_text': 'CON(C)C(=O)[C@@H](C(C)C)[C@H](C)[C@H](C)OC(=S)SC',
        'products_text': 'CON(C)C(=O)[C@@H](C(C)C)[C@H](C)C',
        'reagents_text': '(n-Bu)3SnH | AIBN',
        'conditions_text': 'toluene, 65%',
        'notes_text': 'Promoted from the multi-step page-level extraction on the Barton-McCombie synthetic applications page. Uses the explicit xanthate-to-deoxygenated-side-chain step that the vision JSON already recovered. [' + TAG + ']',
        'molecules': [
            ('reactant', 'CON(C)C(=O)[C@@H](C(C)C)[C@H](C)[C@H](C)OC(=S)SC', 'zaragozic side-chain xanthate', 'reactants_text', 1),
            ('product', 'CON(C)C(=O)[C@@H](C(C)C)[C@H](C)C', 'deoxygenated zaragozic side-chain fragment', 'products_text', 1),
        ],
    },
]


def canon(smiles: Optional[str]) -> Optional[str]:
    if smiles is None:
        return None
    if Chem is None:
        return smiles
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        raise ValueError(f'RDKit failed to parse SMILES: {smiles}')
    return Chem.MolToSmiles(mol)


def fp_blob(smiles: Optional[str]) -> Optional[bytes]:
    if smiles is None or Chem is None or AllChem is None:
        return None
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return None
    fp = AllChem.GetMorganFingerprintAsBitVect(mol, 2, nBits=2048)
    return bytes(fp.ToBitString(), 'ascii')


def family_norm(name: str) -> str:
    return name.lower().replace('(', ' ').replace(')', ' ').replace('-', ' ').replace('/', ' ').replace(',', ' ').replace('  ', ' ').strip()


def queryable_family_count(conn: sqlite3.Connection) -> int:
    return conn.execute('SELECT COUNT(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1').fetchone()[0]


def queryable_count(conn: sqlite3.Connection) -> int:
    return conn.execute('SELECT COUNT(*) FROM extract_molecules WHERE queryable=1').fetchone()[0]


def normalize_family_aliases(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    out = []
    for alias, official in ALIASES.items():
        for table in ('reaction_extracts', 'extract_molecules'):
            if table == 'reaction_extracts':
                cur = conn.execute(
                    'UPDATE reaction_extracts SET reaction_family_name=?, reaction_family_name_norm=?, updated_at=? WHERE reaction_family_name=?',
                    (official, family_norm(official), NOW, alias),
                )
            else:
                cur = conn.execute(
                    'UPDATE extract_molecules SET reaction_family_name=? WHERE reaction_family_name=?',
                    (official, alias),
                )
            out.append({'table': table, 'from': alias, 'to': official, 'rows_updated': cur.rowcount})
    return out


def backfill_extract_smiles_from_molecules(conn: sqlite3.Connection, families: Sequence[str]) -> List[Dict[str, Any]]:
    out = []
    for family in families:
        rows = conn.execute(
            '''
            SELECT re.id,
                   MAX(CASE WHEN em.role='reactant' AND em.queryable=1 AND em.smiles IS NOT NULL THEN em.smiles END) AS reactant_smiles,
                   MAX(CASE WHEN em.role='product' AND em.queryable=1 AND em.smiles IS NOT NULL THEN em.smiles END) AS product_smiles
            FROM reaction_extracts re
            LEFT JOIN extract_molecules em ON em.extract_id = re.id
            WHERE re.reaction_family_name = ?
            GROUP BY re.id
            ''',
            (family,),
        ).fetchall()
        updated = 0
        for extract_id, reactant, product in rows:
            cur = conn.execute(
                '''
                UPDATE reaction_extracts
                SET reactant_smiles = COALESCE(reactant_smiles, ?),
                    product_smiles = COALESCE(product_smiles, ?),
                    smiles_confidence = CASE
                        WHEN COALESCE(reactant_smiles, ?) IS NOT NULL OR COALESCE(product_smiles, ?) IS NOT NULL THEN MAX(COALESCE(smiles_confidence, 0.0), 0.85)
                        ELSE smiles_confidence
                    END,
                    updated_at = ?
                WHERE id = ?
                ''',
                (reactant, product, reactant, product, NOW, extract_id),
            )
            updated += cur.rowcount
        out.append({'family': family, 'extract_rows_touched': updated})
    return out


def find_anchor(conn: sqlite3.Connection, seed: Dict[str, Any]) -> Tuple[int, int, str]:
    if seed.get('anchor_scheme_role'):
        row = conn.execute(
            '''
            SELECT sc.id, pi.page_no, pi.image_filename
            FROM scheme_candidates sc
            JOIN page_images pi ON pi.id = sc.page_image_id
            WHERE pi.image_filename = ? AND sc.scheme_role = ?
            ORDER BY sc.id
            LIMIT 1
            ''',
            (seed['anchor_image'], seed['anchor_scheme_role']),
        ).fetchone()
        if row:
            return row
    row = conn.execute(
        '''
        SELECT sc.id, pi.page_no, pi.image_filename
        FROM scheme_candidates sc
        JOIN page_images pi ON pi.id = sc.page_image_id
        WHERE pi.image_filename = ?
        ORDER BY CASE WHEN sc.scheme_role='canonical_overview' THEN 0 WHEN sc.scheme_role='application_example' THEN 1 ELSE 2 END, sc.id
        LIMIT 1
        ''',
        (seed['anchor_image'],),
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
        (seed['page_no_expected'],),
    ).fetchone()
    if not row:
        raise RuntimeError(f"Could not find anchor for {seed['family']} / {seed['anchor_image']}")
    return row


def existing_seed_extract(conn: sqlite3.Connection, family: str, notes_text: str) -> Optional[int]:
    row = conn.execute(
        'SELECT id FROM reaction_extracts WHERE reaction_family_name=? AND notes_text=? LIMIT 1',
        (family, notes_text),
    ).fetchone()
    return row[0] if row else None


def insert_seed(conn: sqlite3.Connection, seed: Dict[str, Any]) -> Dict[str, Any]:
    fam = seed['family']
    fam_norm = family_norm(fam)
    existing = existing_seed_extract(conn, fam, seed['notes_text'])
    anchor_id, page_no, image_filename = find_anchor(conn, seed)
    if existing:
        return {
            'family': fam,
            'status': 'skipped_existing',
            'extract_id': existing,
            'page_no': page_no,
            'image_filename': image_filename,
            'inserted_molecules': 0,
        }

    reactant_smiles = None
    product_smiles = None
    for role, smiles, _name, _source_field, queryable in seed['molecules']:
        if queryable != 1 or smiles is None:
            continue
        cs = canon(smiles)
        if role == 'reactant' and reactant_smiles is None:
            reactant_smiles = cs
        if role == 'product' and product_smiles is None:
            product_smiles = cs

    cur = conn.execute(
        '''
        INSERT INTO reaction_extracts (
            scheme_candidate_id, reaction_family_name, reaction_family_name_norm, extract_kind,
            transformation_text, reactants_text, products_text, intermediates_text, reagents_text,
            conditions_text, notes_text, reactant_smiles, product_smiles,
            smiles_confidence, extraction_confidence, parse_status, promote_decision,
            extractor_model, extractor_prompt_version, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            anchor_id,
            fam,
            fam_norm,
            seed['extract_kind'],
            seed.get('transformation_text'),
            seed.get('reactants_text'),
            seed.get('products_text'),
            seed.get('intermediates_text'),
            seed.get('reagents_text'),
            seed.get('conditions_text'),
            seed.get('notes_text'),
            reactant_smiles,
            product_smiles,
            0.98 if (reactant_smiles or product_smiles) else 0.0,
            0.98,
            'curated',
            'promote',
            'manual_curated_seed',
            TAG,
            NOW,
            NOW,
        ),
    )
    extract_id = cur.lastrowid
    inserted = 0
    for role, smiles, name, source_field, queryable in seed['molecules']:
        csmiles = canon(smiles) if smiles else None
        conn.execute(
            '''
            INSERT INTO extract_molecules (
                extract_id, role, smiles, smiles_kind, quality_tier, reaction_family_name,
                source_zip, page_no, queryable, note_text, morgan_fp, normalized_text,
                source_field, structure_source, fg_tags, role_confidence, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                extract_id,
                role,
                csmiles,
                'canonical',
                1 if queryable and csmiles else 3,
                fam,
                'named reactions.pdf',
                page_no,
                1 if queryable and csmiles else 0,
                f'[{TAG}: {name}]' if name else f'[{TAG}]',
                fp_blob(csmiles) if (queryable and csmiles) else None,
                (name.lower() if name else None),
                source_field,
                TAG if csmiles else None,
                None,
                0.98 if csmiles else 0.0,
                NOW,
            ),
        )
        inserted += 1
    return {
        'family': fam,
        'status': 'inserted',
        'extract_id': extract_id,
        'page_no': page_no,
        'image_filename': image_filename,
        'inserted_molecules': inserted,
    }


def verify_family(conn: sqlite3.Connection, family: str) -> Dict[str, Any]:
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
    return {
        'family': family,
        **dict(row),
        **dict(mol),
        'completion_gate_minimum_pass': bool((row['overview_count'] or 0) >= 1 and (row['application_count'] or 0) >= 2 and (row['extract_with_both'] or 0) >= 3),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description='Complete family evidence for Alkene (Olefin) Metathesis and Barton-McCombie Radical Deoxygenation Reaction.')
    ap.add_argument('--db', default='app/labint.db')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--report-dir', default='reports/family_completion_phase3')
    args = ap.parse_args()

    db_path = Path(args.db)
    report_dir = Path(args.report_dir) / dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    report_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        before = {
            'queryable_family_count': queryable_family_count(conn),
            'queryable_count': queryable_count(conn),
        }
        alias_updates = normalize_family_aliases(conn)
        backfill = backfill_extract_smiles_from_molecules(conn, sorted({s['family'] for s in SEEDS}))
        inserted = [insert_seed(conn, seed) for seed in SEEDS]
        after = {
            'queryable_family_count': queryable_family_count(conn),
            'queryable_count': queryable_count(conn),
        }
        verify = [verify_family(conn, fam) for fam in sorted({s['family'] for s in SEEDS})]
        payload = {
            'tag': TAG,
            'db': str(db_path),
            'dry_run': args.dry_run,
            'before': before,
            'alias_updates': alias_updates,
            'backfill': backfill,
            'inserted': inserted,
            'after': after,
            'verify': verify,
        }
        (report_dir / 'family_completion_phase3_summary.json').write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8')
        md = [
            '# Family completion phase3 summary',
            '',
            f'- tag: `{TAG}`',
            f'- db: `{db_path}`',
            f'- dry_run: `{args.dry_run}`',
            '',
            '## Before',
            f"- queryable family count: {before['queryable_family_count']}",
            f"- queryable molecule count: {before['queryable_count']}",
            '',
            '## Alias updates',
        ]
        for item in alias_updates:
            md.append(f"- {item['table']}: {item['from']} → {item['to']} ({item['rows_updated']} rows)")
        md.append('')
        md.append('## Inserted seeds')
        for item in inserted:
            md.append(f"- {item['family']}: {item['status']} | extract_id={item['extract_id']} | page={item['page_no']} | molecules={item['inserted_molecules']}")
        md.append('')
        md.append('## Verification')
        for item in verify:
            md.append(f"### {item['family']}")
            for k in ['extract_count','overview_count','application_count','extract_with_reactant','extract_with_product','extract_with_both','queryable_reactants','queryable_products','completion_gate_minimum_pass']:
                md.append(f'- {k}: {item[k]}')
            md.append('')
        (report_dir / 'family_completion_phase3_summary.md').write_text('\n'.join(md), encoding='utf-8')
        if args.dry_run:
            conn.rollback()
            print('[DRY-RUN] rolled back changes')
        else:
            conn.commit()
            print('[APPLY] committed changes')
        print('summary json:', report_dir / 'family_completion_phase3_summary.json')
        print('summary md:  ', report_dir / 'family_completion_phase3_summary.md')
        return 0
    finally:
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())
