import argparse
import datetime as dt
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from rdkit import Chem
    from rdkit.Chem import rdFingerprintGenerator
except Exception:
    Chem = None
    rdFingerprintGenerator = None

TAG = 'phase4_family_completion_missing_trio_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Data-table alias cleanup only. We intentionally do NOT mutate reaction_family_patterns here,
# because those legacy long-form rows act as registry/ontology history rows and may carry unique constraints.
DATA_ALIASES = {
    'Alkene (olefin) Metathesis': 'Alkene (Olefin) Metathesis',
    'Barton-Mccombie Radical Deoxygenation Reaction': 'Barton-McCombie Radical Deoxygenation Reaction',
    'Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement': 'Fries Rearrangement',
    'Hofmann-Löffler-Freytag Reaction (Remote Functionalization)': 'Hofmann-Loffler-Freytag Reaction',
    'Houben-Hoesch Reaction/Synthesis': 'Houben-Hoesch Reaction',
}

FAMILIES = [
    'Fries Rearrangement',
    'Hofmann-Loffler-Freytag Reaction',
    'Houben-Hoesch Reaction',
]

SEEDS: List[Dict[str, Any]] = [
    {
        'family': 'Fries Rearrangement',
        'anchor_image': 'named reactions_233.jpg',
        'page_no_expected': 181,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Fries rearrangement of phenyl benzoate to 2-hydroxybenzophenone.',
        'reactants_text': 'phenyl benzoate',
        'products_text': '2-hydroxybenzophenone',
        'reagents_text': 'AlCl3',
        'conditions_text': 'curated application-class seed representing Lewis-acid promoted acyl migration to an ortho-acylated phenol',
        'notes_text': 'Manual curated application-class seed added during the missing-family completion sprint for Fries Rearrangement. Simplified representative application pair, not claimed to be the exact textbook substrate. [' + TAG + ']',
        'molecules': [
            ('reactant', 'O=C(Oc1ccccc1)c1ccccc1', 'phenyl benzoate', 'reactants_text', 1),
            ('product', 'O=C(c1ccccc1)c1ccccc1O', '2-hydroxybenzophenone', 'products_text', 1),
        ],
    },
    {
        'family': 'Fries Rearrangement',
        'anchor_image': 'named reactions_233.jpg',
        'page_no_expected': 181,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Fries rearrangement of 1-naphthyl acetate to 2-acetyl-1-naphthol.',
        'reactants_text': '1-naphthyl acetate',
        'products_text': '2-acetyl-1-naphthol',
        'reagents_text': 'AlCl3',
        'conditions_text': 'curated application-class seed representing acyl migration on a fused aromatic system',
        'notes_text': 'Manual curated application-class seed added during the missing-family completion sprint for Fries Rearrangement to thicken scaffold diversity beyond simple phenyl acetate. [' + TAG + ']',
        'molecules': [
            ('reactant', 'CC(=O)Oc1cccc2ccccc12', '1-naphthyl acetate', 'reactants_text', 1),
            ('product', 'CC(=O)c1c(O)ccc2ccccc12', '2-acetyl-1-naphthol', 'products_text', 1),
        ],
    },
    {
        'family': 'Hofmann-Loffler-Freytag Reaction',
        'anchor_image': 'named reactions_261.jpg',
        'page_no_expected': 209,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: intramolecular Hofmann-Loffler-Freytag cyclization of an N-chloro-N-butyl p-toluenesulfonamide to an N-tosylpyrrolidine.',
        'reactants_text': 'N-chloro-N-butyl p-toluenesulfonamide',
        'products_text': 'N-tosylpyrrolidine',
        'reagents_text': 'hν | acid',
        'conditions_text': 'curated application-class seed representing remote 1,5-hydrogen abstraction followed by C-N bond formation',
        'notes_text': 'Manual curated application-class seed added during the missing-family completion sprint for Hofmann-Loffler-Freytag Reaction. Simplified representative sulfonamide-based HLF cyclization. [' + TAG + ']',
        'molecules': [
            ('reactant', 'Cc1ccc(S(=O)(=O)N(Cl)CCCC)cc1', 'N-chloro-N-butyl p-toluenesulfonamide', 'reactants_text', 1),
            ('product', 'Cc1ccc(S(=O)(=O)N1CCCC1)cc1', 'N-tosylpyrrolidine', 'products_text', 1),
        ],
    },
    {
        'family': 'Hofmann-Loffler-Freytag Reaction',
        'anchor_image': 'named reactions_261.jpg',
        'page_no_expected': 209,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: intramolecular Hofmann-Loffler-Freytag cyclization of an N-chloro-N-pentyl p-toluenesulfonamide to an N-tosylpiperidine.',
        'reactants_text': 'N-chloro-N-pentyl p-toluenesulfonamide',
        'products_text': 'N-tosylpiperidine',
        'reagents_text': 'hν | acid',
        'conditions_text': 'curated application-class seed representing remote C-H amination to a six-membered cyclic amine',
        'notes_text': 'Manual curated application-class seed added during the missing-family completion sprint for Hofmann-Loffler-Freytag Reaction to provide a second ring-size example. [' + TAG + ']',
        'molecules': [
            ('reactant', 'Cc1ccc(S(=O)(=O)N(Cl)CCCCC)cc1', 'N-chloro-N-pentyl p-toluenesulfonamide', 'reactants_text', 1),
            ('product', 'Cc1ccc(S(=O)(=O)N1CCCCC1)cc1', 'N-tosylpiperidine', 'products_text', 1),
        ],
    },
    {
        'family': 'Houben-Hoesch Reaction',
        'anchor_image': 'named reactions_269.jpg',
        'page_no_expected': 217,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Houben-Hoesch acylation of phloroglucinol with benzonitrile to 2,4,6-trihydroxybenzophenone.',
        'reactants_text': 'phloroglucinol | benzonitrile',
        'products_text': '2,4,6-trihydroxybenzophenone',
        'reagents_text': 'HCl | ZnCl2',
        'conditions_text': 'curated application-class seed representing nitrile-based aromatic acylation to an aryl ketone',
        'notes_text': 'Manual curated application-class seed added during the missing-family completion sprint for Houben-Hoesch Reaction to complement the existing acetyl example with an aryl nitrile substrate. [' + TAG + ']',
        'molecules': [
            ('reactant', 'N#Cc1ccccc1', 'benzonitrile', 'reactants_text', 1),
            ('reactant', 'Oc1cc(O)cc(O)c1', 'phloroglucinol', 'reactants_text', 1),
            ('product', 'O=C(c1ccccc1)c1c(O)cc(O)cc1O', '2,4,6-trihydroxybenzophenone', 'products_text', 1),
        ],
    },
    {
        'family': 'Houben-Hoesch Reaction',
        'anchor_image': 'named reactions_269.jpg',
        'page_no_expected': 217,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Houben-Hoesch acylation of resorcinol with acetonitrile to 2,4-dihydroxyacetophenone.',
        'reactants_text': 'resorcinol | acetonitrile',
        'products_text': '2,4-dihydroxyacetophenone',
        'reagents_text': 'HCl | ZnCl2',
        'conditions_text': 'curated application-class seed representing simple aliphatic nitrile acylation on an activated aromatic ring',
        'notes_text': 'Manual curated application-class seed added during the missing-family completion sprint for Houben-Hoesch Reaction to provide a second explicit substrate class. [' + TAG + ']',
        'molecules': [
            ('reactant', 'CC#N', 'acetonitrile', 'reactants_text', 1),
            ('reactant', 'Oc1cccc(O)c1', 'resorcinol', 'reactants_text', 1),
            ('product', 'CC(=O)c1ccc(O)cc1O', '2,4-dihydroxyacetophenone', 'products_text', 1),
        ],
    },
]


def family_norm(name: str) -> str:
    txt = name.lower()
    for ch in '(),-/':
        txt = txt.replace(ch, ' ')
    return ' '.join(txt.split())


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
    if smiles is None or Chem is None or rdFingerprintGenerator is None:
        return None
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return None
    gen = rdFingerprintGenerator.GetMorganGenerator(radius=2, fpSize=2048)
    fp = gen.GetFingerprint(mol)
    return bytes(fp.ToBitString(), 'ascii')


def queryable_family_count(conn: sqlite3.Connection) -> int:
    return conn.execute('SELECT COUNT(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1').fetchone()[0]


def queryable_count(conn: sqlite3.Connection) -> int:
    return conn.execute('SELECT COUNT(*) FROM extract_molecules WHERE queryable=1').fetchone()[0]


def normalize_aliases(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for alias, official in DATA_ALIASES.items():
        fam_norm = family_norm(official)
        cur = conn.execute(
            'UPDATE reaction_extracts SET reaction_family_name=?, reaction_family_name_norm=?, updated_at=? WHERE reaction_family_name=?',
            (official, fam_norm, NOW, alias),
        )
        out.append({'table': 'reaction_extracts', 'from': alias, 'to': official, 'rows_updated': cur.rowcount})
        cur = conn.execute(
            'UPDATE extract_molecules SET reaction_family_name=? WHERE reaction_family_name=?',
            (official, alias),
        )
        out.append({'table': 'extract_molecules', 'from': alias, 'to': official, 'rows_updated': cur.rowcount})
    return out


def backfill_extract_smiles_from_molecules(conn: sqlite3.Connection, families: Sequence[str]) -> List[Dict[str, Any]]:
    out = []
    for family in families:
        rows = conn.execute(
            '''
            SELECT re.id,
                   GROUP_CONCAT(CASE WHEN em.role='reactant' AND em.queryable=1 AND em.smiles IS NOT NULL THEN em.smiles END, ' | ') AS reactant_smiles,
                   GROUP_CONCAT(CASE WHEN em.role='product' AND em.queryable=1 AND em.smiles IS NOT NULL THEN em.smiles END, ' | ') AS product_smiles
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
                SET reactant_smiles = CASE WHEN COALESCE(reactant_smiles,'')='' THEN COALESCE(?, reactant_smiles) ELSE reactant_smiles END,
                    product_smiles  = CASE WHEN COALESCE(product_smiles,'')='' THEN COALESCE(?, product_smiles) ELSE product_smiles END,
                    smiles_confidence = CASE
                        WHEN (COALESCE(reactant_smiles,'')='' AND ? IS NOT NULL) OR (COALESCE(product_smiles,'')='' AND ? IS NOT NULL)
                        THEN MAX(COALESCE(smiles_confidence, 0.0), 0.85)
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
    row = None
    try:
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
    except sqlite3.DatabaseError:
        row = None
    if row:
        return int(row[0]), int(row[1]), row[2]
    try:
        row = conn.execute(
            '''
            SELECT sc.id
            FROM scheme_candidates sc
            JOIN page_images pi ON pi.id = sc.page_image_id
            WHERE pi.page_no = ?
            ORDER BY sc.id
            LIMIT 1
            ''',
            (seed['page_no_expected'],),
        ).fetchone()
        if row:
            return int(row[0]), seed['page_no_expected'], seed['anchor_image']
    except sqlite3.DatabaseError:
        pass
    # Final fallback: reuse the existing overview extract anchor for the same family.
    row = conn.execute(
        '''
        SELECT scheme_candidate_id
        FROM reaction_extracts
        WHERE reaction_family_name=? AND scheme_candidate_id IS NOT NULL
        ORDER BY CASE WHEN extract_kind='canonical_overview' THEN 0 ELSE 1 END, id
        LIMIT 1
        ''',
        (seed['family'],),
    ).fetchone()
    if row and row[0] is not None:
        return int(row[0]), seed['page_no_expected'], seed['anchor_image']
    raise RuntimeError(f"Could not find usable anchor for {seed['family']} / {seed['anchor_image']}")


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

    reactants_for_extract: List[str] = []
    products_for_extract: List[str] = []
    for role, smiles, _name, _source_field, queryable in seed['molecules']:
        if queryable != 1 or smiles is None:
            continue
        cs = canon(smiles)
        if role == 'reactant':
            reactants_for_extract.append(cs)
        elif role == 'product':
            products_for_extract.append(cs)

    reactant_smiles = ' | '.join(sorted(set(reactants_for_extract))) if reactants_for_extract else None
    product_smiles = ' | '.join(sorted(set(products_for_extract))) if products_for_extract else None

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
        WHERE reaction_family_name=?
          AND COALESCE(reactant_smiles,'')<>''
          AND COALESCE(product_smiles,'')<>''
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


def main() -> int:
    ap = argparse.ArgumentParser(description='Complete the three remaining canonicalized missing families.')
    ap.add_argument('--db', default='app/labint.db')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--report-dir', default='reports/family_completion_phase4_missing_trio')
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
        alias_updates = normalize_aliases(conn)
        backfill = backfill_extract_smiles_from_molecules(conn, FAMILIES)
        inserted = [insert_seed(conn, seed) for seed in SEEDS]
        after = {
            'queryable_family_count': queryable_family_count(conn),
            'queryable_count': queryable_count(conn),
        }
        verify = [verify_family(conn, fam) for fam in FAMILIES]
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
            'registry_alias_rows_left_untouched': list(DATA_ALIASES.keys()),
            'note': 'This patch intentionally does not mutate reaction_family_patterns. The dashboard/view layer should continue to collapse legacy registry aliases for display.',
        }
        (report_dir / 'family_completion_phase4_missing_trio_summary.json').write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8')
        md = [
            '# Family completion phase4 missing-trio summary','',
            f'- tag: `{TAG}`',
            f'- db: `{db_path}`',
            f'- dry_run: `{args.dry_run}`','',
            '## Before',
            f"- queryable family count: {before['queryable_family_count']}",
            f"- queryable molecule count: {before['queryable_count']}",'',
            '## Alias updates (data tables only)',
        ]
        for item in alias_updates:
            md.append(f"- {item['table']}: {item['from']} → {item['to']} ({item['rows_updated']} rows)")
        md += ['', '## Inserted seeds']
        for item in inserted:
            md.append(f"- {item['family']}: {item['status']} | extract_id={item['extract_id']} | page={item['page_no']} | molecules={item['inserted_molecules']}")
        md += ['', '## Verification']
        for item in verify:
            md.append(f"### {item['family']}")
            for k in ['extract_count','overview_count','application_count','extract_with_reactant','extract_with_product','extract_with_both','queryable_reactants','queryable_products','unique_queryable_pair_count','completion_gate_minimum_pass','rich_completion_pass']:
                md.append(f'- {k}: {item[k]}')
            md.append('')
        md += ['## After',
               f"- queryable family count: {after['queryable_family_count']}",
               f"- queryable molecule count: {after['queryable_count']}",'',
               '## Important note',
               '- This patch intentionally leaves legacy alias rows in reaction_family_patterns untouched.',
               '- Continue using final_state_verifier + family_completion_dashboard canonicalization for display/tracking after apply.',]
        (report_dir / 'family_completion_phase4_missing_trio_summary.md').write_text('\n'.join(md), encoding='utf-8')
        if args.dry_run:
            conn.rollback()
            print('[DRY-RUN] rolled back changes')
        else:
            conn.commit()
            print('[APPLY] committed changes')
        print('summary json:', report_dir / 'family_completion_phase4_missing_trio_summary.json')
        print('summary md:  ', report_dir / 'family_completion_phase4_missing_trio_summary.md')
        return 0
    finally:
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())
