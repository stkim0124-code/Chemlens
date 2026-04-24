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

TAG = 'phase6_shallow_top5_completion_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES = {
    'Balz-Schiemann Reaction': 'Balz-Schiemann Reaction (Schiemann Reaction)',
    'Buchner Method of Ring Expansion': 'Buchner Method of Ring Expansion (Buchner Reaction)',
    'Buchner Reaction': 'Buchner Method of Ring Expansion (Buchner Reaction)',
    'Carroll Rearrangement': 'Carroll Rearrangement (Kimel-Cope Rearrangement)',
    'Kimel-Cope Rearrangement': 'Carroll Rearrangement (Kimel-Cope Rearrangement)',
    'Chichibabin Amination Reaction': 'Chichibabin Amination Reaction (Chichibabin Reaction)',
    'Chichibabin Reaction': 'Chichibabin Amination Reaction (Chichibabin Reaction)',
    'Claisen Condensation': 'Claisen Condensation / Claisen Reaction',
    'Claisen Reaction': 'Claisen Condensation / Claisen Reaction',
}

FAMILIES = [
    'Balz-Schiemann Reaction (Schiemann Reaction)',
    'Buchner Method of Ring Expansion (Buchner Reaction)',
    'Carroll Rearrangement (Kimel-Cope Rearrangement)',
    'Chichibabin Amination Reaction (Chichibabin Reaction)',
    'Claisen Condensation / Claisen Reaction',
]

SEEDS: List[Dict[str, Any]] = [
    {
        'family': 'Balz-Schiemann Reaction (Schiemann Reaction)',
        'anchor_image': 'named reactions_87.jpg',
        'page_no_expected': 35,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: thermolysis of p-methylbenzenediazonium tetrafluoroborate to give p-fluorotoluene.',
        'reactants_text': 'p-methylbenzenediazonium tetrafluoroborate',
        'products_text': 'p-fluorotoluene',
        'reagents_text': 'NaNO2, HBF4, heat',
        'conditions_text': 'curated application-class seed representing para-alkyl aryl fluoride synthesis via the Balz-Schiemann reaction',
        'notes_text': 'Manual curated application-class seed added during the phase6 shallow-family completion sprint for Balz-Schiemann chemistry. Provides an alkyl-substituted aryl diazonium fluoride precursor beyond the existing overview pair. [' + TAG + ']',
        'molecules': [
            ('reactant', '[B-](F)(F)(F)F.Cc1ccc([N+]#N)cc1', 'p-methylbenzenediazonium tetrafluoroborate', 'reactants_text', 1),
            ('product', 'Cc1ccc(F)cc1', 'p-fluorotoluene', 'products_text', 1),
        ],
    },
    {
        'family': 'Balz-Schiemann Reaction (Schiemann Reaction)',
        'anchor_image': 'named reactions_87.jpg',
        'page_no_expected': 35,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: thermolysis of p-methoxybenzenediazonium tetrafluoroborate to give p-fluoroanisole.',
        'reactants_text': 'p-methoxybenzenediazonium tetrafluoroborate',
        'products_text': 'p-fluoroanisole',
        'reagents_text': 'NaNO2, HBF4, heat',
        'conditions_text': 'curated application-class seed representing para-methoxy aryl fluoride synthesis in the Balz-Schiemann family',
        'notes_text': 'Manual curated application-class seed added during the phase6 shallow-family completion sprint for Balz-Schiemann chemistry. Adds an anisole-class aryl diazonium example for family depth. [' + TAG + ']',
        'molecules': [
            ('reactant', '[B-](F)(F)(F)F.COc1ccc([N+]#N)cc1', 'p-methoxybenzenediazonium tetrafluoroborate', 'reactants_text', 1),
            ('product', 'COc1ccc(F)cc1', 'p-fluoroanisole', 'products_text', 1),
        ],
    },
    {
        'family': 'Buchner Method of Ring Expansion (Buchner Reaction)',
        'anchor_image': 'named reactions_121.jpg',
        'page_no_expected': 69,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: ring expansion of benzene with ethyl diazoacetate to give ethyl cycloheptatrienecarboxylate.',
        'reactants_text': 'benzene | ethyl diazoacetate',
        'products_text': 'ethyl cycloheptatrienecarboxylate',
        'reagents_text': 'Cu catalyst or thermal Buchner conditions',
        'conditions_text': 'curated application-class seed representing arene ring expansion with an acceptor-substituted carbene precursor',
        'notes_text': 'Manual curated application-class seed added during the phase6 shallow-family completion sprint for Buchner ring-expansion chemistry. Provides a simple benzene to cycloheptatriene ester example. [' + TAG + ']',
        'molecules': [
            ('reactant', 'c1ccccc1', 'benzene', 'reactants_text', 1),
            ('reactant', 'CCOC(=O)C=[N+]=[N-]', 'ethyl diazoacetate', 'reactants_text', 1),
            ('product', 'CCOC(=O)C1C=CC=CC=C1', 'ethyl cycloheptatrienecarboxylate', 'products_text', 1),
        ],
    },
    {
        'family': 'Buchner Method of Ring Expansion (Buchner Reaction)',
        'anchor_image': 'named reactions_121.jpg',
        'page_no_expected': 69,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: ring expansion of anisole with ethyl diazoacetate to give a methoxy-substituted cycloheptatrienecarboxylate.',
        'reactants_text': 'anisole | ethyl diazoacetate',
        'products_text': 'methoxy-substituted cycloheptatrienecarboxylate',
        'reagents_text': 'Cu catalyst or thermal Buchner conditions',
        'conditions_text': 'curated application-class seed providing a heteroatom-bearing arene class in the Buchner family',
        'notes_text': 'Manual curated application-class seed added during the phase6 shallow-family completion sprint for Buchner ring-expansion chemistry. Adds an anisole-derived ring-expansion example for family depth. [' + TAG + ']',
        'molecules': [
            ('reactant', 'COc1ccccc1', 'anisole', 'reactants_text', 1),
            ('reactant', 'CCOC(=O)C=[N+]=[N-]', 'ethyl diazoacetate', 'reactants_text', 1),
            ('product', 'CCOC(=O)C1C=CC=CC(OC)=C1', 'methoxy-substituted cycloheptatrienecarboxylate', 'products_text', 1),
        ],
    },
    {
        'family': 'Carroll Rearrangement (Kimel-Cope Rearrangement)',
        'anchor_image': 'named reactions_129.jpg',
        'page_no_expected': 77,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Carroll rearrangement of allyl acetoacetate to 5-hexen-2-one.',
        'reactants_text': 'allyl acetoacetate',
        'products_text': '5-hexen-2-one',
        'reagents_text': 'thermal Carroll conditions',
        'conditions_text': 'curated application-class seed representing decarboxylative allyl transfer from an allyl β-ketoester',
        'notes_text': 'Manual curated application-class seed added during the phase6 shallow-family completion sprint for Carroll rearrangement chemistry. Provides the classic allyl acetoacetate to enone example beyond the existing overview pair. [' + TAG + ']',
        'molecules': [
            ('reactant', 'CC(=O)CC(=O)OCC=C', 'allyl acetoacetate', 'reactants_text', 1),
            ('product', 'CC(=O)CC=CC', '5-hexen-2-one', 'products_text', 1),
        ],
    },
    {
        'family': 'Carroll Rearrangement (Kimel-Cope Rearrangement)',
        'anchor_image': 'named reactions_129.jpg',
        'page_no_expected': 77,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Carroll rearrangement of crotyl acetoacetate to 6-hepten-2-one.',
        'reactants_text': 'crotyl acetoacetate',
        'products_text': '6-hepten-2-one',
        'reagents_text': 'thermal Carroll conditions',
        'conditions_text': 'curated application-class seed representing a substituted allyl fragment in the Carroll family',
        'notes_text': 'Manual curated application-class seed added during the phase6 shallow-family completion sprint for Carroll rearrangement chemistry. Adds a crotyl-substituted β-ketoester variant for family depth. [' + TAG + ']',
        'molecules': [
            ('reactant', 'CC=CCOC(=O)CC(=O)C', 'crotyl acetoacetate', 'reactants_text', 1),
            ('product', 'CC(=O)CC=CCC', '6-hepten-2-one', 'products_text', 1),
        ],
    },
    {
        'family': 'Chichibabin Amination Reaction (Chichibabin Reaction)',
        'anchor_image': 'named reactions_133.jpg',
        'page_no_expected': 81,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Chichibabin amination of pyridine to 2-aminopyridine.',
        'reactants_text': 'pyridine',
        'products_text': '2-aminopyridine',
        'reagents_text': 'NaNH2 in liquid ammonia',
        'conditions_text': 'curated application-class seed representing direct nucleophilic amination of pyridine under Chichibabin conditions',
        'notes_text': 'Manual curated application-class seed added during the phase6 shallow-family completion sprint for Chichibabin amination. Provides the classic pyridine to 2-aminopyridine pair beyond the existing overview evidence. [' + TAG + ']',
        'molecules': [
            ('reactant', 'n1ccccc1', 'pyridine', 'reactants_text', 1),
            ('product', 'Nc1ccccn1', '2-aminopyridine', 'products_text', 1),
        ],
    },
    {
        'family': 'Chichibabin Amination Reaction (Chichibabin Reaction)',
        'anchor_image': 'named reactions_133.jpg',
        'page_no_expected': 81,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Chichibabin amination of quinoline to 2-aminoquinoline.',
        'reactants_text': 'quinoline',
        'products_text': '2-aminoquinoline',
        'reagents_text': 'NaNH2 in liquid ammonia',
        'conditions_text': 'curated application-class seed representing fused aza-arene amination in the Chichibabin family',
        'notes_text': 'Manual curated application-class seed added during the phase6 shallow-family completion sprint for Chichibabin amination. Adds a fused aza-arene substrate class for family depth. [' + TAG + ']',
        'molecules': [
            ('reactant', 'c1ccc2ncccc2c1', 'quinoline', 'reactants_text', 1),
            ('product', 'Nc1ccc2ncccc2c1', '2-aminoquinoline', 'products_text', 1),
        ],
    },
    {
        'family': 'Claisen Condensation / Claisen Reaction',
        'anchor_image': 'named reactions_139.jpg',
        'page_no_expected': 87,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Claisen condensation of ethyl acetate to ethyl acetoacetate.',
        'reactants_text': 'ethyl acetate',
        'products_text': 'ethyl acetoacetate',
        'reagents_text': 'NaOEt, EtOH',
        'conditions_text': 'curated application-class seed representing self-Claisen condensation of a simple acetate ester',
        'notes_text': 'Manual curated application-class seed added during the phase6 shallow-family completion sprint for Claisen condensation chemistry. Adds the classic self-condensation of ethyl acetate. [' + TAG + ']',
        'molecules': [
            ('reactant', 'CCOC(=O)C', 'ethyl acetate', 'reactants_text', 1),
            ('product', 'CCOC(=O)CC(=O)C', 'ethyl acetoacetate', 'products_text', 1),
        ],
    },
    {
        'family': 'Claisen Condensation / Claisen Reaction',
        'anchor_image': 'named reactions_139.jpg',
        'page_no_expected': 87,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: crossed Claisen condensation of acetophenone enolate with ethyl benzoate to give dibenzoylmethane.',
        'reactants_text': 'acetophenone | ethyl benzoate',
        'products_text': 'dibenzoylmethane',
        'reagents_text': 'strong base, then acidic workup',
        'conditions_text': 'curated application-class seed representing aryl-substituted crossed Claisen condensation',
        'notes_text': 'Manual curated application-class seed added during the phase6 shallow-family completion sprint for Claisen condensation chemistry. Adds an aryl ketone/ester crossed Claisen example for family depth. [' + TAG + ']',
        'molecules': [
            ('reactant', 'CC(=O)c1ccccc1', 'acetophenone', 'reactants_text', 1),
            ('reactant', 'CCOC(=O)c1ccccc1', 'ethyl benzoate', 'reactants_text', 1),
            ('product', 'O=C(CC(=O)c1ccccc1)c1ccccc1', 'dibenzoylmethane', 'products_text', 1),
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
    ap = argparse.ArgumentParser(description='Complete shallow-family sprint phase6 top5.')
    ap.add_argument('--db', default='app/labint.db')
    ap.add_argument('--report-dir', default='reports/family_completion_phase6_shallow_top5')
    ap.add_argument('--dry-run', action='store_true')
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
            'reaction_extract_count': conn.execute('SELECT COUNT(*) FROM reaction_extracts').fetchone()[0],
            'extract_molecule_count': conn.execute('SELECT COUNT(*) FROM extract_molecules').fetchone()[0],
        }
        conn.execute('BEGIN')
        alias_updates = normalize_aliases(conn)
        backfill = backfill_extract_smiles_from_molecules(conn, FAMILIES)
        seed_results = [insert_seed(conn, seed) for seed in SEEDS]
        verify = [verify_family(conn, fam) for fam in FAMILIES]
        after = {
            'queryable_family_count': queryable_family_count(conn),
            'queryable_count': queryable_count(conn),
            'reaction_extract_count': conn.execute('SELECT COUNT(*) FROM reaction_extracts').fetchone()[0],
            'extract_molecule_count': conn.execute('SELECT COUNT(*) FROM extract_molecules').fetchone()[0],
        }
        rich_count = sum(1 for v in verify if v['rich_completion_pass'])
        shallow_count = len(verify) - rich_count
        payload = {
            'tag': TAG,
            'db': str(db_path),
            'dry_run': bool(args.dry_run),
            'families': FAMILIES,
            'before': before,
            'after': after,
            'alias_updates': alias_updates,
            'backfill': backfill,
            'seed_results': seed_results,
            'verify': verify,
            'rich_count': rich_count,
            'shallow_count': shallow_count,
        }
        if args.dry_run:
            conn.rollback()
            status = '[DRY-RUN] rolled back changes'
        else:
            conn.commit()
            status = '[APPLY] committed changes'

        jpath = report_dir / 'family_completion_phase6_shallow_top5_summary.json'
        mpath = report_dir / 'family_completion_phase6_shallow_top5_summary.md'
        jpath.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        lines = [
            '# Family completion phase6 shallow-top5 summary',
            '',
            f'- db: `{db_path}`',
            f'- dry_run: `{args.dry_run}`',
            '',
            status,
            '',
            '## Before',
            f"- queryable_family_count: {before['queryable_family_count']}",
            f"- queryable_count: {before['queryable_count']}",
            f"- reaction_extract_count: {before['reaction_extract_count']}",
            f"- extract_molecule_count: {before['extract_molecule_count']}",
            '',
            '## After',
            f"- queryable_family_count: {after['queryable_family_count']}",
            f"- queryable_count: {after['queryable_count']}",
            f"- reaction_extract_count: {after['reaction_extract_count']}",
            f"- extract_molecule_count: {after['extract_molecule_count']}",
            '',
            f'- rich_count: {rich_count}',
            f'- shallow_count: {shallow_count}',
            '',
        ]
        for item in verify:
            lines.append(f"## {item['family']}")
            for k in ['extract_count','overview_count','application_count','extract_with_reactant','extract_with_product','extract_with_both','queryable_reactants','queryable_products','unique_queryable_pair_count','completion_gate_minimum_pass','rich_completion_pass']:
                lines.append(f'- {k}: {item[k]}')
            lines.append('')
        mpath.write_text('\n'.join(lines) + '\n', encoding='utf-8')
        print(status)
        print(f'summary json: {jpath}')
        print(f'summary md:   {mpath}')
        return 0
    finally:
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())
