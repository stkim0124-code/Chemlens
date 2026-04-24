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

TAG = 'phase5_shallow_top5_completion_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# data-table alias cleanup only; registry/ontology tables are intentionally left untouched
DATA_ALIASES = {
    'Alder Ene Reaction': 'Alder (Ene) Reaction (Hydro-Allyl Addition)',
    'Alder (ene) Reaction': 'Alder (Ene) Reaction (Hydro-Allyl Addition)',
    'Amadori Rearrangement': 'Amadori Reaction / Rearrangement',
    'Arbuzov Reaction': 'Arbuzov Reaction (Michaelis-Arbuzov Reaction)',
    'Aza-Claisen Rearrangement': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)',
}

FAMILIES = [
    'Alder (Ene) Reaction (Hydro-Allyl Addition)',
    'Amadori Reaction / Rearrangement',
    'Arbuzov Reaction (Michaelis-Arbuzov Reaction)',
    'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)',
    'Baeyer-Villiger Oxidation/Rearrangement',
]

SEEDS: List[Dict[str, Any]] = [
    {
        'family': 'Alder (Ene) Reaction (Hydro-Allyl Addition)',
        'anchor_image': 'named reactions_60.jpg',
        'page_no_expected': 7,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: carbonyl-ene addition of isobutene to formaldehyde to give 3-methyl-3-buten-1-ol.',
        'reactants_text': 'isobutene | formaldehyde',
        'products_text': '3-methyl-3-buten-1-ol',
        'reagents_text': 'thermal or Lewis-acid-promoted ene conditions',
        'conditions_text': 'curated application-class seed representing a simple intermolecular carbonyl-ene addition on a substituted alkene',
        'notes_text': 'Manual curated application-class seed added during the shallow-family top-5 completion sprint for the canonical ene-reaction family. Simplified representative ene application chosen to thicken pair diversity beyond the existing propene/formaldehyde overview. [' + TAG + ']',
        'molecules': [
            ('reactant', 'CC(=C)', 'isobutene', 'reactants_text', 1),
            ('reactant', 'C=O', 'formaldehyde', 'reactants_text', 1),
            ('product', 'CC(=C)CO', '3-methyl-3-buten-1-ol', 'products_text', 1),
        ],
    },
    {
        'family': 'Alder (Ene) Reaction (Hydro-Allyl Addition)',
        'anchor_image': 'named reactions_60.jpg',
        'page_no_expected': 7,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: carbonyl-ene addition of cyclohexene to formaldehyde to give 2-methylenecyclohexan-1-ol.',
        'reactants_text': 'cyclohexene | formaldehyde',
        'products_text': '2-methylenecyclohexan-1-ol',
        'reagents_text': 'thermal or Lewis-acid-promoted ene conditions',
        'conditions_text': 'curated application-class seed representing an intramolecularly biased cyclic alkene substrate in the ene family',
        'notes_text': 'Manual curated application-class seed added during the shallow-family top-5 completion sprint for the canonical ene-reaction family. Provides a cyclic substrate/product pair on the same overview/application page lineage. [' + TAG + ']',
        'molecules': [
            ('reactant', 'C1=CCCCC1', 'cyclohexene', 'reactants_text', 1),
            ('reactant', 'C=O', 'formaldehyde', 'reactants_text', 1),
            ('product', 'C=C1CCCCC1O', '2-methylenecyclohexan-1-ol', 'products_text', 1),
        ],
    },
    {
        'family': 'Amadori Reaction / Rearrangement',
        'anchor_image': 'named reactions_67.jpg',
        'page_no_expected': 15,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Amadori rearrangement of the p-anisidine glycosylamine of D-glucose to the corresponding 1-deoxy-1-(p-anisidino)-D-fructose derivative.',
        'reactants_text': 'p-anisidine D-glucosylamine',
        'products_text': '1-deoxy-1-(p-anisidino)-D-fructose derivative',
        'reagents_text': 'acid or Lewis-acid promoted Amadori conditions',
        'conditions_text': 'curated application-class seed representing an aniline-substituted glucosylamine rearrangement within the Amadori family',
        'notes_text': 'Manual curated application-class seed added during the shallow-family top-5 completion sprint for Amadori chemistry. Uses a p-anisidine variant to provide scaffold diversity beyond the existing aniline overview pair. [' + TAG + ']',
        'molecules': [
            ('reactant', 'COc1ccc(N[C@H]2O[C@H](CO)[C@H](O)[C@@H](O)[C@H]2O)cc1', 'p-anisidine glucosylamine', 'reactants_text', 1),
            ('product', 'COc1ccc(NCC(=O)[C@H](O)[C@H](O)[C@H](O)CO)cc1', 'p-anisidine fructosylamine derivative', 'products_text', 1),
        ],
    },
    {
        'family': 'Amadori Reaction / Rearrangement',
        'anchor_image': 'named reactions_67.jpg',
        'page_no_expected': 15,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Amadori rearrangement of the p-toluidine glycosylamine of D-glucose to the corresponding 1-deoxy-1-(p-toluidino)-D-fructose derivative.',
        'reactants_text': 'p-toluidine D-glucosylamine',
        'products_text': '1-deoxy-1-(p-toluidino)-D-fructose derivative',
        'reagents_text': 'acid or Lewis-acid promoted Amadori conditions',
        'conditions_text': 'curated application-class seed providing a second aromatic amine class for the Amadori family',
        'notes_text': 'Manual curated application-class seed added during the shallow-family top-5 completion sprint for Amadori chemistry. Provides a second explicit glycosylamine-to-ketosamine pair for family depth. [' + TAG + ']',
        'molecules': [
            ('reactant', 'Cc1ccc(N[C@H]2O[C@H](CO)[C@H](O)[C@@H](O)[C@H]2O)cc1', 'p-toluidine glucosylamine', 'reactants_text', 1),
            ('product', 'Cc1ccc(NCC(=O)[C@H](O)[C@H](O)[C@H](O)CO)cc1', 'p-toluidine fructosylamine derivative', 'products_text', 1),
        ],
    },
    {
        'family': 'Arbuzov Reaction (Michaelis-Arbuzov Reaction)',
        'anchor_image': 'named reactions_69.jpg',
        'page_no_expected': 17,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Arbuzov reaction of triethyl phosphite with ethyl bromoacetate to give diethyl phosphonoacetate.',
        'reactants_text': 'triethyl phosphite | ethyl bromoacetate',
        'products_text': 'diethyl phosphonoacetate | bromoethane',
        'reagents_text': 'thermal Michaelis-Arbuzov conditions',
        'conditions_text': 'curated application-class seed representing phosphonoacetate formation for downstream Horner-Wadsworth-Emmons chemistry',
        'notes_text': 'Manual curated application-class seed added during the shallow-family top-5 completion sprint for Arbuzov chemistry. Provides a classic phosphonoacetate-forming example beyond the existing benzyl chloride overview pair. [' + TAG + ']',
        'molecules': [
            ('reactant', 'CCOP(OCC)OCC', 'triethyl phosphite', 'reactants_text', 1),
            ('reactant', 'BrCC(=O)OCC', 'ethyl bromoacetate', 'reactants_text', 1),
            ('product', 'CCOP(=O)(CC(=O)OCC)OCC', 'diethyl phosphonoacetate', 'products_text', 1),
            ('product', 'CCBr', 'bromoethane', 'products_text', 1),
        ],
    },
    {
        'family': 'Arbuzov Reaction (Michaelis-Arbuzov Reaction)',
        'anchor_image': 'named reactions_69.jpg',
        'page_no_expected': 17,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Arbuzov reaction of trimethyl phosphite with benzyl bromide to give dimethyl benzylphosphonate.',
        'reactants_text': 'trimethyl phosphite | benzyl bromide',
        'products_text': 'dimethyl benzylphosphonate | bromomethane',
        'reagents_text': 'thermal Michaelis-Arbuzov conditions',
        'conditions_text': 'curated application-class seed providing an alkoxy-variant phosphite reagent class within the Arbuzov family',
        'notes_text': 'Manual curated application-class seed added during the shallow-family top-5 completion sprint for Arbuzov chemistry. Introduces a methyl phosphite variant to diversify the official search layer. [' + TAG + ']',
        'molecules': [
            ('reactant', 'COP(OC)OC', 'trimethyl phosphite', 'reactants_text', 1),
            ('reactant', 'BrCc1ccccc1', 'benzyl bromide', 'reactants_text', 1),
            ('product', 'COP(=O)(Cc1ccccc1)OC', 'dimethyl benzylphosphonate', 'products_text', 1),
            ('product', 'CBr', 'bromomethane', 'products_text', 1),
        ],
    },
    {
        'family': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)',
        'anchor_image': 'named reactions_73.jpg',
        'page_no_expected': 21,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Aza-Claisen rearrangement of a crotyl-substituted cyclohexenyl enamine to a γ,δ-unsaturated imine.',
        'reactants_text': 'crotyl cyclohexenyl enamine',
        'products_text': 'crotyl-shifted cyclohexyl imine',
        'reagents_text': 'thermal aza-Claisen conditions',
        'conditions_text': 'curated application-class seed representing a substituted allyl side chain in the aza-Claisen family',
        'notes_text': 'Manual curated application-class seed added during the shallow-family top-5 completion sprint for aza-Claisen chemistry. Provides a crotyl-substituted variant beyond the existing allyl overview pair. [' + TAG + ']',
        'molecules': [
            ('reactant', 'CC=CCN(C)C1=CCCCC1', 'crotyl cyclohexenyl enamine', 'reactants_text', 1),
            ('product', 'CC=CCC1CCCCC1=NC', 'crotyl-shifted cyclohexyl imine', 'products_text', 1),
        ],
    },
    {
        'family': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)',
        'anchor_image': 'named reactions_73.jpg',
        'page_no_expected': 21,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Aza-Claisen rearrangement of a methyl-substituted cyclopentenyl enamine to a γ,δ-unsaturated imine.',
        'reactants_text': 'methyl-substituted cyclopentenyl enamine',
        'products_text': 'rearranged cyclopentyl imine',
        'reagents_text': 'thermal aza-Claisen conditions',
        'conditions_text': 'curated application-class seed representing a smaller ring aza-Claisen substrate class',
        'notes_text': 'Manual curated application-class seed added during the shallow-family top-5 completion sprint for aza-Claisen chemistry. Provides a second ring-size example for family depth. [' + TAG + ']',
        'molecules': [
            ('reactant', 'C=CCN(C)C1=CCCC(C)1', 'methyl-substituted cyclopentenyl enamine', 'reactants_text', 1),
            ('product', 'C=CCC1CCC(C)C1=NC', 'rearranged cyclopentyl imine', 'products_text', 1),
        ],
    },
    {
        'family': 'Baeyer-Villiger Oxidation/Rearrangement',
        'anchor_image': 'named reactions_81.jpg',
        'page_no_expected': 29,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Baeyer-Villiger oxidation of acetophenone to phenyl acetate.',
        'reactants_text': 'acetophenone',
        'products_text': 'phenyl acetate',
        'reagents_text': 'peracid (e.g. mCPBA)',
        'conditions_text': 'curated application-class seed representing aryl ketone oxidation in the Baeyer-Villiger family',
        'notes_text': 'Manual curated application-class seed added during the shallow-family top-5 completion sprint for Baeyer-Villiger chemistry. Adds an aryl ketone example beyond the existing cyclic ketone overview pair. [' + TAG + ']',
        'molecules': [
            ('reactant', 'CC(=O)c1ccccc1', 'acetophenone', 'reactants_text', 1),
            ('product', 'CC(=O)Oc1ccccc1', 'phenyl acetate', 'products_text', 1),
        ],
    },
    {
        'family': 'Baeyer-Villiger Oxidation/Rearrangement',
        'anchor_image': 'named reactions_81.jpg',
        'page_no_expected': 29,
        'extract_kind': 'application_example',
        'transformation_text': 'Application example: Baeyer-Villiger oxidation of cyclobutanone to γ-butyrolactone.',
        'reactants_text': 'cyclobutanone',
        'products_text': 'γ-butyrolactone',
        'reagents_text': 'peracid (e.g. mCPBA)',
        'conditions_text': 'curated application-class seed representing ring-expanding oxidation of a small cyclic ketone',
        'notes_text': 'Manual curated application-class seed added during the shallow-family top-5 completion sprint for Baeyer-Villiger chemistry. Adds a second ring-expansion class distinct from cyclohexanone. [' + TAG + ']',
        'molecules': [
            ('reactant', 'O=C1CCC1', 'cyclobutanone', 'reactants_text', 1),
            ('product', 'O=C1OCCC1', 'γ-butyrolactone', 'products_text', 1),
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
    ap = argparse.ArgumentParser(description='Complete shallow-family sprint round 1 for five canonical families.')
    ap.add_argument('--db', default='app/labint.db')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--report-dir', default='reports/family_completion_phase5_shallow_top5')
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
            'note': 'This patch intentionally does not mutate reaction_family_patterns. final_state_verifier/dashboard should continue to collapse these short-form aliases for display.',
        }
        (report_dir / 'family_completion_phase5_shallow_top5_summary.json').write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8')
        md = [
            '# Family completion phase5 shallow-top5 summary','',
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
        md += ['', '## After', f"- queryable family count: {after['queryable_family_count']}", f"- queryable molecule count: {after['queryable_count']}"]
        (report_dir / 'family_completion_phase5_shallow_top5_summary.md').write_text('\n'.join(md) + '\n', encoding='utf-8')

        if args.dry_run:
            conn.rollback()
            print('[DRY-RUN] rolled back changes')
        else:
            conn.commit()
            print('[APPLY] committed changes')
        print(f'summary json: {report_dir / "family_completion_phase5_shallow_top5_summary.json"}')
        print(f'summary md:   {report_dir / "family_completion_phase5_shallow_top5_summary.md"}')
        return 0
    finally:
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())
