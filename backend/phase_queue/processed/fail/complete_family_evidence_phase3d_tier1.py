"""Phase 3d tier-1 pilot — depth sprint for top-5 rich-but-thin admission victims.

Adds 3 structurally-distinct D/E/F seeds per family on top of existing A/B/C,
pushing unique_queryable_pair_count from 3 → 6 without altering rich/shallow status.

Target families (rank 1, 2, 3, 5, 10 from phase3d_candidates_top.md):
  1.  Hantzsch Dihydropyridine Synthesis
  2.  Feist-Bénary Furan Synthesis
  3.  Knoevenagel Condensation
  5.  Kumada Cross-Coupling
  10. Dess-Martin Oxidation

To stage for execution, copy this file to
  C:\\chemlens\\backend\\phase_queue\\inbox\\

The rdkit_daemon at pid 33356 (daemon process started 2026-04-20T23:31) will
pick it up, move to processing, run with --db app/labint.db and --report-dir
reports/family_completion_phase3d_tier1, then archive into processed/pass/ or
processed/fail/.

NOTE: This file is a *pilot* and has NOT been committed via the daemon yet.
User review of the SMILES before submission is recommended — especially for
Hantzsch and Feist-Bénary where multi-component condensation makes the
balanced reactant list non-trivial.
"""

import argparse
import datetime as dt
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from rdkit import Chem
    from rdkit.Chem import rdFingerprintGenerator
except Exception:
    Chem = None
    rdFingerprintGenerator = None

# --- ensure backend_dir is on sys.path so smiles_guard imports ---
import os as _os
import sys as _sys
from pathlib import Path as _Path
_HERE = _Path(__file__).resolve().parent
_cand = _HERE
for _ in range(4):
    if (_cand / "smiles_guard.py").exists():
        if str(_cand) not in _sys.path:
            _sys.path.insert(0, str(_cand))
        break
    _cand = _cand.parent
for _p in _os.environ.get("PYTHONPATH", "").split(_os.pathsep):
    if _p and _p not in _sys.path:
        _sys.path.insert(0, _p)
# ------------------------------------------------------------------
try:
    from smiles_guard import is_smiles_safe
except Exception as e:
    print(f'[phase3d] ERROR: could not import smiles_guard: {e}')
    raise

TAG = 'phase3d_tier1_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {}  # none required; canonical names already match

TARGET_FAMILIES: List[str] = [
    'Hantzsch Dihydropyridine Synthesis',
    'Feist-Bénary Furan Synthesis',
    'Knoevenagel Condensation',
    'Kumada Cross-Coupling',
    'Dess-Martin Oxidation',
]

# Three NEW distinct seeds (D/E/F) per family. Structurally distinct from the
# existing A/B/C to widen fingerprint coverage.
SEEDS: List[Dict[str, Any]] = [
    # === Knoevenagel Condensation (exists: benzaldehyde+diethylmalonate, ArCHO+CH2(CN)2 x2) ===
    {
        'family': 'Knoevenagel Condensation',
        'extract_kind': 'application_example',
        'transformation_text': 'Knoevenagel with aliphatic aldehyde + β-ketoester to give α,β-unsaturated β-ketoester.',
        'reactants_text': 'propanal + ethyl acetoacetate',
        'products_text': 'ethyl (2E)-2-acetyl-2-pentenoate',
        'reagents_text': 'piperidine (cat.), AcOH',
        'conditions_text': 'toluene, Dean-Stark, reflux',
        'notes_text': f'Manual curated application-class seed (variant D — aliphatic aldehyde + beta-ketoester) added during {TAG} depth sprint. [{TAG}]',
        'molecules': [
            ('reactant', 'CCC=O',              'propanal',                        'reactants_text', 1),
            ('reactant', 'CCOC(=O)CC(=O)C',    'ethyl acetoacetate',              'reactants_text', 1),
            ('product',  'CCOC(=O)C(=CCC)C(=O)C', 'ethyl (2E)-2-acetyl-2-pentenoate','products_text',  1),
        ],
    },
    {
        'family': 'Knoevenagel Condensation',
        'extract_kind': 'application_example',
        'transformation_text': 'Doebner-modified Knoevenagel: furfural + malonic acid with decarboxylation.',
        'reactants_text': 'furfural + malonic acid',
        'products_text': '(2E)-3-(furan-2-yl)acrylic acid',
        'reagents_text': 'pyridine, piperidine (cat.)',
        'conditions_text': 'reflux',
        'notes_text': f'Manual curated application-class seed (variant E — heteroaromatic aldehyde, Doebner decarboxylation) added during {TAG} depth sprint. [{TAG}]',
        'molecules': [
            ('reactant', 'O=Cc1ccco1',     'furfural',                   'reactants_text', 1),
            ('reactant', 'OC(=O)CC(=O)O',  'malonic acid',               'reactants_text', 1),
            ('product',  'OC(=O)C=Cc1ccco1', '(2E)-3-(furan-2-yl)acrylic acid', 'products_text', 1),
        ],
    },
    {
        'family': 'Knoevenagel Condensation',
        'extract_kind': 'application_example',
        'transformation_text': 'Knoevenagel of cyclohexanone (ketone) + ethyl cyanoacetate to give an alkylidene cyanoester.',
        'reactants_text': 'cyclohexanone + ethyl cyanoacetate',
        'products_text': 'ethyl 2-cyano-2-cyclohexylideneacetate',
        'reagents_text': 'piperidine (cat.)',
        'conditions_text': 'EtOH, rt',
        'notes_text': f'Manual curated application-class seed (variant F — ketone substrate) added during {TAG} depth sprint. [{TAG}]',
        'molecules': [
            ('reactant', 'O=C1CCCCC1',            'cyclohexanone',                          'reactants_text', 1),
            ('reactant', 'N#CCC(=O)OCC',          'ethyl cyanoacetate',                     'reactants_text', 1),
            ('product',  'N#CC(=C1CCCCC1)C(=O)OCC', 'ethyl 2-cyano-2-cyclohexylideneacetate', 'products_text',  1),
        ],
    },

    # === Hantzsch Dihydropyridine Synthesis ===
    {
        'family': 'Hantzsch Dihydropyridine Synthesis',
        'extract_kind': 'application_example',
        'transformation_text': 'Hantzsch 4-component: propanal + 2 eq ethyl acetoacetate + NH3 → 4-ethyl-2,6-dimethyl-3,5-diethoxycarbonyl-1,4-DHP.',
        'reactants_text': 'propanal + 2 eq ethyl acetoacetate + NH3',
        'products_text': '4-ethyl-2,6-dimethyl-3,5-bis(ethoxycarbonyl)-1,4-dihydropyridine',
        'reagents_text': 'NH4OAc (NH3 equiv)',
        'conditions_text': 'EtOH, reflux',
        'notes_text': f'Manual curated application-class seed (variant D — aliphatic aldehyde) added during {TAG} depth sprint. [{TAG}]',
        'molecules': [
            ('reactant', 'CCC=O',                                'propanal',                               'reactants_text', 1),
            ('reactant', 'CCOC(=O)CC(=O)C',                      'ethyl acetoacetate',                     'reactants_text', 1),
            ('reactant', 'N',                                    'ammonia (NH3)',                          'reactants_text', 0),  # unqueryable: fragment too small
            ('product',  'CCOC(=O)C1=C(C)NC(C)=C(C(=O)OCC)C1CC', '4-ethyl-2,6-dimethyl-3,5-bis(ethoxycarbonyl)-1,4-DHP', 'products_text', 1),
        ],
    },
    {
        'family': 'Hantzsch Dihydropyridine Synthesis',
        'extract_kind': 'application_example',
        'transformation_text': 'Hantzsch with 4-nitrobenzaldehyde (classic nifedipine-style DHP).',
        'reactants_text': '4-nitrobenzaldehyde + 2 eq methyl acetoacetate + NH3',
        'products_text': 'methyl 4-(4-nitrophenyl)-2,6-dimethyl-3,5-bis(methoxycarbonyl)-1,4-DHP',
        'reagents_text': 'NH4OAc',
        'conditions_text': 'MeOH, reflux',
        'notes_text': f'Manual curated application-class seed (variant E — nitroaryl aldehyde, nifedipine scaffold) added during {TAG} depth sprint. [{TAG}]',
        'molecules': [
            ('reactant', 'O=Cc1ccc([N+](=O)[O-])cc1',                     '4-nitrobenzaldehyde',                           'reactants_text', 1),
            ('reactant', 'COC(=O)CC(=O)C',                                'methyl acetoacetate',                           'reactants_text', 1),
            ('product',  'COC(=O)C1=C(C)NC(C)=C(C(=O)OC)C1c1ccc([N+](=O)[O-])cc1', 'nifedipine-like 1,4-DHP',          'products_text',  1),
        ],
    },
    {
        'family': 'Hantzsch Dihydropyridine Synthesis',
        'extract_kind': 'application_example',
        'transformation_text': 'Hantzsch with heteroaromatic aldehyde and cyclic 1,3-diketone (dimedone) to give acridinedione variant.',
        'reactants_text': '2-thiophenecarbaldehyde + 2 eq dimedone + NH3',
        'products_text': '9-(thiophen-2-yl)-3,3,6,6-tetramethyl-1,2,3,4,5,6,7,8,9,10-decahydroacridine-1,8-dione',
        'reagents_text': 'NH4OAc',
        'conditions_text': 'EtOH, reflux',
        'notes_text': f'Manual curated application-class seed (variant F — heteroaryl aldehyde + cyclic 1,3-diketone = acridinedione branch) added during {TAG} depth sprint. [{TAG}]',
        'molecules': [
            ('reactant', 'O=Cc1cccs1',                                               '2-thiophenecarbaldehyde',                         'reactants_text', 1),
            ('reactant', 'O=C1CC(C)(C)CC(=O)C1',                                     'dimedone',                                        'reactants_text', 1),
            ('product',  'O=C1CC(C)(C)CC2=C1NC1=C(C2c2cccs2)C(=O)CC(C)(C)C1',       'thienyl-acridinedione',                           'products_text',  1),
        ],
    },

    # === Feist-Bénary Furan Synthesis ===
    {
        'family': 'Feist-Bénary Furan Synthesis',
        'extract_kind': 'application_example',
        'transformation_text': 'Feist-Bénary: α-haloketone + β-ketoester → 3-acyl/alkoxycarbonyl-2,5-disubstituted furan.',
        'reactants_text': 'chloroacetone + ethyl acetoacetate',
        'products_text': 'ethyl 2,4-dimethyl-5-methylfuran-3-carboxylate',
        'reagents_text': 'NaOAc, NH3',
        'conditions_text': 'EtOH, 80°C',
        'notes_text': f'Manual curated application-class seed (variant D — simple alpha-chloroketone) added during {TAG} depth sprint. [{TAG}]',
        'molecules': [
            ('reactant', 'ClCC(=O)C',             'chloroacetone',                               'reactants_text', 1),
            ('reactant', 'CCOC(=O)CC(=O)C',       'ethyl acetoacetate',                          'reactants_text', 1),
            ('product',  'CCOC(=O)c1c(C)oc(C)c1C','ethyl 2,4-dimethyl-5-methylfuran-3-carboxylate','products_text',  1),
        ],
    },
    {
        'family': 'Feist-Bénary Furan Synthesis',
        'extract_kind': 'application_example',
        'transformation_text': 'Feist-Bénary with phenacyl bromide to install aryl at furan-2.',
        'reactants_text': 'phenacyl bromide + ethyl acetoacetate',
        'products_text': 'ethyl 5-methyl-2-phenylfuran-3-carboxylate',
        'reagents_text': 'pyridine',
        'conditions_text': 'EtOH, reflux',
        'notes_text': f'Manual curated application-class seed (variant E — aryl alpha-bromoketone) added during {TAG} depth sprint. [{TAG}]',
        'molecules': [
            ('reactant', 'BrCC(=O)c1ccccc1',          'phenacyl bromide',                        'reactants_text', 1),
            ('reactant', 'CCOC(=O)CC(=O)C',           'ethyl acetoacetate',                      'reactants_text', 1),
            ('product',  'CCOC(=O)c1cc(c2ccccc2)oc1C','ethyl 5-methyl-2-phenylfuran-3-carboxylate','products_text',  1),
        ],
    },
    {
        'family': 'Feist-Bénary Furan Synthesis',
        'extract_kind': 'application_example',
        'transformation_text': 'Feist-Bénary with β-diketone partner (acetylacetone) instead of β-ketoester.',
        'reactants_text': 'chloroacetone + acetylacetone',
        'products_text': '1-(2,4,5-trimethylfuran-3-yl)ethan-1-one',
        'reagents_text': 'K2CO3',
        'conditions_text': 'acetone, 60°C',
        'notes_text': f'Manual curated application-class seed (variant F — 1,3-diketone partner instead of beta-ketoester) added during {TAG} depth sprint. [{TAG}]',
        'molecules': [
            ('reactant', 'ClCC(=O)C',           'chloroacetone',                            'reactants_text', 1),
            ('reactant', 'CC(=O)CC(=O)C',       'acetylacetone',                            'reactants_text', 1),
            ('product',  'CC(=O)c1c(C)oc(C)c1C','1-(2,4,5-trimethylfuran-3-yl)ethan-1-one', 'products_text',  1),
        ],
    },

    # === Kumada Cross-Coupling ===
    {
        'family': 'Kumada Cross-Coupling',
        'extract_kind': 'application_example',
        'transformation_text': 'Kumada: aryl halide + alkyl/aryl Grignard under Ni or Pd catalysis.',
        'reactants_text': '4-bromotoluene + n-butylmagnesium bromide',
        'products_text': '4-butyltoluene',
        'reagents_text': 'NiCl2(dppp) (cat.)',
        'conditions_text': 'Et2O, rt',
        'notes_text': f'Manual curated application-class seed (variant D — aryl bromide + alkyl Grignard, Ni-catalyzed) added during {TAG} depth sprint. [{TAG}]',
        'molecules': [
            ('reactant', 'Cc1ccc(Br)cc1',     '4-bromotoluene',                'reactants_text', 1),
            ('reactant', 'CCCC[Mg]Br',        'n-butylmagnesium bromide',      'reactants_text', 0),  # organomagnesium — queryable=0 for safety
            ('product',  'CCCCc1ccc(C)cc1',   '4-butyltoluene',                'products_text',  1),
        ],
    },
    {
        'family': 'Kumada Cross-Coupling',
        'extract_kind': 'application_example',
        'transformation_text': 'Kumada biaryl coupling.',
        'reactants_text': 'chlorobenzene + 4-methoxyphenylmagnesium bromide',
        'products_text': '4-methoxybiphenyl',
        'reagents_text': 'Pd(dba)2 / SIPr',
        'conditions_text': 'THF, rt',
        'notes_text': f'Manual curated application-class seed (variant E — aryl chloride + aryl Grignard biaryl) added during {TAG} depth sprint. [{TAG}]',
        'molecules': [
            ('reactant', 'Clc1ccccc1',        'chlorobenzene',                'reactants_text', 1),
            ('reactant', 'COc1ccc([Mg]Br)cc1','4-methoxyphenylmagnesium bromide','reactants_text', 0),
            ('product',  'COc1ccc(-c2ccccc2)cc1', '4-methoxybiphenyl',        'products_text',  1),
        ],
    },
    {
        'family': 'Kumada Cross-Coupling',
        'extract_kind': 'application_example',
        'transformation_text': 'Kumada on vinyl halide (alkenyl-Kumada) with MeMgBr.',
        'reactants_text': 'β-bromostyrene + methylmagnesium bromide',
        'products_text': 'β-methylstyrene (1-phenyl-1-propene)',
        'reagents_text': 'Pd(PPh3)4',
        'conditions_text': 'THF, rt',
        'notes_text': f'Manual curated application-class seed (variant F — vinyl halide substrate) added during {TAG} depth sprint. [{TAG}]',
        'molecules': [
            ('reactant', 'BrC=Cc1ccccc1',    '(E)-β-bromostyrene',              'reactants_text', 1),
            ('reactant', 'C[Mg]Br',            'methylmagnesium bromide',         'reactants_text', 0),
            ('product',  'CC=Cc1ccccc1',     '(E)-β-methylstyrene',             'products_text',  1),
        ],
    },

    # === Dess-Martin Oxidation ===
    {
        'family': 'Dess-Martin Oxidation',
        'extract_kind': 'application_example',
        'transformation_text': 'Dess-Martin (DMP) oxidation of 1° alcohol to aldehyde; mild, no over-oxidation.',
        'reactants_text': '1-hexanol',
        'products_text': 'hexanal',
        'reagents_text': 'Dess-Martin periodinane (DMP), NaHCO3',
        'conditions_text': 'DCM, 0°C→rt',
        'notes_text': f'Manual curated application-class seed (variant D — primary aliphatic alcohol) added during {TAG} depth sprint. [{TAG}]',
        'molecules': [
            ('reactant', 'OCCCCCC',   '1-hexanol',  'reactants_text', 1),
            ('product',  'O=CCCCCC',  'hexanal',    'products_text',  1),
        ],
    },
    {
        'family': 'Dess-Martin Oxidation',
        'extract_kind': 'application_example',
        'transformation_text': 'DMP oxidation of 2° benzylic alcohol to ketone.',
        'reactants_text': '1-phenylethanol',
        'products_text': 'acetophenone',
        'reagents_text': 'DMP, pyridine',
        'conditions_text': 'DCM, rt',
        'notes_text': f'Manual curated application-class seed (variant E — secondary benzylic alcohol) added during {TAG} depth sprint. [{TAG}]',
        'molecules': [
            ('reactant', 'OC(c1ccccc1)C',  '1-phenylethanol',  'reactants_text', 1),
            ('product',  'O=C(c1ccccc1)C', 'acetophenone',     'products_text',  1),
        ],
    },
    {
        'family': 'Dess-Martin Oxidation',
        'extract_kind': 'application_example',
        'transformation_text': 'DMP oxidation of allylic alcohol to enone (preserves alkene).',
        'reactants_text': '(E)-2-hexen-1-ol',
        'products_text': '(E)-2-hexenal',
        'reagents_text': 'DMP, NaHCO3',
        'conditions_text': 'DCM, 0°C',
        'notes_text': f'Manual curated application-class seed (variant F — allylic alcohol, preserves alkene) added during {TAG} depth sprint. [{TAG}]',
        'molecules': [
            ('reactant', 'OC/C=C/CCC',   '(E)-2-hexen-1-ol',  'reactants_text', 1),
            ('product',  'O=C/C=C/CCC',  '(E)-2-hexenal',     'products_text',  1),
        ],
    },
]


def family_norm(name: str) -> str:
    txt = name.lower()
    for ch in "(),'/-":
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


def preflight_smiles_guard(seeds: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    failures: List[Dict[str, Any]] = []
    for seed in seeds:
        for role, smiles, name, _field, queryable in seed['molecules']:
            # Skip queryable=0 items; they go into the DB but aren't fingerprinted.
            if queryable != 1:
                continue
            ok, reasons = is_smiles_safe(smiles)
            if not ok:
                failures.append({
                    'family': seed['family'],
                    'role': role,
                    'name': name,
                    'smiles': smiles,
                    'reasons': reasons,
                })
    return failures


def queryable_family_count(conn: sqlite3.Connection) -> int:
    return conn.execute('SELECT COUNT(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1').fetchone()[0]


def queryable_count(conn: sqlite3.Connection) -> int:
    return conn.execute('SELECT COUNT(*) FROM extract_molecules WHERE queryable=1').fetchone()[0]


def find_anchor(conn: sqlite3.Connection, seed: Dict[str, Any]) -> Tuple[int, int, str]:
    row = conn.execute("""
        SELECT re.scheme_candidate_id, COALESCE(pi.page_no, 0) AS page_no, COALESCE(pi.image_filename, ?) AS image_filename
        FROM reaction_extracts re
        LEFT JOIN scheme_candidates sc ON sc.id = re.scheme_candidate_id
        LEFT JOIN page_images pi ON pi.id = sc.page_image_id
        WHERE re.reaction_family_name=? AND re.scheme_candidate_id IS NOT NULL
        ORDER BY CASE WHEN re.extract_kind='canonical_overview' THEN 0 WHEN re.extract_kind='application_example' THEN 1 ELSE 2 END, re.id
        LIMIT 1
    """, (seed.get('anchor_image', 'phase3d_anchor.jpg'), seed['family'])).fetchone()
    if row and row[0] is not None:
        return int(row[0]), int(row[1] or 0), row[2]
    raise RuntimeError(f"Could not find usable anchor for {seed['family']}")


def existing_seed_extract(conn: sqlite3.Connection, family: str, notes_text: str) -> Optional[int]:
    row = conn.execute('SELECT id FROM reaction_extracts WHERE reaction_family_name=? AND notes_text=? LIMIT 1', (family, notes_text)).fetchone()
    return row[0] if row else None


def insert_seed(conn: sqlite3.Connection, seed: Dict[str, Any]) -> Dict[str, Any]:
    fam = seed['family']
    fam_norm = family_norm(fam)
    existing = existing_seed_extract(conn, fam, seed['notes_text'])
    anchor_id, page_no, image_filename = find_anchor(conn, seed)
    if existing:
        return {'family': fam, 'status': 'skipped_existing', 'extract_id': existing, 'page_no': page_no, 'image_filename': image_filename, 'inserted_molecules': 0}
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
    cur = conn.execute("""
        INSERT INTO reaction_extracts (
            scheme_candidate_id, reaction_family_name, reaction_family_name_norm, extract_kind,
            transformation_text, reactants_text, products_text, intermediates_text, reagents_text,
            conditions_text, notes_text, reactant_smiles, product_smiles,
            smiles_confidence, extraction_confidence, parse_status, promote_decision,
            extractor_model, extractor_prompt_version, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        anchor_id, fam, fam_norm, seed['extract_kind'], seed.get('transformation_text'),
        seed.get('reactants_text'), seed.get('products_text'), seed.get('intermediates_text'),
        seed.get('reagents_text'), seed.get('conditions_text'), seed.get('notes_text'),
        reactant_smiles, product_smiles, 0.98 if (reactant_smiles or product_smiles) else 0.0,
        0.98, 'curated', 'promote', 'manual_curated_seed', TAG, NOW, NOW,
    ))
    extract_id = cur.lastrowid
    inserted = 0
    for role, smiles, name, source_field, queryable in seed['molecules']:
        csmiles = canon(smiles) if smiles else None
        conn.execute("""
            INSERT INTO extract_molecules (
                extract_id, role, smiles, smiles_kind, quality_tier, reaction_family_name,
                source_zip, page_no, queryable, note_text, morgan_fp, normalized_text,
                source_field, structure_source, fg_tags, role_confidence, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            extract_id, role, csmiles, 'canonical', 1 if queryable and csmiles else 3, fam,
            'named reactions.pdf', page_no, 1 if queryable and csmiles else 0,
            f'[{TAG}: {name}]' if name else f'[{TAG}]', fp_blob(csmiles) if (queryable and csmiles) else None,
            (name.lower() if name else None), source_field, TAG if csmiles else None, None,
            0.98 if csmiles else 0.0, NOW,
        ))
        inserted += 1
    return {'family': fam, 'status': 'inserted', 'extract_id': extract_id, 'page_no': page_no, 'image_filename': image_filename, 'inserted_molecules': inserted}


def verify_family(conn: sqlite3.Connection, family: str) -> Dict[str, Any]:
    row = conn.execute("""
        SELECT COUNT(*) AS extract_count,
               SUM(CASE WHEN extract_kind='canonical_overview'  THEN 1 ELSE 0 END) AS overview_count,
               SUM(CASE WHEN extract_kind='application_example' THEN 1 ELSE 0 END) AS application_count
        FROM reaction_extracts WHERE reaction_family_name=?
    """, (family,)).fetchone()
    mol = conn.execute("""
        SELECT SUM(CASE WHEN role='reactant' AND queryable=1 AND smiles IS NOT NULL AND smiles<>'' THEN 1 ELSE 0 END) AS queryable_reactants,
               SUM(CASE WHEN role='product'  AND queryable=1 AND smiles IS NOT NULL AND smiles<>'' THEN 1 ELSE 0 END) AS queryable_products,
               COUNT(*) AS molecule_rows
        FROM extract_molecules WHERE reaction_family_name=?
    """, (family,)).fetchone()
    pair_count = conn.execute("""
        SELECT COUNT(DISTINCT COALESCE(reactant_smiles,'') || ' || ' || COALESCE(product_smiles,''))
        FROM reaction_extracts
        WHERE reaction_family_name=?
          AND COALESCE(reactant_smiles,'')<>''
          AND COALESCE(product_smiles,'')<>''
    """, (family,)).fetchone()[0]
    return {
        'family': family,
        **dict(row),
        **dict(mol),
        'unique_queryable_pair_count': int(pair_count or 0),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default='app/labint.db')
    ap.add_argument('--report-dir', default='reports/family_completion_phase3d_tier1')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    db_path = Path(args.db)
    report_dir = Path(args.report_dir) / dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    report_dir.mkdir(parents=True, exist_ok=True)

    guard_failures = preflight_smiles_guard(SEEDS)
    if guard_failures:
        print(f'[phase3d] smiles_guard REJECTED {len(guard_failures)} molecule(s) before DB touch:')
        for f in guard_failures:
            print(f'  - {f["family"]} [{f["role"]}] {f["smiles"]!r} reasons={f["reasons"]}')
        (report_dir / 'smiles_guard_failures.json').write_text(
            json.dumps(guard_failures, indent=2, ensure_ascii=False),
            encoding='utf-8',
        )
        return 2

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        before_families = queryable_family_count(conn)
        before_queryable = queryable_count(conn)

        results = [insert_seed(conn, seed) for seed in SEEDS]

        if args.dry_run:
            conn.rollback()
            print('[phase3d] DRY RUN — rolled back')
        else:
            conn.commit()

        verify = [verify_family(conn, fam) for fam in TARGET_FAMILIES]
        after_families = queryable_family_count(conn)
        after_queryable = queryable_count(conn)

        report = {
            'phase_tag': TAG,
            'dry_run': args.dry_run,
            'seeds_submitted': len(SEEDS),
            'target_families': TARGET_FAMILIES,
            'before_queryable_families': before_families,
            'after_queryable_families': after_families,
            'before_queryable_rows': before_queryable,
            'after_queryable_rows': after_queryable,
            'per_seed_results': results,
            'per_family_verify': verify,
        }
        (report_dir / 'phase3d_tier1_report.json').write_text(
            json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8',
        )
        print(f'[phase3d] report: {report_dir / "phase3d_tier1_report.json"}')
        print(f'  queryable rows: {before_queryable} -> {after_queryable} (+{after_queryable - before_queryable})')
        for v in verify:
            print(f'  {v["family"]}: ov={v["overview_count"]} ap={v["application_count"]} qR={v["queryable_reactants"]} qP={v["queryable_products"]} pairs={v["unique_queryable_pair_count"]}')
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    sys.exit(main())
