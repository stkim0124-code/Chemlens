"""phase16 apply script — draft (to be placed in phase_queue/inbox by the
generator). Completes 10 shallow families in a 5+5 split via the same
pattern as phase15. Key additions vs phase15:

1. Pre-validates every seed SMILES via smiles_guard.is_smiles_safe before
   touching the DB. If any pre-flight check fails, the script exits with a
   non-zero code BEFORE any SQL write, so the daemon records a FAIL and
   Claude can emit a hotfix.
2. Same anchor / backfill / alias / verify / summary pattern as phase15.
3. Commits on success, rolls back on any exception.

Expected invocation by rdkit_daemon.py:
    python complete_family_evidence_phase16_shallow_top10.py \
        --db app/labint.db \
        --report-dir reports/family_completion_phase16_shallow_top10
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

# --- hotfix3: ensure backend_dir is on sys.path so smiles_guard imports ---
import os as _os
import sys as _sys
from pathlib import Path as _Path
_HERE = _Path(__file__).resolve().parent
# If running from phase_queue/processing/, backend_dir is two levels up.
# Walk upward until we find smiles_guard.py (cap at 4 hops).
_cand = _HERE
for _ in range(4):
    if (_cand / "smiles_guard.py").exists():
        if str(_cand) not in _sys.path:
            _sys.path.insert(0, str(_cand))
        break
    _cand = _cand.parent
# also try PYTHONPATH if daemon provided it
for _p in _os.environ.get("PYTHONPATH", "").split(_os.pathsep):
    if _p and _p not in _sys.path:
        _sys.path.insert(0, _p)
# -------------------------------------------------------------------------
# smiles_guard lives next to this script (chemlens/backend/)
try:
    from smiles_guard import is_smiles_safe
except Exception as e:
    print(f'[phase16] ERROR: could not import smiles_guard: {e}')
    raise

TAG = 'phase18_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Kulinkovich Reaction',
        'Kulinovich Reaction',
        'Larock Indole Synthesis',
        'Madelung Indole Synthesis',
        'Malonic Ester Synthesis',
    ],
    'b': [
        'McMurry Coupling',
        'Meerwein Arylation',
        'Meerwein-Ponndorf-Verley Reduction',
        'Meisenheimer Rearrangement',
        'Meyer-Schuster and Rupe Rearrangement',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Kulinkovich Reaction ===
        {
            'family': 'Kulinkovich Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Kulinkovich reaction: ethyl ester converted to cyclopropanol using EtMgBr and catalytic Ti(OiPr)4 via titanacyclopropane intermediate.',
            'reactants_text': 'ethyl pentanoate',
            'products_text': '1-butyl-cyclopropanol',
            'reagents_text': 'Ti(OiPr)4 (cat.), EtMgBr',
            'conditions_text': 'Et2O, rt, slow addition',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase18_shallow_top10_v1 sprint for Kulinkovich Reaction. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)CCCC', 'ethyl pentanoate', 'reactants_text', 1),
                ('product', 'OC1(CCCC)CC1', '1-butyl-cyclopropanol', 'products_text', 1),
            ],
        },
        {
            'family': 'Kulinkovich Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Kulinkovich cyclopropanation of aromatic ester gives 1-aryl cyclopropanol.',
            'reactants_text': 'methyl benzoate',
            'products_text': '1-phenyl-cyclopropanol',
            'reagents_text': 'Ti(OiPr)4, EtMgBr (3 equiv)',
            'conditions_text': 'Et2O, 0°C→rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase18_shallow_top10_v1 sprint for Kulinkovich Reaction. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'COC(=O)c1ccccc1', 'methyl benzoate', 'reactants_text', 1),
                ('product', 'OC1(c2ccccc2)CC1', '1-phenyl-cyclopropanol', 'products_text', 1),
            ],
        },
        {
            'family': 'Kulinkovich Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Kulinkovich selective mono-cyclopropanation of malonate ester.',
            'reactants_text': 'diethyl malonate',
            'products_text': 'ethyl 2-(1-hydroxycyclopropyl)acetate',
            'reagents_text': 'Ti(OiPr)4 (20 mol%), EtMgBr',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase18_shallow_top10_v1 sprint for Kulinkovich Reaction. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)CC(=O)OCC', 'diethyl malonate', 'reactants_text', 1),
                ('product', 'OC1(CC(=O)OCC)CC1', 'ethyl 2-(1-hydroxycyclopropyl)acetate', 'products_text', 1),
            ],
        },
        # === Kulinovich Reaction ===
        {
            'family': 'Kulinovich Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Kulinovich (Kulinkovich-alias) cyclopropanation of ethyl acetate with EtMgBr/Ti.',
            'reactants_text': 'ethyl acetate',
            'products_text': '1-methyl-cyclopropanol',
            'reagents_text': 'Ti(OiPr)4 (cat.), EtMgBr',
            'conditions_text': 'Et2O, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase18_shallow_top10_v1 sprint for Kulinovich Reaction. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)C', 'ethyl acetate', 'reactants_text', 1),
                ('product', 'OC1(C)CC1', '1-methyl-cyclopropanol', 'products_text', 1),
            ],
        },
        {
            'family': 'Kulinovich Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Kulinovich cyclopropanol synthesis from α-branched ester.',
            'reactants_text': 'ethyl isobutyrate',
            'products_text': '1-isopropyl-cyclopropanol',
            'reagents_text': 'Ti(OiPr)4 / EtMgBr',
            'conditions_text': 'THF, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase18_shallow_top10_v1 sprint for Kulinovich Reaction. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)C(C)C', 'ethyl isobutyrate', 'reactants_text', 1),
                ('product', 'OC1(C(C)C)CC1', '1-isopropyl-cyclopropanol', 'products_text', 1),
            ],
        },
        {
            'family': 'Kulinovich Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Kulinovich cyclopropanation delivering 1-ethylcyclopropanol.',
            'reactants_text': 'ethyl propanoate',
            'products_text': '1-ethyl-cyclopropanol',
            'reagents_text': 'Ti(OiPr)4, EtMgBr (2.5 equiv)',
            'conditions_text': 'Et2O, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase18_shallow_top10_v1 sprint for Kulinovich Reaction. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)CC', 'ethyl propanoate', 'reactants_text', 1),
                ('product', 'OC1(CC)CC1', '1-ethyl-cyclopropanol', 'products_text', 1),
            ],
        },
        # === Larock Indole Synthesis ===
        {
            'family': 'Larock Indole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Larock indole synthesis: o-haloaniline + internal alkyne with Pd catalysis gives 2,3-disubstituted indole.',
            'reactants_text': '2-iodoaniline',
            'products_text': '3-phenylindole (from diphenylacetylene)',
            'reagents_text': 'diphenylacetylene, Pd(OAc)2, LiCl, Na2CO3, LiOAc',
            'conditions_text': 'DMF, 100°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase18_shallow_top10_v1 sprint for Larock Indole Synthesis. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Nc1ccccc1I', '2-iodoaniline', 'reactants_text', 1),
                ('product', 'c1ccc2[nH]cc(-c3ccccc3)c2c1', '3-phenylindole (from diphenylacetylene)', 'products_text', 1),
            ],
        },
        {
            'family': 'Larock Indole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Larock indole: o-iodoaniline + 2-butyne gives 2,3-dimethylindole.',
            'reactants_text': '2-iodoaniline',
            'products_text': '2,3-dimethylindole',
            'reagents_text': '2-butyne, Pd(OAc)2, n-Bu4NCl, Na2CO3',
            'conditions_text': 'DMF, 100°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase18_shallow_top10_v1 sprint for Larock Indole Synthesis. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Nc1ccccc1I', '2-iodoaniline', 'reactants_text', 1),
                ('product', 'Cc1[nH]c2ccccc2c1C', '2,3-dimethylindole', 'products_text', 1),
            ],
        },
        {
            'family': 'Larock Indole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Larock heteroannulation of substituted iodoaniline with propenylbenzene.',
            'reactants_text': '2-iodo-4-methylaniline',
            'products_text': '5-methyl-2-phenyl-3-methylindole',
            'reagents_text': '1-phenyl-1-propyne, Pd(OAc)2, LiCl, K2CO3',
            'conditions_text': 'DMF, 100°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase18_shallow_top10_v1 sprint for Larock Indole Synthesis. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Nc1ccc(C)cc1I', '2-iodo-4-methylaniline', 'reactants_text', 1),
                ('product', 'Cc1ccc2[nH]c(-c3ccccc3)c(C)c2c1', '5-methyl-2-phenyl-3-methylindole', 'products_text', 1),
            ],
        },
        # === Madelung Indole Synthesis ===
        {
            'family': 'Madelung Indole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Madelung indole synthesis: N-acyl o-toluidine undergoes strong-base induced cyclodehydration to 2-substituted indole.',
            'reactants_text': 'N-acetyl-o-toluidine',
            'products_text': '2-methylindole',
            'reagents_text': 'NaOEt or NaNH2, high temp',
            'conditions_text': 'sealed tube, 360°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase18_shallow_top10_v1 sprint for Madelung Indole Synthesis. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(=O)Nc1ccccc1C', 'N-acetyl-o-toluidine', 'reactants_text', 1),
                ('product', 'Cc1cc2ccccc2[nH]1', '2-methylindole', 'products_text', 1),
            ],
        },
        {
            'family': 'Madelung Indole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Madelung synthesis of 2-phenylindole from N-benzoyl-o-toluidine.',
            'reactants_text': 'N-benzoyl-o-toluidine',
            'products_text': '2-phenylindole',
            'reagents_text': 'NaNH2, KOH',
            'conditions_text': '250–300°C, neat',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase18_shallow_top10_v1 sprint for Madelung Indole Synthesis. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(c1ccccc1)Nc2ccccc2C', 'N-benzoyl-o-toluidine', 'reactants_text', 1),
                ('product', 'c1ccc(-c2cc3ccccc3[nH]2)cc1', '2-phenylindole', 'products_text', 1),
            ],
        },
        {
            'family': 'Madelung Indole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Madelung cyclization giving 2-ethylindole.',
            'reactants_text': 'N-propanoyl-o-toluidine',
            'products_text': '2-ethylindole',
            'reagents_text': 't-BuOK',
            'conditions_text': '300°C, sealed vessel',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase18_shallow_top10_v1 sprint for Madelung Indole Synthesis. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCC(=O)Nc1ccccc1C', 'N-propanoyl-o-toluidine', 'reactants_text', 1),
                ('product', 'CCc1cc2ccccc2[nH]1', '2-ethylindole', 'products_text', 1),
            ],
        },
        # === Malonic Ester Synthesis ===
        {
            'family': 'Malonic Ester Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Malonic ester synthesis: mono-alkylation of diethyl malonate with n-butyl bromide, then saponification and decarboxylation to hexanoic acid.',
            'reactants_text': 'diethyl malonate',
            'products_text': 'hexanoic acid',
            'reagents_text': 'NaOEt, n-BuBr; then aq. KOH; H3O+/Δ',
            'conditions_text': 'EtOH reflux, then hydrolysis/decarb',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase18_shallow_top10_v1 sprint for Malonic Ester Synthesis. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)CC(=O)OCC', 'diethyl malonate', 'reactants_text', 1),
                ('product', 'CCCCCC(=O)O', 'hexanoic acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Malonic Ester Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Malonic ester synthesis: alkylation with n-hexyl bromide then hydrolysis/decarboxylation to octanoic acid.',
            'reactants_text': 'diethyl malonate',
            'products_text': 'octanoic acid',
            'reagents_text': 'NaOEt, n-hexyl bromide; then aq. NaOH, H+/heat',
            'conditions_text': 'EtOH, then H2O reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase18_shallow_top10_v1 sprint for Malonic Ester Synthesis. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)CC(=O)OCC', 'diethyl malonate', 'reactants_text', 1),
                ('product', 'CCCCCCCC(=O)O', 'octanoic acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Malonic Ester Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Malonic ester di-alkylation: sequential ethyl + n-butyl, then saponification and decarboxylation to 2-ethylhexanoic acid.',
            'reactants_text': 'diethyl malonate',
            'products_text': '2-ethylhexanoic acid',
            'reagents_text': 'NaOEt, EtBr; NaOEt, n-BuBr; aq. KOH, H+/Δ',
            'conditions_text': 'EtOH, stepwise',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase18_shallow_top10_v1 sprint for Malonic Ester Synthesis. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)CC(=O)OCC', 'diethyl malonate', 'reactants_text', 1),
                ('product', 'CCCCC(CC)CC(=O)O', '2-ethylhexanoic acid', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === McMurry Coupling ===
        {
            'family': 'McMurry Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'McMurry coupling: reductive carbonyl coupling of two acetophenone molecules gives tetrasubstituted alkene.',
            'reactants_text': 'acetophenone',
            'products_text': '(E)-2,3-diphenyl-2-butene',
            'reagents_text': 'TiCl3/Zn or TiCl4/Zn',
            'conditions_text': 'DME reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase18_shallow_top10_v1 sprint for McMurry Coupling. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)c1ccccc1', 'acetophenone', 'reactants_text', 1),
                ('product', 'CC(=C(C)c1ccccc1)c2ccccc2', '(E)-2,3-diphenyl-2-butene', 'products_text', 1),
            ],
        },
        {
            'family': 'McMurry Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'McMurry coupling of benzaldehyde dimer to (E)-stilbene via low-valent titanium.',
            'reactants_text': 'benzaldehyde',
            'products_text': 'stilbene',
            'reagents_text': 'TiCl3/LiAlH4',
            'conditions_text': 'THF reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase18_shallow_top10_v1 sprint for McMurry Coupling. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1),
                ('product', 'C(=Cc1ccccc1)c2ccccc2', 'stilbene', 'products_text', 1),
            ],
        },
        {
            'family': 'McMurry Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Intermolecular McMurry coupling of two cyclohexanones to bicyclohexylidene.',
            'reactants_text': 'cyclohexanone',
            'products_text': 'bicyclohexylidene',
            'reagents_text': 'TiCl4, Zn',
            'conditions_text': 'DME/pyridine, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase18_shallow_top10_v1 sprint for McMurry Coupling. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCCC1', 'cyclohexanone', 'reactants_text', 1),
                ('product', 'O=C2CCCCC2=C1CCCCC1', 'bicyclohexylidene', 'products_text', 1),
            ],
        },
        # === Meerwein Arylation ===
        {
            'family': 'Meerwein Arylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Meerwein arylation: aryl radical from diazonium adds to activated alkene with chloride transfer.',
            'reactants_text': 'benzenediazonium chloride + methyl acrylate',
            'products_text': 'methyl 3-chloro-3-phenylpropanoate',
            'reagents_text': 'CuCl2 (cat.), NaOAc',
            'conditions_text': 'acetone/water, 0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase18_shallow_top10_v1 sprint for Meerwein Arylation. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', '[N+]#Nc1ccccc1.[Cl-].C=CC(=O)OC', 'benzenediazonium chloride + methyl acrylate', 'reactants_text', 1),
                ('product', 'COC(=O)CC(Cl)c1ccccc1', 'methyl 3-chloro-3-phenylpropanoate', 'products_text', 1),
            ],
        },
        {
            'family': 'Meerwein Arylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Meerwein arylation of acrylonitrile with p-chloroaryl diazonium salt.',
            'reactants_text': '4-chlorobenzenediazonium + acrylonitrile',
            'products_text': '3-chloro-3-(4-chlorophenyl)propanenitrile',
            'reagents_text': 'CuCl2, NaOAc',
            'conditions_text': 'H2O/acetone, 0–5°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase18_shallow_top10_v1 sprint for Meerwein Arylation. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', '[N+]#Nc1ccc(Cl)cc1.[Cl-].C=CC#N', '4-chlorobenzenediazonium + acrylonitrile', 'reactants_text', 1),
                ('product', 'N#CC(Cl)Cc1ccc(Cl)cc1', '3-chloro-3-(4-chlorophenyl)propanenitrile', 'products_text', 1),
            ],
        },
        {
            'family': 'Meerwein Arylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Meerwein arylation: aryl radical adds to enone with chloride quench.',
            'reactants_text': 'p-methoxybenzenediazonium + phenyl vinyl ketone',
            'products_text': '1-(4-methoxyphenyl)-3-chloro-3-benzoylpropane (β-chloro ketone)',
            'reagents_text': 'CuCl2, NaOAc',
            'conditions_text': 'acetone/H2O, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase18_shallow_top10_v1 sprint for Meerwein Arylation. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', '[N+]#Nc1ccc(OC)cc1.[Cl-].C=CC(=O)c2ccccc2', 'p-methoxybenzenediazonium + phenyl vinyl ketone', 'reactants_text', 1),
                ('product', 'COc1ccc(CC(Cl)C(=O)c2ccccc2)cc1', '1-(4-methoxyphenyl)-3-chloro-3-benzoylpropane (β-chloro ketone)', 'products_text', 1),
            ],
        },
        # === Meerwein-Ponndorf-Verley Reduction ===
        {
            'family': 'Meerwein-Ponndorf-Verley Reduction',
            'extract_kind': 'application_example',
            'transformation_text': 'MPV reduction: aluminum isopropoxide transfers hydride from isopropanol to ketone giving secondary alcohol.',
            'reactants_text': 'acetophenone',
            'products_text': '1-phenylethanol',
            'reagents_text': 'Al(OiPr)3, isopropanol (excess)',
            'conditions_text': 'iPrOH, reflux, distillation of acetone byproduct',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase18_shallow_top10_v1 sprint for Meerwein-Ponndorf-Verley Reduction. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)c1ccccc1', 'acetophenone', 'reactants_text', 1),
                ('product', 'OC(C)c1ccccc1', '1-phenylethanol', 'products_text', 1),
            ],
        },
        {
            'family': 'Meerwein-Ponndorf-Verley Reduction',
            'extract_kind': 'application_example',
            'transformation_text': 'MPV reduction of cyclohexanone to cyclohexanol using aluminum isopropoxide.',
            'reactants_text': 'cyclohexanone',
            'products_text': 'cyclohexanol',
            'reagents_text': 'Al(OiPr)3, iPrOH',
            'conditions_text': 'reflux, removing acetone',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase18_shallow_top10_v1 sprint for Meerwein-Ponndorf-Verley Reduction. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCCC1', 'cyclohexanone', 'reactants_text', 1),
                ('product', 'OC1CCCCC1', 'cyclohexanol', 'products_text', 1),
            ],
        },
        {
            'family': 'Meerwein-Ponndorf-Verley Reduction',
            'extract_kind': 'application_example',
            'transformation_text': 'MPV reduction delivering aliphatic secondary alcohol.',
            'reactants_text': '3-pentanone',
            'products_text': '3-pentanol',
            'reagents_text': 'Al(OiPr)3, iPrOH',
            'conditions_text': 'iPrOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase18_shallow_top10_v1 sprint for Meerwein-Ponndorf-Verley Reduction. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCC(=O)CC', '3-pentanone', 'reactants_text', 1),
                ('product', 'CCC(O)CC', '3-pentanol', 'products_text', 1),
            ],
        },
        # === Meisenheimer Rearrangement ===
        {
            'family': 'Meisenheimer Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Meisenheimer [2,3]-rearrangement: allylic/benzyl amine N-oxide migrates C→O via concerted sigmatropic shift.',
            'reactants_text': 'N,N-dimethyl-N-benzylamine N-oxide',
            'products_text': 'O-benzyl-N,N-dimethylhydroxylamine',
            'reagents_text': 'thermal',
            'conditions_text': 'benzene, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase18_shallow_top10_v1 sprint for Meisenheimer Rearrangement. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C[N+](C)(Cc1ccccc1)[O-]', 'N,N-dimethyl-N-benzylamine N-oxide', 'reactants_text', 1),
                ('product', 'CN(C)OCc1ccccc1', 'O-benzyl-N,N-dimethylhydroxylamine', 'products_text', 1),
            ],
        },
        {
            'family': 'Meisenheimer Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Meisenheimer rearrangement of allyl amine N-oxide via [2,3]-sigmatropic shift.',
            'reactants_text': 'N,N-dimethylallylamine N-oxide',
            'products_text': 'O-allyl-N,N-dimethylhydroxylamine',
            'reagents_text': 'thermal',
            'conditions_text': 'neat, 80°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase18_shallow_top10_v1 sprint for Meisenheimer Rearrangement. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C[N+](C)(CC=C)[O-]', 'N,N-dimethylallylamine N-oxide', 'reactants_text', 1),
                ('product', 'C=CCON(C)C', 'O-allyl-N,N-dimethylhydroxylamine', 'products_text', 1),
            ],
        },
        {
            'family': 'Meisenheimer Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Meisenheimer rearrangement giving diethyl hydroxylamine O-benzyl ether.',
            'reactants_text': 'N,N-diethyl-N-benzylamine N-oxide',
            'products_text': 'O-benzyl-N,N-diethylhydroxylamine',
            'reagents_text': 'thermal',
            'conditions_text': 'toluene, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase18_shallow_top10_v1 sprint for Meisenheimer Rearrangement. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC[N+](CC)(Cc1ccccc1)[O-]', 'N,N-diethyl-N-benzylamine N-oxide', 'reactants_text', 1),
                ('product', 'CCN(CC)OCc1ccccc1', 'O-benzyl-N,N-diethylhydroxylamine', 'products_text', 1),
            ],
        },
        # === Meyer-Schuster and Rupe Rearrangement ===
        {
            'family': 'Meyer-Schuster and Rupe Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Meyer-Schuster/Rupe: propargylic tertiary alcohol rearranges to α,β-unsaturated carbonyl; with this substrate Rupe pathway gives senecioaldehyde.',
            'reactants_text': '2-methyl-3-butyn-2-ol',
            'products_text': '3-methyl-2-butenal (senecioaldehyde)',
            'reagents_text': 'H2SO4 (cat.) or Au(I)',
            'conditions_text': 'aqueous acid, 60°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase18_shallow_top10_v1 sprint for Meyer-Schuster and Rupe Rearrangement. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)(C)C#C', '2-methyl-3-butyn-2-ol', 'reactants_text', 1),
                ('product', 'CC(C)=CC=O', '3-methyl-2-butenal (senecioaldehyde)', 'products_text', 1),
            ],
        },
        {
            'family': 'Meyer-Schuster and Rupe Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Meyer-Schuster rearrangement of propargyl alcohol to α,β-unsaturated aldehyde.',
            'reactants_text': '1-phenyl-2-propyn-1-ol',
            'products_text': 'cinnamaldehyde',
            'reagents_text': 'H2SO4 or V2O5 (cat.)',
            'conditions_text': 'aqueous acid, Δ',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase18_shallow_top10_v1 sprint for Meyer-Schuster and Rupe Rearrangement. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C#C)c1ccccc1', '1-phenyl-2-propyn-1-ol', 'reactants_text', 1),
                ('product', 'O=CCC(=Cc1ccccc1)', 'cinnamaldehyde', 'products_text', 1),
            ],
        },
        {
            'family': 'Meyer-Schuster and Rupe Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Rupe rearrangement of α,α-disubstituted propargyl alcohol to α,β-unsaturated ketone.',
            'reactants_text': '1-cyclohexyl-2-methyl-3-butyn-2-ol',
            'products_text': '(E)-1-cyclohexyl-2-methyl-2-buten-1-one (enone)',
            'reagents_text': 'HgSO4 / H2SO4 (cat.)',
            'conditions_text': 'aqueous acid, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase18_shallow_top10_v1 sprint for Meyer-Schuster and Rupe Rearrangement. [phase18_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)(C#C)C1CCCCC1', '1-cyclohexyl-2-methyl-3-butyn-2-ol', 'reactants_text', 1),
                ('product', 'O=C(/C=C(\\C)/C1CCCCC1)', '(E)-1-cyclohexyl-2-methyl-2-buten-1-one (enone)', 'products_text', 1),
            ],
        },
    ],
}




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
    """Run smiles_guard over every SMILES in every seed. Returns list of
    failures (empty on success)."""
    failures: List[Dict[str, Any]] = []
    for seed in seeds:
        for role, smiles, name, _field, queryable in seed['molecules']:
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


def normalize_aliases(conn: sqlite3.Connection, families: Sequence[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    target_set = set(families)
    for alias, official in DATA_ALIASES.items():
        if official not in target_set:
            continue
        fam_norm = family_norm(official)
        cur = conn.execute('UPDATE reaction_extracts SET reaction_family_name=?, reaction_family_name_norm=?, updated_at=? WHERE reaction_family_name=?',
                           (official, fam_norm, NOW, alias))
        out.append({'table': 'reaction_extracts', 'from': alias, 'to': official, 'rows_updated': cur.rowcount})
        cur = conn.execute('UPDATE extract_molecules SET reaction_family_name=? WHERE reaction_family_name=?', (official, alias))
        out.append({'table': 'extract_molecules', 'from': alias, 'to': official, 'rows_updated': cur.rowcount})
    return out


def backfill_extract_smiles_from_molecules(conn: sqlite3.Connection, families: Sequence[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for family in families:
        rows = conn.execute("""
            SELECT re.id,
                   GROUP_CONCAT(CASE WHEN em.role='reactant' AND em.queryable=1 AND em.smiles IS NOT NULL THEN em.smiles END, ' | ') AS reactant_smiles,
                   GROUP_CONCAT(CASE WHEN em.role='product'  AND em.queryable=1 AND em.smiles IS NOT NULL THEN em.smiles END, ' | ') AS product_smiles
            FROM reaction_extracts re
            LEFT JOIN extract_molecules em ON em.extract_id = re.id
            WHERE re.reaction_family_name = ?
            GROUP BY re.id
        """, (family,)).fetchall()
        updated = 0
        for extract_id, reactant, product in rows:
            cur = conn.execute("""
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
            """, (reactant, product, reactant, product, NOW, extract_id))
            updated += cur.rowcount
        out.append({'family': family, 'extract_rows_touched': updated})
    return out


def find_anchor(conn: sqlite3.Connection, seed: Dict[str, Any]) -> Tuple[int, int, str]:
    row = conn.execute("""
        SELECT re.scheme_candidate_id, COALESCE(pi.page_no, 0) AS page_no, COALESCE(pi.image_filename, ?) AS image_filename
        FROM reaction_extracts re
        LEFT JOIN scheme_candidates sc ON sc.id = re.scheme_candidate_id
        LEFT JOIN page_images pi ON pi.id = sc.page_image_id
        WHERE re.reaction_family_name=? AND re.scheme_candidate_id IS NOT NULL
        ORDER BY CASE WHEN re.extract_kind='canonical_overview' THEN 0 WHEN re.extract_kind='application_example' THEN 1 ELSE 2 END, re.id
        LIMIT 1
    """, (seed.get('anchor_image', 'phase16_anchor.jpg'), seed['family'])).fetchone()
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
               SUM(CASE WHEN extract_kind='application_example' THEN 1 ELSE 0 END) AS application_count,
               SUM(CASE WHEN COALESCE(reactant_smiles,'')<>'' THEN 1 ELSE 0 END) AS extract_with_reactant,
               SUM(CASE WHEN COALESCE(product_smiles ,'')<>'' THEN 1 ELSE 0 END) AS extract_with_product,
               SUM(CASE WHEN COALESCE(reactant_smiles,'')<>'' AND COALESCE(product_smiles,'')<>'' THEN 1 ELSE 0 END) AS extract_with_both
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
        'completion_minimum_pass': (
            int((row['overview_count']    or 0)) >= 1 and
            int((row['application_count'] or 0)) >= 2 and
            int((mol['queryable_reactants'] or 0)) >= 1 and
            int((mol['queryable_products']  or 0)) >= 1 and
            int(pair_count or 0) >= 1
        ),
        'rich_completion_pass': (
            int((row['overview_count']    or 0)) >= 1 and
            int((row['application_count'] or 0)) >= 2 and
            int((mol['queryable_reactants'] or 0)) >= 3 and
            int((mol['queryable_products']  or 0)) >= 3 and
            int(pair_count or 0) >= 3
        ),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description='Complete phase16 shallow top10 in 5+5 batches.')
    ap.add_argument('--db', default='app/labint.db')
    ap.add_argument('--report-dir', default='reports/family_completion_phase16_shallow_top10')
    ap.add_argument('--batch', choices=['a', 'b', 'all'], default='all')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    db_path = Path(args.db)
    report_dir = Path(args.report_dir) / dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    report_dir.mkdir(parents=True, exist_ok=True)

    batches = ['a', 'b'] if args.batch == 'all' else [args.batch]

    # --- preflight smiles_guard ---
    all_seeds: List[Dict[str, Any]] = []
    for b in batches:
        all_seeds.extend(SEEDS_BY_BATCH[b])
    guard_failures = preflight_smiles_guard(all_seeds)
    if guard_failures:
        print(f'[phase16] smiles_guard REJECTED {len(guard_failures)} molecule(s) before DB touch:')
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

        all_results: List[Dict[str, Any]] = []
        all_alias: List[Dict[str, Any]] = []
        all_backfill: List[Dict[str, Any]] = []
        verify: List[Dict[str, Any]] = []
        families: List[str] = []

        for batch in batches:
            fams = BATCH_FAMILIES[batch]
            families.extend(fams)
            all_alias.extend(normalize_aliases(conn, fams))
            all_backfill.extend(backfill_extract_smiles_from_molecules(conn, fams))
            all_results.extend([insert_seed(conn, seed) for seed in SEEDS_BY_BATCH[batch]])

        for fam in families:
            verify.append(verify_family(conn, fam))

        after_families = queryable_family_count(conn)
        after_queryable = queryable_count(conn)

        payload = {
            'tag': TAG,
            'db': str(db_path),
            'batch': args.batch,
            'dry_run': bool(args.dry_run),
            'before': {'queryable_family_count': before_families, 'queryable_count': before_queryable},
            'after':  {'queryable_family_count': after_families,  'queryable_count': after_queryable},
            'alias_updates': all_alias,
            'backfill_updates': all_backfill,
            'seed_results': all_results,
            'verify': verify,
        }

        suffix = f'phase16_shallow_top10_{args.batch}'
        jpath = report_dir / f'{suffix}_summary.json'
        mpath = report_dir / f'{suffix}_summary.md'
        jpath.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8')
        lines = [
            f'# Family completion {suffix} summary',
            '',
            f'- db: `{db_path}`',
            f'- dry_run: `{args.dry_run}`',
            f'- queryable_family_count: {before_families} → {after_families}',
            f'- queryable_count: {before_queryable} → {after_queryable}',
            '',
        ]
        for item in verify:
            lines.append(f"## {item['family']}")
            for k in ['extract_count', 'overview_count', 'application_count',
                      'extract_with_reactant', 'extract_with_product', 'extract_with_both',
                      'queryable_reactants', 'queryable_products', 'unique_queryable_pair_count',
                      'completion_minimum_pass', 'rich_completion_pass']:
                lines.append(f'- {k}: {item[k]}')
            lines.append('')
        mpath.write_text('\n'.join(lines) + '\n', encoding='utf-8')

        if args.dry_run:
            conn.rollback()
            print('[DRY-RUN] rolled back changes')
        else:
            conn.commit()
            print('[APPLY] committed changes')
        print(f'summary json: {jpath}')
        print(f'summary md:   {mpath}')
        return 0
    except Exception as exc:
        conn.rollback()
        print(f'[phase16] exception, rolled back: {exc}')
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    sys.exit(main())
