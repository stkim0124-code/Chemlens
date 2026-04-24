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

TAG = 'phase21_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Paal-Knorr Furan Synthesis',
        'Paal-Knorr Pyrrole Synthesis',
        'Passerini Multicomponent Reaction',
        'Paternò-Büchi Reaction',
        'Pauson-Khand Reaction',
    ],
    'b': [
        'Payne Rearrangement',
        'Perkin Reaction',
        'Petasis Boronic Acid-Mannich Reaction',
        'Petasis-Ferrier Rearrangement',
        'Peterson Olefination',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Paal-Knorr Furan Synthesis ===
        {
            'family': 'Paal-Knorr Furan Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Paal-Knorr furan synthesis: 1,4-diketone cyclodehydrates under acid to give 2,5-disubstituted furan.',
            'reactants_text': 'hexane-2,5-dione',
            'products_text': '2,5-dimethylfuran',
            'reagents_text': 'H2SO4 or P2O5',
            'conditions_text': 'toluene, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase21_shallow_top10_v1 sprint for Paal-Knorr Furan Synthesis. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(=O)CCC(=O)C', 'hexane-2,5-dione', 'reactants_text', 1),
                ('product', 'Cc1ccc(C)o1', '2,5-dimethylfuran', 'products_text', 1),
            ],
        },
        {
            'family': 'Paal-Knorr Furan Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Paal-Knorr cyclodehydration of aliphatic 1,4-diketone.',
            'reactants_text': 'octane-3,6-dione',
            'products_text': '2,5-diethylfuran',
            'reagents_text': 'concentrated H2SO4',
            'conditions_text': 'benzene, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase21_shallow_top10_v1 sprint for Paal-Knorr Furan Synthesis. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCC(=O)CCC(=O)CC', 'octane-3,6-dione', 'reactants_text', 1),
                ('product', 'CCc1ccc(CC)o1', '2,5-diethylfuran', 'products_text', 1),
            ],
        },
        {
            'family': 'Paal-Knorr Furan Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Paal-Knorr synthesis of diarylfuran.',
            'reactants_text': '1,4-diphenyl-1,4-butanedione',
            'products_text': '2,5-diphenylfuran',
            'reagents_text': 'p-TsOH',
            'conditions_text': 'toluene, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase21_shallow_top10_v1 sprint for Paal-Knorr Furan Synthesis. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(c1ccccc1)CCC(=O)c2ccccc2', '1,4-diphenyl-1,4-butanedione', 'reactants_text', 1),
                ('product', 'c1ccc(-c2ccc(-c3ccccc3)o2)cc1', '2,5-diphenylfuran', 'products_text', 1),
            ],
        },
        # === Paal-Knorr Pyrrole Synthesis ===
        {
            'family': 'Paal-Knorr Pyrrole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Paal-Knorr pyrrole synthesis: 1,4-diketone + primary amine (or NH3) gives pyrrole.',
            'reactants_text': 'hexane-2,5-dione + ammonia',
            'products_text': '2,5-dimethyl-1H-pyrrole',
            'reagents_text': 'NH4OAc or NH3',
            'conditions_text': 'EtOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase21_shallow_top10_v1 sprint for Paal-Knorr Pyrrole Synthesis. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(=O)CCC(=O)C.N', 'hexane-2,5-dione + ammonia', 'reactants_text', 1),
                ('product', 'Cc1ccc(C)[nH]1', '2,5-dimethyl-1H-pyrrole', 'products_text', 1),
            ],
        },
        {
            'family': 'Paal-Knorr Pyrrole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Paal-Knorr on primary amine gives N-alkyl pyrrole.',
            'reactants_text': 'hexane-2,5-dione + ethylamine',
            'products_text': '1-ethyl-2,5-dimethyl-1H-pyrrole',
            'reagents_text': 'ethylamine, AcOH (cat.)',
            'conditions_text': 'MeOH, 60°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase21_shallow_top10_v1 sprint for Paal-Knorr Pyrrole Synthesis. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(=O)CCC(=O)C.NCC', 'hexane-2,5-dione + ethylamine', 'reactants_text', 1),
                ('product', 'CCN1C(C)=CC=C1C', '1-ethyl-2,5-dimethyl-1H-pyrrole', 'products_text', 1),
            ],
        },
        {
            'family': 'Paal-Knorr Pyrrole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Paal-Knorr with aromatic amine for N-aryl pyrrole.',
            'reactants_text': '1,4-diphenyl-1,4-butanedione + aniline',
            'products_text': '1,2,5-triphenyl-1H-pyrrole',
            'reagents_text': 'aniline, p-TsOH',
            'conditions_text': 'toluene, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase21_shallow_top10_v1 sprint for Paal-Knorr Pyrrole Synthesis. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(c1ccccc1)CCC(=O)c2ccccc2.Nc3ccccc3', '1,4-diphenyl-1,4-butanedione + aniline', 'reactants_text', 1),
                ('product', 'c1ccc(N2C(c3ccccc3)=CC=C2c4ccccc4)cc1', '1,2,5-triphenyl-1H-pyrrole', 'products_text', 1),
            ],
        },
        # === Passerini Multicomponent Reaction ===
        {
            'family': 'Passerini Multicomponent Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Passerini three-component reaction: aldehyde + carboxylic acid + isocyanide gives α-acyloxy amide.',
            'reactants_text': 'benzaldehyde + acetic acid + tert-butyl isocyanide',
            'products_text': 'α-acyloxy amide (Passerini product)',
            'reagents_text': 'CH2Cl2 or neat',
            'conditions_text': 'rt, 24 h',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase21_shallow_top10_v1 sprint for Passerini Multicomponent Reaction. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1.OC(=O)C.[C-]#[N+]C(C)(C)C', 'benzaldehyde + acetic acid + tert-butyl isocyanide', 'reactants_text', 1),
                ('product', 'O=C(C)OC(c1ccccc1)C(=O)NC(C)(C)C', 'α-acyloxy amide (Passerini product)', 'products_text', 1),
            ],
        },
        {
            'family': 'Passerini Multicomponent Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Passerini-3CR with ketone, aromatic carboxylic acid and aryl isocyanide.',
            'reactants_text': 'butan-2-one + benzoic acid + phenyl isocyanide',
            'products_text': 'α-acyloxy amide from ketone',
            'reagents_text': 'CH2Cl2',
            'conditions_text': 'rt, 48 h',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase21_shallow_top10_v1 sprint for Passerini Multicomponent Reaction. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(=O)CC.OC(=O)c1ccccc1.[C-]#[N+]c2ccccc2', 'butan-2-one + benzoic acid + phenyl isocyanide', 'reactants_text', 1),
                ('product', 'O=C(c1ccccc1)OC(CC)(C)C(=O)Nc2ccccc2', 'α-acyloxy amide from ketone', 'products_text', 1),
            ],
        },
        {
            'family': 'Passerini Multicomponent Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Passerini reaction giving tri-substituted α-acyloxy amide.',
            'reactants_text': 'cyclohexanecarboxaldehyde + isobutyric acid + isobutyl isocyanide',
            'products_text': 'α-acyloxy amide',
            'reagents_text': 'neat or CH2Cl2',
            'conditions_text': 'rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase21_shallow_top10_v1 sprint for Passerini Multicomponent Reaction. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CC1CCCCC1.OC(=O)C(C)C.[C-]#[N+]CC(C)C', 'cyclohexanecarboxaldehyde + isobutyric acid + isobutyl isocyanide', 'reactants_text', 1),
                ('product', 'O=C(C(C)C)OC(C1CCCCC1)C(=O)NCC(C)C', 'α-acyloxy amide', 'products_text', 1),
            ],
        },
        # === Paternò-Büchi Reaction ===
        {
            'family': 'Paternò-Büchi Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Paternò-Büchi: photoexcited ketone (n,π*) adds [2+2] to alkene giving oxetane.',
            'reactants_text': 'acetone + propene',
            'products_text': '2,2-dimethyl-3-methyl-oxetane',
            'reagents_text': 'hν (UV)',
            'conditions_text': 'benzene, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase21_shallow_top10_v1 sprint for Paternò-Büchi Reaction. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)C.C=CC', 'acetone + propene', 'reactants_text', 1),
                ('product', 'CC1(C)OC(C)C1', '2,2-dimethyl-3-methyl-oxetane', 'products_text', 1),
            ],
        },
        {
            'family': 'Paternò-Büchi Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Paternò-Büchi with aryl ketone and aliphatic alkene.',
            'reactants_text': 'acetophenone + 1-butene',
            'products_text': '2-methyl-2-phenyl-4-ethyl-oxetane',
            'reagents_text': 'hν',
            'conditions_text': 'benzene, 25°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase21_shallow_top10_v1 sprint for Paternò-Büchi Reaction. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(c1ccccc1)C.C=CCC', 'acetophenone + 1-butene', 'reactants_text', 1),
                ('product', 'c1ccc(C2(C)OC(CC)C2)cc1', '2-methyl-2-phenyl-4-ethyl-oxetane', 'products_text', 1),
            ],
        },
        {
            'family': 'Paternò-Büchi Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Paternò-Büchi reaction of aldehyde and 1,1-disubstituted alkene.',
            'reactants_text': 'benzaldehyde + isobutylene',
            'products_text': '2,2-dimethyl-4-phenyl-oxetane',
            'reagents_text': 'hν',
            'conditions_text': 'acetone sensitizer, benzene',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase21_shallow_top10_v1 sprint for Paternò-Büchi Reaction. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1.C=C(C)C', 'benzaldehyde + isobutylene', 'reactants_text', 1),
                ('product', 'c1ccc(C2OC(C)(C)C2)cc1', '2,2-dimethyl-4-phenyl-oxetane', 'products_text', 1),
            ],
        },
        # === Pauson-Khand Reaction ===
        {
            'family': 'Pauson-Khand Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Pauson-Khand [2+2+1] cycloaddition of alkyne, alkene, and CO giving cyclopentenone.',
            'reactants_text': 'phenylacetylene + ethylene + CO',
            'products_text': '2-phenyl-cyclopent-2-enone',
            'reagents_text': 'Co2(CO)8, NMO',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase21_shallow_top10_v1 sprint for Pauson-Khand Reaction. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C#Cc1ccccc1.C=C.O=C=O', 'phenylacetylene + ethylene + CO', 'reactants_text', 1),
                ('product', 'O=C1CC(c2ccccc2)=CC1', '2-phenyl-cyclopent-2-enone', 'products_text', 1),
            ],
        },
        {
            'family': 'Pauson-Khand Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Pauson-Khand producing trisubstituted cyclopentenone.',
            'reactants_text': 'propyne + 1-butene + CO',
            'products_text': '2-methyl-3-ethyl-cyclopent-2-enone',
            'reagents_text': 'Co2(CO)8, NMO',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase21_shallow_top10_v1 sprint for Pauson-Khand Reaction. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C#CC.C=CCC.O=C=O', 'propyne + 1-butene + CO', 'reactants_text', 1),
                ('product', 'O=C1CC(C)=C(CC)C1', '2-methyl-3-ethyl-cyclopent-2-enone', 'products_text', 1),
            ],
        },
        {
            'family': 'Pauson-Khand Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Intermolecular Pauson-Khand giving bicyclic ring system.',
            'reactants_text': 'phenylacetylene + cyclopentene + CO',
            'products_text': 'bicyclic cyclopentenone',
            'reagents_text': 'Co2(CO)8, NMO, 4Å MS',
            'conditions_text': 'CH2Cl2',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase21_shallow_top10_v1 sprint for Pauson-Khand Reaction. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C#Cc1ccccc1.C1=CCCC1.O=C=O', 'phenylacetylene + cyclopentene + CO', 'reactants_text', 1),
                ('product', 'O=C1CC2CCCCC2=C1c3ccccc3', 'bicyclic cyclopentenone', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Payne Rearrangement ===
        {
            'family': 'Payne Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Payne rearrangement: 2,3-epoxy-1-alcohol interconverts with 1,2-epoxy-3-alcohol via intramolecular alkoxide attack.',
            'reactants_text': '2,3-epoxy-1-propanol (glycidol)',
            'products_text': '1,2-epoxy-3-propanol',
            'reagents_text': 'NaOH (aq. base)',
            'conditions_text': 'aqueous base, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase21_shallow_top10_v1 sprint for Payne Rearrangement. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCC1OC1C', '2,3-epoxy-1-propanol (glycidol)', 'reactants_text', 1),
                ('product', 'OC(C)C1CO1', '1,2-epoxy-3-propanol', 'products_text', 1),
            ],
        },
        {
            'family': 'Payne Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Payne equilibrium between 1,2- and 2,3-epoxy alcohols under basic conditions.',
            'reactants_text': '2-hydroxymethyl-1,2-epoxypropanol',
            'products_text': 'equilibrium Payne isomer',
            'reagents_text': 'aq. NaOH',
            'conditions_text': 'H2O, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase21_shallow_top10_v1 sprint for Payne Rearrangement. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCC(O)C1CO1', '2-hydroxymethyl-1,2-epoxypropanol', 'reactants_text', 1),
                ('product', 'OCC1OC1CO', 'equilibrium Payne isomer', 'products_text', 1),
            ],
        },
        {
            'family': 'Payne Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Payne rearrangement with aryl-substituted epoxy alcohol.',
            'reactants_text': '2-phenyl-2,3-epoxy-1-propanol',
            'products_text': '1,2-epoxy-3-phenyl-3-hydroxypropane',
            'reagents_text': 'NaOH / H2O',
            'conditions_text': 'rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase21_shallow_top10_v1 sprint for Payne Rearrangement. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCC1OC1c1ccccc1', '2-phenyl-2,3-epoxy-1-propanol', 'reactants_text', 1),
                ('product', 'OC(c1ccccc1)C1CO1', '1,2-epoxy-3-phenyl-3-hydroxypropane', 'products_text', 1),
            ],
        },
        # === Perkin Reaction ===
        {
            'family': 'Perkin Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Perkin reaction: aromatic aldehyde + anhydride + base condensation → α,β-unsaturated acid.',
            'reactants_text': 'benzaldehyde + acetic anhydride',
            'products_text': '(E)-cinnamic acid',
            'reagents_text': 'acetic anhydride, NaOAc (base)',
            'conditions_text': '180°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase21_shallow_top10_v1 sprint for Perkin Reaction. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1.CC(=O)OC(=O)C', 'benzaldehyde + acetic anhydride', 'reactants_text', 1),
                ('product', 'O=C(O)C=Cc1ccccc1', '(E)-cinnamic acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Perkin Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Perkin condensation on electron-rich aromatic aldehyde.',
            'reactants_text': 'p-anisaldehyde + acetic anhydride',
            'products_text': '(E)-p-methoxycinnamic acid',
            'reagents_text': 'Ac2O, K2CO3',
            'conditions_text': '180°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase21_shallow_top10_v1 sprint for Perkin Reaction. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccc(OC)cc1.CC(=O)OC(=O)C', 'p-anisaldehyde + acetic anhydride', 'reactants_text', 1),
                ('product', 'O=C(O)C=Cc1ccc(OC)cc1', '(E)-p-methoxycinnamic acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Perkin Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Perkin reaction with electron-poor aryl aldehyde.',
            'reactants_text': 'p-nitrobenzaldehyde + acetic anhydride',
            'products_text': '(E)-p-nitrocinnamic acid',
            'reagents_text': 'Ac2O, NaOAc',
            'conditions_text': '150-180°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase21_shallow_top10_v1 sprint for Perkin Reaction. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccc([N+](=O)[O-])cc1.CC(=O)OC(=O)C', 'p-nitrobenzaldehyde + acetic anhydride', 'reactants_text', 1),
                ('product', 'O=C(O)C=Cc1ccc([N+](=O)[O-])cc1', '(E)-p-nitrocinnamic acid', 'products_text', 1),
            ],
        },
        # === Petasis Boronic Acid-Mannich Reaction ===
        {
            'family': 'Petasis Boronic Acid-Mannich Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Petasis boronic Mannich: glyoxalic acid + amine + boronic acid gives α-aryl amino acid.',
            'reactants_text': 'phenylboronic acid + glyoxalic acid + piperidine',
            'products_text': 'α-aryl α-amino acid',
            'reagents_text': 'EtOH/CH2Cl2',
            'conditions_text': 'rt, 24 h',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase21_shallow_top10_v1 sprint for Petasis Boronic Acid-Mannich Reaction. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OB(O)c1ccccc1.O=CC(=O)O.N1CCCCC1', 'phenylboronic acid + glyoxalic acid + piperidine', 'reactants_text', 1),
                ('product', 'OC(=O)C(c1ccccc1)N2CCCCC2', 'α-aryl α-amino acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Petasis Boronic Acid-Mannich Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Petasis multicomponent synthesis of α-aryl amino acid.',
            'reactants_text': 'p-tolylboronic acid + glyoxalic acid + benzylamine',
            'products_text': 'α-tolyl-N-benzyl amino acid',
            'reagents_text': 'EtOH, rt',
            'conditions_text': 'rt, 24 h',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase21_shallow_top10_v1 sprint for Petasis Boronic Acid-Mannich Reaction. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OB(O)c1ccc(C)cc1.O=Cc(=O)O.NCc2ccccc2', 'p-tolylboronic acid + glyoxalic acid + benzylamine', 'reactants_text', 1),
                ('product', 'OC(=O)C(c1ccc(C)cc1)NCc2ccccc2', 'α-tolyl-N-benzyl amino acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Petasis Boronic Acid-Mannich Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Petasis allylic amination using vinyl boronic acid.',
            'reactants_text': '(E)-1-pentenyl boronic acid + benzaldehyde + cyclopentylamine',
            'products_text': 'allylic amine',
            'reagents_text': 'CH2Cl2, rt',
            'conditions_text': 'rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase21_shallow_top10_v1 sprint for Petasis Boronic Acid-Mannich Reaction. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OB(O)/C=C/CCC.O=Cc1ccccc1.NC2CCCC2', '(E)-1-pentenyl boronic acid + benzaldehyde + cyclopentylamine', 'reactants_text', 1),
                ('product', 'C(/C=C/CCC)(c1ccccc1)NC2CCCC2', 'allylic amine', 'products_text', 1),
            ],
        },
        # === Petasis-Ferrier Rearrangement ===
        {
            'family': 'Petasis-Ferrier Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Petasis-Ferrier rearrangement: enol acetal isomerizes to β-alkoxy ketone/tetrahydropyranone under Lewis acid.',
            'reactants_text': 'vinyl acetal (enol acetal)',
            'products_text': 'tetrahydropyranone',
            'reagents_text': 'Me2AlCl or Cp2TiMe2',
            'conditions_text': 'CH2Cl2, -40°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase21_shallow_top10_v1 sprint for Petasis-Ferrier Rearrangement. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC1=COC(C)C(C)O1', 'vinyl acetal (enol acetal)', 'reactants_text', 1),
                ('product', 'O=C(C)C1OC(C)C(C)C1', 'tetrahydropyranone', 'products_text', 1),
            ],
        },
        {
            'family': 'Petasis-Ferrier Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Petasis-Ferrier with aryl substituent.',
            'reactants_text': 'aryl enol acetal',
            'products_text': 'substituted tetrahydropyranone',
            'reagents_text': 'Me2AlCl',
            'conditions_text': 'CH2Cl2, -78→-20°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase21_shallow_top10_v1 sprint for Petasis-Ferrier Rearrangement. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC1=COC(c2ccccc2)C(C)O1', 'aryl enol acetal', 'reactants_text', 1),
                ('product', 'O=C(C)C1OC(c2ccccc2)C(C)C1', 'substituted tetrahydropyranone', 'products_text', 1),
            ],
        },
        {
            'family': 'Petasis-Ferrier Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Petasis-Ferrier on spiro enol acetal.',
            'reactants_text': 'spirocyclic enol acetal',
            'products_text': 'spirocyclic ketone',
            'reagents_text': 'Cp2TiMe2 (Petasis reagent)',
            'conditions_text': 'toluene, 60°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase21_shallow_top10_v1 sprint for Petasis-Ferrier Rearrangement. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC1=COC2(CCCCC2)C1', 'spirocyclic enol acetal', 'reactants_text', 1),
                ('product', 'O=C(C)C3CC4(CCCCC4)CC3', 'spirocyclic ketone', 'products_text', 1),
            ],
        },
        # === Peterson Olefination ===
        {
            'family': 'Peterson Olefination',
            'extract_kind': 'application_example',
            'transformation_text': 'Peterson olefination: α-silyl carbanion adds to ketone, then β-elimination to alkene.',
            'reactants_text': 'acetophenone + (trimethylsilyl)methyl anion',
            'products_text': '2-phenyl-1-propene',
            'reagents_text': 'TMS-CH2MgCl (or TMS-CH2Li), then acid/base workup',
            'conditions_text': 'THF, -78°C→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase21_shallow_top10_v1 sprint for Peterson Olefination. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(=O)c1ccccc1.C[Si](C)(C)CC', 'acetophenone + (trimethylsilyl)methyl anion', 'reactants_text', 1),
                ('product', 'CC(=Cc1ccccc1)C', '2-phenyl-1-propene', 'products_text', 1),
            ],
        },
        {
            'family': 'Peterson Olefination',
            'extract_kind': 'application_example',
            'transformation_text': 'Peterson olefination with secondary α-silyl anion and diaryl ketone.',
            'reactants_text': 'benzophenone + α-silyl butyl anion',
            'products_text': '1,1-diphenyl-2-methyl-propene',
            'reagents_text': 'n-BuLi, TMS-CH(CH3)2; workup',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase21_shallow_top10_v1 sprint for Peterson Olefination. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(c1ccccc1)c2ccccc2.CC(C[Si](C)(C)C)C', 'benzophenone + α-silyl butyl anion', 'reactants_text', 1),
                ('product', 'CC(=C(c1ccccc1)c2ccccc2)C', '1,1-diphenyl-2-methyl-propene', 'products_text', 1),
            ],
        },
        {
            'family': 'Peterson Olefination',
            'extract_kind': 'application_example',
            'transformation_text': 'Peterson olefination producing exocyclic alkene on cyclohexanone.',
            'reactants_text': 'cyclohexanone + benzyltrimethylsilane anion',
            'products_text': 'benzylidenecyclohexane',
            'reagents_text': 'benzylTMS, n-BuLi; KH or acid workup',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase21_shallow_top10_v1 sprint for Peterson Olefination. [phase21_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCCC1.C[Si](C)(C)CC1=CC=CC=C1', 'cyclohexanone + benzyltrimethylsilane anion', 'reactants_text', 1),
                ('product', 'C(=C2CCCCC2)c3ccccc3', 'benzylidenecyclohexane', 'products_text', 1),
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
