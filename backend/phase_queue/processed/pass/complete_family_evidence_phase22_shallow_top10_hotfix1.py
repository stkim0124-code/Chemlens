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

TAG = 'phase22_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Pfitzner-Moffatt Oxidation',
        'Pictet-Spengler Tetrahydroisoquinoline Synthesis',
        'Pinacol and Semipinacol Rearrangement',
        'Pinner Reaction',
        'Pinnick Oxidation',
    ],
    'b': [
        'Polonovski Reaction',
        'Pomeranz-Fritsch Reaction',
        'Prilezhaev Reaction',
        'Prins Reaction',
        'Prins-Pinacol Rearrangement',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Pfitzner-Moffatt Oxidation ===
        {
            'family': 'Pfitzner-Moffatt Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Pfitzner-Moffatt oxidation: DMSO/DCC couple to activate alcohol, oxidize primary OH to aldehyde without over-oxidation.',
            'reactants_text': 'benzyl alcohol',
            'products_text': 'benzaldehyde',
            'reagents_text': 'DCC, DMSO, pyridinium trifluoroacetate',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase22_shallow_top10_v1 sprint for Pfitzner-Moffatt Oxidation. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCc1ccccc1', 'benzyl alcohol', 'reactants_text', 1),
                ('product', 'O=Cc1ccccc1', 'benzaldehyde', 'products_text', 1),
            ],
        },
        {
            'family': 'Pfitzner-Moffatt Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Pfitzner-Moffatt oxidation of secondary alcohol to ketone.',
            'reactants_text': '1-phenylethanol',
            'products_text': 'acetophenone',
            'reagents_text': 'DCC, DMSO, H3PO4',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase22_shallow_top10_v1 sprint for Pfitzner-Moffatt Oxidation. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)c1ccccc1', '1-phenylethanol', 'reactants_text', 1),
                ('product', 'O=C(C)c1ccccc1', 'acetophenone', 'products_text', 1),
            ],
        },
        {
            'family': 'Pfitzner-Moffatt Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Pfitzner-Moffatt for aliphatic primary alcohol.',
            'reactants_text': 'cyclopentylmethanol',
            'products_text': 'cyclopentanecarboxaldehyde',
            'reagents_text': 'DCC, DMSO, pyridine·TFA',
            'conditions_text': 'rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase22_shallow_top10_v1 sprint for Pfitzner-Moffatt Oxidation. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCC1CCCC1', 'cyclopentylmethanol', 'reactants_text', 1),
                ('product', 'O=CC1CCCC1', 'cyclopentanecarboxaldehyde', 'products_text', 1),
            ],
        },
        # === Pictet-Spengler Tetrahydroisoquinoline Synthesis ===
        {
            'family': 'Pictet-Spengler Tetrahydroisoquinoline Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Pictet-Spengler: β-arylethylamine + aldehyde under acid gives tetrahydroisoquinoline via iminium/cyclization.',
            'reactants_text': 'β-phenethylamine + acetaldehyde',
            'products_text': '1-methyl-1,2,3,4-tetrahydroisoquinoline',
            'reagents_text': 'HCl (cat.), EtOH',
            'conditions_text': 'reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase22_shallow_top10_v1 sprint for Pictet-Spengler Tetrahydroisoquinoline Synthesis. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'NCCc1ccccc1.O=CC', 'β-phenethylamine + acetaldehyde', 'reactants_text', 1),
                ('product', 'CC1NCCc2ccccc21', '1-methyl-1,2,3,4-tetrahydroisoquinoline', 'products_text', 1),
            ],
        },
        {
            'family': 'Pictet-Spengler Tetrahydroisoquinoline Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Pictet-Spengler on activated phenethylamine with aryl aldehyde.',
            'reactants_text': 'p-methoxyphenethylamine + benzaldehyde',
            'products_text': '6-methoxy-1-phenyl-1,2,3,4-tetrahydroisoquinoline',
            'reagents_text': 'TFA',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase22_shallow_top10_v1 sprint for Pictet-Spengler Tetrahydroisoquinoline Synthesis. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'NCCc1ccc(OC)cc1.O=Cc2ccccc2', 'p-methoxyphenethylamine + benzaldehyde', 'reactants_text', 1),
                ('product', 'COc1ccc2c(c1)CCN(C)C2c3ccccc3', '6-methoxy-1-phenyl-1,2,3,4-tetrahydroisoquinoline', 'products_text', 1),
            ],
        },
        {
            'family': 'Pictet-Spengler Tetrahydroisoquinoline Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Pictet-Spengler on doubly activated phenethylamine.',
            'reactants_text': 'dimethoxyphenethylamine + phenylacetaldehyde',
            'products_text': 'dimethoxy-1-benzyl-tetrahydroisoquinoline',
            'reagents_text': 'HCl, EtOH',
            'conditions_text': 'reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase22_shallow_top10_v1 sprint for Pictet-Spengler Tetrahydroisoquinoline Synthesis. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'NCCc1ccc(OC)c(OC)c1.O=CCc2ccccc2', 'dimethoxyphenethylamine + phenylacetaldehyde', 'reactants_text', 1),
                ('product', 'COc1cc2c(cc1OC)CCN(C)C2Cc3ccccc3', 'dimethoxy-1-benzyl-tetrahydroisoquinoline', 'products_text', 1),
            ],
        },
        # === Pinacol and Semipinacol Rearrangement ===
        {
            'family': 'Pinacol and Semipinacol Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Pinacol rearrangement: 1,2-diol protonated, carbocation formed, 1,2-methyl shift gives ketone.',
            'reactants_text': 'pinacol (2,3-dimethyl-2,3-butanediol)',
            'products_text': 'pinacolone (3,3-dimethyl-2-butanone)',
            'reagents_text': 'concentrated H2SO4',
            'conditions_text': '50°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase22_shallow_top10_v1 sprint for Pinacol and Semipinacol Rearrangement. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)(C)C(O)(C)C', 'pinacol (2,3-dimethyl-2,3-butanediol)', 'reactants_text', 1),
                ('product', 'CC(=O)C(C)(C)C', 'pinacolone (3,3-dimethyl-2-butanone)', 'products_text', 1),
            ],
        },
        {
            'family': 'Pinacol and Semipinacol Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Semipinacol rearrangement: 1,2-diol to ring-expanded ketone via 1,2-alkyl shift.',
            'reactants_text': '1-methylcyclopentane-1,2-diol',
            'products_text': '2-methylcyclohexanone (ring-expanded)',
            'reagents_text': 'H2SO4',
            'conditions_text': 'aqueous acid, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase22_shallow_top10_v1 sprint for Pinacol and Semipinacol Rearrangement. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC1(C)CCCC1O', '1-methylcyclopentane-1,2-diol', 'reactants_text', 1),
                ('product', 'O=C1CCCCC1C', '2-methylcyclohexanone (ring-expanded)', 'products_text', 1),
            ],
        },
        {
            'family': 'Pinacol and Semipinacol Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Pinacol/semipinacol on unsymmetric diol to branched carbonyl with 1,2-alkyl shift.',
            'reactants_text': '1-cyclohexyl-2-methyl-2-hydroxy-1-propanol diol',
            'products_text': 'α-branched aliphatic aldehyde',
            'reagents_text': 'BF3·OEt2',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase22_shallow_top10_v1 sprint for Pinacol and Semipinacol Rearrangement. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)(C)C(O)C1CCCCC1', '1-cyclohexyl-2-methyl-2-hydroxy-1-propanol diol', 'reactants_text', 1),
                ('product', 'O=CC(C)(C)C1CCCCC1', 'α-branched aliphatic aldehyde', 'products_text', 1),
            ],
        },
        # === Pinner Reaction ===
        {
            'family': 'Pinner Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Pinner reaction: nitrile + alcohol under HCl gives imidate ester HCl salt.',
            'reactants_text': 'phenylacetonitrile',
            'products_text': 'methyl phenylacetimidate',
            'reagents_text': 'MeOH, HCl (gas)',
            'conditions_text': '0°C, CH2Cl2',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase22_shallow_top10_v1 sprint for Pinner Reaction. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'N#CCc1ccccc1', 'phenylacetonitrile', 'reactants_text', 1),
                ('product', 'COC(=N)Cc1ccccc1', 'methyl phenylacetimidate', 'products_text', 1),
            ],
        },
        {
            'family': 'Pinner Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Pinner synthesis of imidate from simple nitrile.',
            'reactants_text': 'acetonitrile',
            'products_text': 'methyl acetimidate',
            'reagents_text': 'MeOH/HCl',
            'conditions_text': '0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase22_shallow_top10_v1 sprint for Pinner Reaction. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'N#CC', 'acetonitrile', 'reactants_text', 1),
                ('product', 'COC(=N)C', 'methyl acetimidate', 'products_text', 1),
            ],
        },
        {
            'family': 'Pinner Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Pinner with aromatic nitrile and ethanol.',
            'reactants_text': 'benzonitrile',
            'products_text': 'ethyl benzimidate',
            'reagents_text': 'EtOH/HCl',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase22_shallow_top10_v1 sprint for Pinner Reaction. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'N#Cc1ccccc1', 'benzonitrile', 'reactants_text', 1),
                ('product', 'CCOC(=N)c1ccccc1', 'ethyl benzimidate', 'products_text', 1),
            ],
        },
        # === Pinnick Oxidation ===
        {
            'family': 'Pinnick Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Pinnick oxidation: aldehyde to carboxylic acid with NaClO2/NaH2PO4/2-methyl-2-butene (HOCl scavenger).',
            'reactants_text': 'benzaldehyde',
            'products_text': 'benzoic acid',
            'reagents_text': 'NaClO2, NaH2PO4, 2-methyl-2-butene',
            'conditions_text': 'tBuOH/H2O, 0°C→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase22_shallow_top10_v1 sprint for Pinnick Oxidation. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1),
                ('product', 'O=C(O)c1ccccc1', 'benzoic acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Pinnick Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Pinnick oxidation of α,β-unsaturated aldehyde preserving alkene.',
            'reactants_text': 'cinnamaldehyde',
            'products_text': 'cinnamic acid',
            'reagents_text': 'NaClO2, NaH2PO4, 2-methyl-2-butene',
            'conditions_text': 'tBuOH/H2O',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase22_shallow_top10_v1 sprint for Pinnick Oxidation. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CC=Cc1ccccc1', 'cinnamaldehyde', 'reactants_text', 1),
                ('product', 'O=C(O)C=Cc1ccccc1', 'cinnamic acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Pinnick Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Pinnick oxidation of aliphatic aldehyde.',
            'reactants_text': 'cyclohexanecarboxaldehyde',
            'products_text': 'cyclohexanecarboxylic acid',
            'reagents_text': 'NaClO2, NaH2PO4, 2-methyl-2-butene',
            'conditions_text': 'tBuOH/H2O, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase22_shallow_top10_v1 sprint for Pinnick Oxidation. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CC1CCCCC1', 'cyclohexanecarboxaldehyde', 'reactants_text', 1),
                ('product', 'O=C(O)C1CCCCC1', 'cyclohexanecarboxylic acid', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Polonovski Reaction ===
        {
            'family': 'Polonovski Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Polonovski: tertiary amine N-oxide + acetic anhydride gives iminium ion after α-proton loss; hydrolysis furnishes secondary amine.',
            'reactants_text': 'trimethylamine N-oxide + acetic anhydride',
            'products_text': 'dimethylaminomethyl iminium (via elimination)',
            'reagents_text': 'Ac2O',
            'conditions_text': 'CHCl3, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase22_shallow_top10_v1 sprint for Polonovski Reaction. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C[N+](C)(C)[O-].CC(=O)OC(=O)C', 'trimethylamine N-oxide + acetic anhydride', 'reactants_text', 1),
                ('product', 'CN(C)C', 'dimethylaminomethyl iminium (via elimination)', 'products_text', 1),
            ],
        },
        {
            'family': 'Polonovski Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Polonovski-Potier: N-oxide + anhydride forms iminium with elimination giving enamine.',
            'reactants_text': 'N-methyl piperidine N-oxide + Ac2O',
            'products_text': 'α,β-unsaturated enamine',
            'reagents_text': 'Ac2O, cold',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase22_shallow_top10_v1 sprint for Polonovski Reaction. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CN1CCCCC1[O-].CC(=O)OC(=O)C', 'N-methyl piperidine N-oxide + Ac2O', 'reactants_text', 1),
                ('product', 'C=C1N(C)CCCC1', 'α,β-unsaturated enamine', 'products_text', 1),
            ],
        },
        {
            'family': 'Polonovski Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Polonovski demethylation via acyl activation and β-elimination.',
            'reactants_text': 'N,N-diethylmethylamine N-oxide + acetyl chloride',
            'products_text': 'tertiary amine (demethylated product)',
            'reagents_text': 'AcCl or Ac2O',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase22_shallow_top10_v1 sprint for Polonovski Reaction. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC[N+](CC)(C)[O-].CC(=O)Cl', 'N,N-diethylmethylamine N-oxide + acetyl chloride', 'reactants_text', 1),
                ('product', 'CCN(CC)C', 'tertiary amine (demethylated product)', 'products_text', 1),
            ],
        },
        # === Pomeranz-Fritsch Reaction ===
        {
            'family': 'Pomeranz-Fritsch Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Pomeranz-Fritsch: aromatic aldehyde + aminoacetaldehyde acetal gives imine → acid-catalyzed cyclization to isoquinoline.',
            'reactants_text': 'benzaldehyde + aminoacetaldehyde diethyl acetal',
            'products_text': 'isoquinoline',
            'reagents_text': 'aminoacetaldehyde diethyl acetal; H2SO4',
            'conditions_text': 'H2SO4 then reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase22_shallow_top10_v1 sprint for Pomeranz-Fritsch Reaction. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1.NCC(OCC)OCC', 'benzaldehyde + aminoacetaldehyde diethyl acetal', 'reactants_text', 1),
                ('product', 'c1ccc2cnccc2c1', 'isoquinoline', 'products_text', 1),
            ],
        },
        {
            'family': 'Pomeranz-Fritsch Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Pomeranz-Fritsch on methoxy aryl aldehyde.',
            'reactants_text': 'p-anisaldehyde + aminoacetaldehyde acetal',
            'products_text': '6-methoxyisoquinoline',
            'reagents_text': 'H2SO4',
            'conditions_text': 'reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase22_shallow_top10_v1 sprint for Pomeranz-Fritsch Reaction. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccc(OC)cc1.NCC(OCC)OCC', 'p-anisaldehyde + aminoacetaldehyde acetal', 'reactants_text', 1),
                ('product', 'COc1ccc2cnccc2c1', '6-methoxyisoquinoline', 'products_text', 1),
            ],
        },
        {
            'family': 'Pomeranz-Fritsch Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Pomeranz-Fritsch variant with halogenated aryl aldehyde.',
            'reactants_text': 'p-chlorobenzaldehyde + aminoacetaldehyde acetal',
            'products_text': '6-chloroisoquinoline',
            'reagents_text': 'H2SO4',
            'conditions_text': 'reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase22_shallow_top10_v1 sprint for Pomeranz-Fritsch Reaction. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccc(Cl)cc1.NCC(OCC)OCC', 'p-chlorobenzaldehyde + aminoacetaldehyde acetal', 'reactants_text', 1),
                ('product', 'Clc1ccc2cnccc2c1', '6-chloroisoquinoline', 'products_text', 1),
            ],
        },
        # === Prilezhaev Reaction ===
        {
            'family': 'Prilezhaev Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Prilezhaev reaction: alkene + peroxyacid gives epoxide.',
            'reactants_text': 'styrene',
            'products_text': 'styrene oxide (2-phenyloxirane)',
            'reagents_text': 'mCPBA',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase22_shallow_top10_v1 sprint for Prilezhaev Reaction. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=Cc1ccccc1', 'styrene', 'reactants_text', 1),
                ('product', 'C1OC1c1ccccc1', 'styrene oxide (2-phenyloxirane)', 'products_text', 1),
            ],
        },
        {
            'family': 'Prilezhaev Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Prilezhaev epoxidation of internal alkene.',
            'reactants_text': '2-butene',
            'products_text': '2,3-epoxybutane',
            'reagents_text': 'mCPBA or CF3CO3H',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase22_shallow_top10_v1 sprint for Prilezhaev Reaction. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC=CC', '2-butene', 'reactants_text', 1),
                ('product', 'CC1OC1C', '2,3-epoxybutane', 'products_text', 1),
            ],
        },
        {
            'family': 'Prilezhaev Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Prilezhaev epoxidation of cycloalkene.',
            'reactants_text': 'cyclohexene',
            'products_text': 'cyclohexene oxide',
            'reagents_text': 'peracetic acid',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase22_shallow_top10_v1 sprint for Prilezhaev Reaction. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C1=CCCCC1', 'cyclohexene', 'reactants_text', 1),
                ('product', 'C2CC3OC3CC2', 'cyclohexene oxide', 'products_text', 1),
            ],
        },
        # === Prins Reaction ===
        {
            'family': 'Prins Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Prins reaction: alkene + aldehyde under acid gives 1,3-dioxane/tetrahydropyran.',
            'reactants_text': 'diallyl ether + formaldehyde (simplified)',
            'products_text': '1,3-dioxane (Prins product)',
            'reagents_text': 'HCl, H2O',
            'conditions_text': 'H2O/dioxane, 80°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase22_shallow_top10_v1 sprint for Prins Reaction. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCC=C.O=C', 'diallyl ether + formaldehyde (simplified)', 'reactants_text', 1),
                ('product', 'OCCC1CC(CO)C1', '1,3-dioxane (Prins product)', 'products_text', 1),
            ],
        },
        {
            'family': 'Prins Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Prins reaction yielding homoallylic alcohol/1,3-diol.',
            'reactants_text': 'propene + acetaldehyde',
            'products_text': '1,3-diol',
            'reagents_text': 'H2SO4 (cat.)',
            'conditions_text': 'H2O, 60°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase22_shallow_top10_v1 sprint for Prins Reaction. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC=C.O=CC', 'propene + acetaldehyde', 'reactants_text', 1),
                ('product', 'OCC(C)C(C)O', '1,3-diol', 'products_text', 1),
            ],
        },
        {
            'family': 'Prins Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Prins addition of vinylcycloalkane to aryl aldehyde.',
            'reactants_text': 'vinylcyclohexane + benzaldehyde',
            'products_text': '1,3-diol',
            'reagents_text': 'BF3·OEt2',
            'conditions_text': 'CH2Cl2, -20°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase22_shallow_top10_v1 sprint for Prins Reaction. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CC1CCCCC1.O=Cc1ccccc1', 'vinylcyclohexane + benzaldehyde', 'reactants_text', 1),
                ('product', 'OC(c1ccccc1)CC(O)C2CCCCC2', '1,3-diol', 'products_text', 1),
            ],
        },
        # === Prins-Pinacol Rearrangement ===
        {
            'family': 'Prins-Pinacol Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Prins-Pinacol: alkene addition followed by 1,2-shift of hydroxyl → carbonyl formation.',
            'reactants_text': 'allyl 1,2-diol',
            'products_text': 'Prins-pinacol rearranged ketone',
            'reagents_text': 'BF3·OEt2, SnCl4, or TfOH',
            'conditions_text': 'CH2Cl2, -78→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase22_shallow_top10_v1 sprint for Prins-Pinacol Rearrangement. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCC(O)C(O)C', 'allyl 1,2-diol', 'reactants_text', 1),
                ('product', 'O=CC(C)C(CCC)', 'Prins-pinacol rearranged ketone', 'products_text', 1),
            ],
        },
        {
            'family': 'Prins-Pinacol Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Prins-pinacol ring-expansion rearrangement.',
            'reactants_text': 'tertiary allyl diol',
            'products_text': 'ring-expanded ketone',
            'reagents_text': 'Sc(OTf)3',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase22_shallow_top10_v1 sprint for Prins-Pinacol Rearrangement. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(O)(C=C)C(O)C1CCCCC1', 'tertiary allyl diol', 'reactants_text', 1),
                ('product', 'O=C(C)CC(C)(CC=C)C2CCCCC2', 'ring-expanded ketone', 'products_text', 1),
            ],
        },
        {
            'family': 'Prins-Pinacol Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Prins-pinacol giving aryl ketone via [1,2]-alkyl shift.',
            'reactants_text': 'homoallyl diol with aryl group',
            'products_text': 'aryl ketone',
            'reagents_text': 'BF3·OEt2',
            'conditions_text': 'CH2Cl2, -78°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase22_shallow_top10_v1 sprint for Prins-Pinacol Rearrangement. [phase22_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C=C)C(O)(C)c1ccccc1', 'homoallyl diol with aryl group', 'reactants_text', 1),
                ('product', 'O=C(C)CC(CC=C)c1ccccc1', 'aryl ketone', 'products_text', 1),
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
