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

TAG = 'phase34_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Baker-Venkataraman Rearrangement',
        'Barton Radical Decarboxylation',
        'Burgess Dehydration Reaction',
        'Aldol Reaction',
    ],
    'b': [
        'Castro-Stephens Coupling',
        'Bartoli Indole Synthesis',
        'Benzoin and Retro-Benzoin Condensation',
        'Bamford-Stevens-Shapiro Olefination',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Baker-Venkataraman Rearrangement ===
        {
            'family': 'Baker-Venkataraman Rearrangement',
            'extract_kind': 'canonical_overview',
            'transformation_text': 'Baker-Venkataraman rearrangement: 2-acyl-phenol ester → 1,3-diketone via base-mediated intramolecular acyl transfer (key for flavone/chromone synthesis).',
            'reactants_text': '2-propanoyl-phenyl benzoate',
            'products_text': '1-(2-hydroxyphenyl)-2-methyl-3-phenyl-1,3-propanedione (BV-product)',
            'reagents_text': 'KOH (base)',
            'conditions_text': 'pyridine, rt',
            'notes_text': 'Manual curated canonical-overview seed (variant A) added during phase34_shallow_top10_v1 sprint for Baker-Venkataraman Rearrangement. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(OC1=CC=CC=C1C(=O)CC)c2ccccc2', '2-propanoyl-phenyl benzoate', 'reactants_text', 1),
                ('product', 'O=C(CC(=O)c1ccccc1O)c2ccccc2CC', '1-(2-hydroxyphenyl)-2-methyl-3-phenyl-1,3-propanedione (BV-product)', 'products_text', 1),
            ],
        },
        {
            'family': 'Baker-Venkataraman Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Baker-Venkataraman with alpha-branched acyl.',
            'reactants_text': '2-acetyl-phenyl isobutyrate',
            'products_text': '1-(2-hydroxyphenyl)-4-methyl-1,3-pentanedione',
            'reagents_text': 'NaH',
            'conditions_text': 'DMF, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase34_shallow_top10_v1 sprint for Baker-Venkataraman Rearrangement. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(OC1=CC=CC=C1C(=O)C)C(C)C', '2-acetyl-phenyl isobutyrate', 'reactants_text', 1),
                ('product', 'O=C(CC(=O)c1ccccc1O)C(C)C', '1-(2-hydroxyphenyl)-4-methyl-1,3-pentanedione', 'products_text', 1),
            ],
        },
        {
            'family': 'Baker-Venkataraman Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Baker-Venkataraman with 4-Cl-aryl ester.',
            'reactants_text': '2-acetyl-phenyl 4-chlorobenzoate',
            'products_text': '1-(2-hydroxyphenyl)-3-(4-chlorophenyl)-1,3-propanedione',
            'reagents_text': 'KOH',
            'conditions_text': 'pyridine',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase34_shallow_top10_v1 sprint for Baker-Venkataraman Rearrangement. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(OC1=CC=CC=C1C(=O)C)c2ccc(Cl)cc2', '2-acetyl-phenyl 4-chlorobenzoate', 'reactants_text', 1),
                ('product', 'O=C(CC(=O)c1ccccc1O)c2ccc(Cl)cc2', '1-(2-hydroxyphenyl)-3-(4-chlorophenyl)-1,3-propanedione', 'products_text', 1),
            ],
        },
        # === Barton Radical Decarboxylation ===
        {
            'family': 'Barton Radical Decarboxylation',
            'extract_kind': 'canonical_overview',
            'transformation_text': 'Barton radical decarboxylation: carboxylic acid → thiohydroxamate ester → hν gives alkyl radical + CO2; radical trapped by H-donor (tBuSH or Bu3SnH).',
            'reactants_text': 'hexanoic acid',
            'products_text': 'pentane (via decarboxylation radical + H abstract)',
            'reagents_text': 'N-hydroxy-2-thiopyridone ester; hv; tBuSH',
            'conditions_text': 'PhH, rt',
            'notes_text': 'Manual curated canonical-overview seed (variant A) added during phase34_shallow_top10_v1 sprint for Barton Radical Decarboxylation. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(=O)CCCCC', 'hexanoic acid', 'reactants_text', 1),
                ('product', 'CCCCC', 'pentane (via decarboxylation radical + H abstract)', 'products_text', 1),
            ],
        },
        {
            'family': 'Barton Radical Decarboxylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Barton of β-branched acid.',
            'reactants_text': '3,3-dimethyl-butanoic acid (pivalic-type)',
            'products_text': 'neopentane',
            'reagents_text': 'Barton ester, Bu3SnH, AIBN',
            'conditions_text': 'PhH, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase34_shallow_top10_v1 sprint for Barton Radical Decarboxylation. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(=O)CC(C)(C)C', '3,3-dimethyl-butanoic acid (pivalic-type)', 'reactants_text', 1),
                ('product', 'CC(C)(C)C', 'neopentane', 'products_text', 1),
            ],
        },
        {
            'family': 'Barton Radical Decarboxylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Barton decarboxylation of aryl propanoic acid.',
            'reactants_text': '3-(4-fluorophenyl)-propanoic acid',
            'products_text': '4-fluoro-toluene (after decarboxylative H-transfer)',
            'reagents_text': 'Barton ester, tBuSH, hv',
            'conditions_text': 'PhH, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase34_shallow_top10_v1 sprint for Barton Radical Decarboxylation. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(=O)CCc1ccc(F)cc1', '3-(4-fluorophenyl)-propanoic acid', 'reactants_text', 1),
                ('product', 'Cc1ccc(F)cc1', '4-fluoro-toluene (after decarboxylative H-transfer)', 'products_text', 1),
            ],
        },
        # === Burgess Dehydration Reaction ===
        {
            'family': 'Burgess Dehydration Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Burgess dehydration: alcohol + Burgess reagent (Et3N-SO2-NCO2Me) under mild conditions gives alkene via concerted syn-elimination.',
            'reactants_text': '2-phenyl-2-propanol',
            'products_text': 'α-methylstyrene',
            'reagents_text': 'Burgess reagent (Et3N·SO2NCO2Me)',
            'conditions_text': 'THF, 60°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase34_shallow_top10_v1 sprint for Burgess Dehydration Reaction. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)(C)c1ccccc1', '2-phenyl-2-propanol', 'reactants_text', 1),
                ('product', 'C=C(c1ccccc1)C', 'α-methylstyrene', 'products_text', 1),
            ],
        },
        {
            'family': 'Burgess Dehydration Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Burgess dehydration of cyclohexanol derivative.',
            'reactants_text': '2-methyl-cyclohexanol',
            'products_text': '1-methyl-cyclohexene',
            'reagents_text': 'Burgess reagent',
            'conditions_text': 'PhH, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase34_shallow_top10_v1 sprint for Burgess Dehydration Reaction. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC1CCCCC1C', '2-methyl-cyclohexanol', 'reactants_text', 1),
                ('product', 'C1=CCCCC1C', '1-methyl-cyclohexene', 'products_text', 1),
            ],
        },
        {
            'family': 'Burgess Dehydration Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Burgess dehydration of secondary aliphatic alcohol.',
            'reactants_text': '2-butanol',
            'products_text': '(E)-2-butene',
            'reagents_text': 'Burgess reagent',
            'conditions_text': 'PhH, 50°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase34_shallow_top10_v1 sprint for Burgess Dehydration Reaction. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)CC', '2-butanol', 'reactants_text', 1),
                ('product', 'CC=CC', '(E)-2-butene', 'products_text', 1),
            ],
        },
        # === Aldol Reaction ===
        {
            'family': 'Aldol Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Aldol reaction: enolate of ketone + aldehyde gives β-hydroxy carbonyl (aldol adduct).',
            'reactants_text': 'acetone + benzaldehyde',
            'products_text': '4-hydroxy-4-phenyl-2-butanone (β-hydroxy ketone)',
            'reagents_text': 'LDA, then warming',
            'conditions_text': 'THF, -78°C→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase34_shallow_top10_v1 sprint for Aldol Reaction. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)C.O=Cc1ccccc1', 'acetone + benzaldehyde', 'reactants_text', 1),
                ('product', 'OC(c1ccccc1)CC(=O)C', '4-hydroxy-4-phenyl-2-butanone (β-hydroxy ketone)', 'products_text', 1),
            ],
        },
        {
            'family': 'Aldol Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Crossed aldol of ethyl ketone and propanal.',
            'reactants_text': '2-butanone + propanal',
            'products_text': '5-hydroxy-4-methyl-heptan-3-one (crossed aldol)',
            'reagents_text': 'LDA',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase34_shallow_top10_v1 sprint for Aldol Reaction. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(CC)C.O=CCC', '2-butanone + propanal', 'reactants_text', 1),
                ('product', 'OC(CC)CC(=O)CC', '5-hydroxy-4-methyl-heptan-3-one (crossed aldol)', 'products_text', 1),
            ],
        },
        {
            'family': 'Aldol Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Aldol of acetophenone enolate with isobutyraldehyde.',
            'reactants_text': 'acetophenone + isobutyraldehyde',
            'products_text': '3-hydroxy-4-methyl-1-phenyl-1-pentanone',
            'reagents_text': 'LDA',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase34_shallow_top10_v1 sprint for Aldol Reaction. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)c1ccccc1.O=CC(C)C', 'acetophenone + isobutyraldehyde', 'reactants_text', 1),
                ('product', 'OC(C(C)C)CC(=O)c1ccccc1', '3-hydroxy-4-methyl-1-phenyl-1-pentanone', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Castro-Stephens Coupling ===
        {
            'family': 'Castro-Stephens Coupling',
            'extract_kind': 'application_example',
            'transformation_text': "Castro-Stephens: Cu(I) acetylide + aryl halide → aryl alkyne (Sonogashira's precursor, needs stoichiometric Cu).",
            'reactants_text': '4-iodoanisole + phenylacetylene',
            'products_text': '1-methoxy-4-(phenylethynyl)benzene',
            'reagents_text': 'CuI, phenylacetylene, DMF',
            'conditions_text': 'pyridine, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase34_shallow_top10_v1 sprint for Castro-Stephens Coupling. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1ccc(OC)cc1.C#Cc2ccccc2', '4-iodoanisole + phenylacetylene', 'reactants_text', 1),
                ('product', 'c1ccc(C#Cc2ccc(OC)cc2)cc1', '1-methoxy-4-(phenylethynyl)benzene', 'products_text', 1),
            ],
        },
        {
            'family': 'Castro-Stephens Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Castro-Stephens aryl bromide + alkyl alkyne.',
            'reactants_text': '4-bromofluorobenzene + 1-butyne',
            'products_text': '4-fluoro-(1-butynyl)-benzene',
            'reagents_text': 'Cu-acetylide, pyridine',
            'conditions_text': 'pyridine, 120°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase34_shallow_top10_v1 sprint for Castro-Stephens Coupling. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccc(F)cc1.C#CCC', '4-bromofluorobenzene + 1-butyne', 'reactants_text', 1),
                ('product', 'CCC#Cc1ccc(F)cc1', '4-fluoro-(1-butynyl)-benzene', 'products_text', 1),
            ],
        },
        {
            'family': 'Castro-Stephens Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Castro-Stephens on heteroaryl halide.',
            'reactants_text': '3-iodopyridine + 4-methoxy-phenylacetylene',
            'products_text': '3-(4-methoxyphenyl)ethynyl-pyridine',
            'reagents_text': 'CuI, pyridine',
            'conditions_text': 'pyridine, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase34_shallow_top10_v1 sprint for Castro-Stephens Coupling. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1cccnc1.C#Cc2ccc(OC)cc2', '3-iodopyridine + 4-methoxy-phenylacetylene', 'reactants_text', 1),
                ('product', 'COc1ccc(C#Cc2cccnc2)cc1', '3-(4-methoxyphenyl)ethynyl-pyridine', 'products_text', 1),
            ],
        },
        # === Bartoli Indole Synthesis ===
        {
            'family': 'Bartoli Indole Synthesis',
            'extract_kind': 'canonical_overview',
            'transformation_text': 'Bartoli indole synthesis: ortho-substituted nitroarenes + 3 eq vinyl Grignard at low T give 7-substituted indoles. Parent/para substrates give 5-substituted or parent indole. Usefully works at low T (-40°C).',
            'reactants_text': '4-methyl-nitrobenzene (substrate)',
            'products_text': '5-methyl-indole (Bartoli-type)',
            'reagents_text': 'CH2=CHMgBr (3 eq)',
            'conditions_text': 'THF, -40°C→rt',
            'notes_text': 'Manual curated canonical-overview seed (variant A) added during phase34_shallow_top10_v1 sprint for Bartoli Indole Synthesis. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=[N+]([O-])c1ccc(C)cc1', '4-methyl-nitrobenzene (substrate)', 'reactants_text', 1),
                ('product', 'Cc1ccc2[nH]ccc2c1', '5-methyl-indole (Bartoli-type)', 'products_text', 1),
            ],
        },
        {
            'family': 'Bartoli Indole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Bartoli: ortho-ethyl nitrobenzene gives 7-ethyl-indole.',
            'reactants_text': '2-ethyl-nitrobenzene',
            'products_text': '7-ethyl-indole',
            'reagents_text': 'vinyl-MgBr (3 eq)',
            'conditions_text': 'THF, -40°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase34_shallow_top10_v1 sprint for Bartoli Indole Synthesis. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCc1ccccc1[N+](=O)[O-]', '2-ethyl-nitrobenzene', 'reactants_text', 1),
                ('product', 'CCc1ccc2[nH]ccc2c1', '7-ethyl-indole', 'products_text', 1),
            ],
        },
        {
            'family': 'Bartoli Indole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Bartoli on 2-chloronitrobenzene.',
            'reactants_text': '2-chloronitrobenzene',
            'products_text': '7-chloroindole',
            'reagents_text': 'vinylMgBr (3 eq)',
            'conditions_text': 'THF, -40°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase34_shallow_top10_v1 sprint for Bartoli Indole Synthesis. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Clc1ccccc1[N+](=O)[O-]', '2-chloronitrobenzene', 'reactants_text', 1),
                ('product', 'Clc1ccc2[nH]ccc2c1', '7-chloroindole', 'products_text', 1),
            ],
        },
        # === Benzoin and Retro-Benzoin Condensation ===
        {
            'family': 'Benzoin and Retro-Benzoin Condensation',
            'extract_kind': 'canonical_overview',
            'transformation_text': 'Benzoin condensation: aromatic aldehyde dimerizes under cyanide (or NHC) catalysis via acyl-anion (Umpolung) to give α-hydroxy ketone. Retro-benzoin is the reverse.',
            'reactants_text': 'benzaldehyde (2 eq)',
            'products_text': 'benzoin (2-hydroxy-1,2-diphenyl-ethanone)',
            'reagents_text': 'KCN (cat.), or NHC',
            'conditions_text': 'EtOH/H2O, reflux',
            'notes_text': 'Manual curated canonical-overview seed (variant A) added during phase34_shallow_top10_v1 sprint for Benzoin and Retro-Benzoin Condensation. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1', 'benzaldehyde (2 eq)', 'reactants_text', 1),
                ('product', 'OC(c1ccccc1)C(=O)c2ccccc2', 'benzoin (2-hydroxy-1,2-diphenyl-ethanone)', 'products_text', 1),
            ],
        },
        {
            'family': 'Benzoin and Retro-Benzoin Condensation',
            'extract_kind': 'application_example',
            'transformation_text': 'Benzoin of electron-poor aryl aldehyde.',
            'reactants_text': '4-chlorobenzaldehyde',
            'products_text': "4,4'-dichloro-benzoin",
            'reagents_text': 'NaCN',
            'conditions_text': 'EtOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase34_shallow_top10_v1 sprint for Benzoin and Retro-Benzoin Condensation. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccc(Cl)cc1', '4-chlorobenzaldehyde', 'reactants_text', 1),
                ('product', 'OC(c1ccc(Cl)cc1)C(=O)c2ccc(Cl)cc2', "4,4'-dichloro-benzoin", 'products_text', 1),
            ],
        },
        {
            'family': 'Benzoin and Retro-Benzoin Condensation',
            'extract_kind': 'application_example',
            'transformation_text': 'Benzoin of electron-rich aryl aldehyde.',
            'reactants_text': '4-methoxybenzaldehyde',
            'products_text': "4,4'-dimethoxy-benzoin (anisoin)",
            'reagents_text': 'NHC cat. (IMes·HCl)',
            'conditions_text': 'THF, 60°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase34_shallow_top10_v1 sprint for Benzoin and Retro-Benzoin Condensation. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccc(OC)cc1', '4-methoxybenzaldehyde', 'reactants_text', 1),
                ('product', 'OC(c1ccc(OC)cc1)C(=O)c2ccc(OC)cc2', "4,4'-dimethoxy-benzoin (anisoin)", 'products_text', 1),
            ],
        },
        # === Bamford-Stevens-Shapiro Olefination ===
        {
            'family': 'Bamford-Stevens-Shapiro Olefination',
            'extract_kind': 'application_example',
            'transformation_text': 'Shapiro: ketone → tosylhydrazone → 2 eq BuLi → vinyllithium → H2O gives less-substituted alkene (thermodynamic vs Bamford-Stevens gives more-substituted).',
            'reactants_text': '2-hexanone (via tosylhydrazone)',
            'products_text': '1-hexene (Shapiro olefination product)',
            'reagents_text': 'TsNHNH2; 2 eq nBuLi; H2O',
            'conditions_text': 'THF/TMEDA, -78°C→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase34_shallow_top10_v1 sprint for Bamford-Stevens-Shapiro Olefination. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)CCCC', '2-hexanone (via tosylhydrazone)', 'reactants_text', 1),
                ('product', 'C=CCCCC', '1-hexene (Shapiro olefination product)', 'products_text', 1),
            ],
        },
        {
            'family': 'Bamford-Stevens-Shapiro Olefination',
            'extract_kind': 'application_example',
            'transformation_text': 'Shapiro/Bamford-Stevens of cyclohexanone via tosylhydrazone.',
            'reactants_text': 'cyclohexanone',
            'products_text': 'cyclohexene',
            'reagents_text': 'TsNHNH2; BuLi',
            'conditions_text': 'hexane',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase34_shallow_top10_v1 sprint for Bamford-Stevens-Shapiro Olefination. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCCC1', 'cyclohexanone', 'reactants_text', 1),
                ('product', 'C1=CCCCC1', 'cyclohexene', 'products_text', 1),
            ],
        },
        {
            'family': 'Bamford-Stevens-Shapiro Olefination',
            'extract_kind': 'application_example',
            'transformation_text': 'Bamford-Stevens of acetophenone via tosylhydrazone/base (aprotic Δ).',
            'reactants_text': 'acetophenone',
            'products_text': 'styrene',
            'reagents_text': 'TsNHNH2; NaOMe',
            'conditions_text': 'DMSO, 100°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase34_shallow_top10_v1 sprint for Bamford-Stevens-Shapiro Olefination. [phase34_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(c1ccccc1)C', 'acetophenone', 'reactants_text', 1),
                ('product', 'C=Cc1ccccc1', 'styrene', 'products_text', 1),
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
