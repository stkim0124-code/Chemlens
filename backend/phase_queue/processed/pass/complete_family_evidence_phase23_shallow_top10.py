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

TAG = 'phase23_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Prévost Reaction',
        'Pummerer Rearrangement',
        'Quasi-Favorskii Rearrangement',
        'Ramberg-Bäcklund Rearrangement',
        'Reformatsky Reaction',
    ],
    'b': [
        'Regitz Diazo Transfer',
        'Reimer-Tiemann Reaction',
        'Retro-Claisen Reaction',
        'Riley Selenium Dioxide Oxidation',
        'Ring-Closing Alkyne Metathesis',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Prévost Reaction ===
        {
            'family': 'Prévost Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Prévost reaction: alkene + I2/AgOBz gives trans-diol after hydrolysis (anti-dihydroxylation).',
            'reactants_text': 'styrene',
            'products_text': 'anti-1-phenylethane-1,2-diol (anti-diol)',
            'reagents_text': 'I2, AgOBz (silver benzoate), H2O workup',
            'conditions_text': 'benzene/H2O, 80°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase23_shallow_top10_v1 sprint for Prévost Reaction. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=Cc1ccccc1', 'styrene', 'reactants_text', 1),
                ('product', 'OC(c1ccccc1)C(O)CO', 'anti-1-phenylethane-1,2-diol (anti-diol)', 'products_text', 1),
            ],
        },
        {
            'family': 'Prévost Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Prévost reaction on cyclohexene giving trans-diol.',
            'reactants_text': 'cyclohexene',
            'products_text': 'trans-cyclohexane-1,2-diol',
            'reagents_text': 'I2, silver benzoate, hydrolysis',
            'conditions_text': 'C6H6, Δ',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase23_shallow_top10_v1 sprint for Prévost Reaction. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C1=CCCCC1', 'cyclohexene', 'reactants_text', 1),
                ('product', 'OC2CCCCC2O', 'trans-cyclohexane-1,2-diol', 'products_text', 1),
            ],
        },
        {
            'family': 'Prévost Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Prévost anti-dihydroxylation of β-methylstyrene.',
            'reactants_text': 'β-methylstyrene',
            'products_text': 'anti-1-phenyl-1,2-propanediol',
            'reagents_text': 'I2, AgOBz',
            'conditions_text': 'benzene, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase23_shallow_top10_v1 sprint for Prévost Reaction. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C(=CC)c1ccccc1', 'β-methylstyrene', 'reactants_text', 1),
                ('product', 'OC(C)C(O)c1ccccc1', 'anti-1-phenyl-1,2-propanediol', 'products_text', 1),
            ],
        },
        # === Pummerer Rearrangement ===
        {
            'family': 'Pummerer Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Pummerer rearrangement: sulfoxide + Ac2O gives α-acyloxy sulfide via sulfur ylide intermediate.',
            'reactants_text': 'ethyl methyl sulfoxide',
            'products_text': 'α-acetoxy sulfide',
            'reagents_text': 'Ac2O',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase23_shallow_top10_v1 sprint for Pummerer Rearrangement. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CS(=O)CC', 'ethyl methyl sulfoxide', 'reactants_text', 1),
                ('product', 'CSC(C)OC(=O)C', 'α-acetoxy sulfide', 'products_text', 1),
            ],
        },
        {
            'family': 'Pummerer Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Pummerer of aryl alkyl sulfoxide with Ac2O.',
            'reactants_text': 'ethyl phenyl sulfoxide',
            'products_text': 'α-acetoxy phenyl ethyl sulfide',
            'reagents_text': 'Ac2O, TsOH',
            'conditions_text': 'CHCl3, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase23_shallow_top10_v1 sprint for Pummerer Rearrangement. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccc(S(=O)CC)cc1', 'ethyl phenyl sulfoxide', 'reactants_text', 1),
                ('product', 'c1ccc(SC(CC)OC(=O)C)cc1', 'α-acetoxy phenyl ethyl sulfide', 'products_text', 1),
            ],
        },
        {
            'family': 'Pummerer Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Pummerer reaction of α-activated sulfoxide.',
            'reactants_text': 'α-cyano sulfoxide + Ac2O',
            'products_text': 'α-acetoxy α-cyano sulfide',
            'reagents_text': 'Ac2O',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase23_shallow_top10_v1 sprint for Pummerer Rearrangement. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=S(CC#N)c1ccccc1', 'α-cyano sulfoxide + Ac2O', 'reactants_text', 1),
                ('product', 'N#CC(OC(=O)C)Sc1ccccc1', 'α-acetoxy α-cyano sulfide', 'products_text', 1),
            ],
        },
        # === Quasi-Favorskii Rearrangement ===
        {
            'family': 'Quasi-Favorskii Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Quasi-Favorskii: α-halo ketone without α-hydrogen adjacent (no cyclopropanone) gives semibenzilic-type rearrangement product.',
            'reactants_text': 'α-chloro-α,α-dimethyl ketone (non-enolizable)',
            'products_text': 'pivalaldehyde / rearranged carbonyl',
            'reagents_text': 'NaOH, Δ',
            'conditions_text': 'MeOH/H2O reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase23_shallow_top10_v1 sprint for Quasi-Favorskii Rearrangement. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C(C)C)C(Cl)(C)C', 'α-chloro-α,α-dimethyl ketone (non-enolizable)', 'reactants_text', 1),
                ('product', 'O=C(C)C(C)(C)C', 'pivalaldehyde / rearranged carbonyl', 'products_text', 1),
            ],
        },
        {
            'family': 'Quasi-Favorskii Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Quasi-Favorskii rearrangement delivering homologated carboxylic acid from non-enolizable α-chloroketone.',
            'reactants_text': 'chloropinacolone',
            'products_text': 'pivalic acid',
            'reagents_text': 'NaOH(aq)',
            'conditions_text': 'MeOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase23_shallow_top10_v1 sprint for Quasi-Favorskii Rearrangement. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C(Cl)(C)C)C', 'chloropinacolone', 'reactants_text', 1),
                ('product', 'O=C(O)C(C)(C)C', 'pivalic acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Quasi-Favorskii Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Quasi-Favorskii semi-benzilic rearrangement to ring-contracted acid.',
            'reactants_text': '2-chloro-2-methylcyclopentanone',
            'products_text': 'cyclopentanecarboxylic acid',
            'reagents_text': 'NaOH',
            'conditions_text': 'aq., reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase23_shallow_top10_v1 sprint for Quasi-Favorskii Rearrangement. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CC(Cl)(C)CC1', '2-chloro-2-methylcyclopentanone', 'reactants_text', 1),
                ('product', 'OC(=O)C1CCCC1', 'cyclopentanecarboxylic acid', 'products_text', 1),
            ],
        },
        # === Ramberg-Bäcklund Rearrangement ===
        {
            'family': 'Ramberg-Bäcklund Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Ramberg-Bäcklund: α-halo sulfone loses SO2 via thiirane-1,1-dioxide intermediate giving alkene.',
            'reactants_text': 'methyl α-chloroethyl sulfone',
            'products_text': '2-butene',
            'reagents_text': 'KOH, CCl4/t-BuOH',
            'conditions_text': 'rt, 12 h',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase23_shallow_top10_v1 sprint for Ramberg-Bäcklund Rearrangement. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CS(=O)(=O)CC(Cl)C', 'methyl α-chloroethyl sulfone', 'reactants_text', 1),
                ('product', 'CC=CC', '2-butene', 'products_text', 1),
            ],
        },
        {
            'family': 'Ramberg-Bäcklund Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Ramberg-Bäcklund olefination giving aryl alkene.',
            'reactants_text': 'α-chloro benzyl methyl sulfone',
            'products_text': 'styrene',
            'reagents_text': 'KOH, CCl4/t-BuOH',
            'conditions_text': 'rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase23_shallow_top10_v1 sprint for Ramberg-Bäcklund Rearrangement. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'ClCS(=O)(=O)Cc1ccccc1', 'α-chloro benzyl methyl sulfone', 'reactants_text', 1),
                ('product', 'C=Cc1ccccc1', 'styrene', 'products_text', 1),
            ],
        },
        {
            'family': 'Ramberg-Bäcklund Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Ramberg-Bäcklund for internal alkene synthesis.',
            'reactants_text': "α-chloro-α,α'-dimethylethyl sulfone",
            'products_text': '4-methyl-2-pentene',
            'reagents_text': 'KOH, CCl4/t-BuOH, Al2O3',
            'conditions_text': 'rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase23_shallow_top10_v1 sprint for Ramberg-Bäcklund Rearrangement. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'ClC(C)S(=O)(=O)C(C)C', "α-chloro-α,α'-dimethylethyl sulfone", 'reactants_text', 1),
                ('product', 'CC=CC(C)C', '4-methyl-2-pentene', 'products_text', 1),
            ],
        },
        # === Reformatsky Reaction ===
        {
            'family': 'Reformatsky Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Reformatsky reaction: α-halo ester + Zn → zinc enolate adds to aldehyde giving β-hydroxy ester.',
            'reactants_text': 'ethyl bromoacetate + benzaldehyde',
            'products_text': 'ethyl β-hydroxy-β-phenyl propionate',
            'reagents_text': 'Zn (activated), ethyl bromoacetate',
            'conditions_text': 'benzene/Et2O reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase23_shallow_top10_v1 sprint for Reformatsky Reaction. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'BrCC(=O)OCC.O=Cc1ccccc1', 'ethyl bromoacetate + benzaldehyde', 'reactants_text', 1),
                ('product', 'OC(CC(=O)OCC)c1ccccc1', 'ethyl β-hydroxy-β-phenyl propionate', 'products_text', 1),
            ],
        },
        {
            'family': 'Reformatsky Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Reformatsky with ketone acceptor.',
            'reactants_text': 'ethyl bromoacetate + acetone',
            'products_text': 'ethyl β-hydroxy-β,β-dimethyl propionate',
            'reagents_text': 'Zn',
            'conditions_text': 'THF, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase23_shallow_top10_v1 sprint for Reformatsky Reaction. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'BrCC(=O)OCC.O=C(C)C', 'ethyl bromoacetate + acetone', 'reactants_text', 1),
                ('product', 'OC(C)(CC(=O)OCC)C', 'ethyl β-hydroxy-β,β-dimethyl propionate', 'products_text', 1),
            ],
        },
        {
            'family': 'Reformatsky Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Reformatsky of α,α-disubstituted bromoester with aryl aldehyde.',
            'reactants_text': 'ethyl 2-bromopropanoate + benzaldehyde',
            'products_text': 'ethyl 3-hydroxy-2-methyl-3-phenyl-propanoate',
            'reagents_text': 'Zn, I2 activator',
            'conditions_text': 'THF, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase23_shallow_top10_v1 sprint for Reformatsky Reaction. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'BrC(C)C(=O)OCC.O=Cc1ccccc1', 'ethyl 2-bromopropanoate + benzaldehyde', 'reactants_text', 1),
                ('product', 'OC(c1ccccc1)C(C)C(=O)OCC', 'ethyl 3-hydroxy-2-methyl-3-phenyl-propanoate', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Regitz Diazo Transfer ===
        {
            'family': 'Regitz Diazo Transfer',
            'extract_kind': 'application_example',
            'transformation_text': 'Regitz diazo transfer: active methylene compound + sulfonyl azide gives α-diazo carbonyl.',
            'reactants_text': 'diethyl malonate',
            'products_text': 'diethyl 2-diazomalonate',
            'reagents_text': 'p-ABSA (4-acetamidobenzenesulfonyl azide), Et3N',
            'conditions_text': 'CH3CN, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase23_shallow_top10_v1 sprint for Regitz Diazo Transfer. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)CC(=O)OCC', 'diethyl malonate', 'reactants_text', 1),
                ('product', 'CCOC(=O)C(=[N+]=[N-])C(=O)OCC', 'diethyl 2-diazomalonate', 'products_text', 1),
            ],
        },
        {
            'family': 'Regitz Diazo Transfer',
            'extract_kind': 'application_example',
            'transformation_text': 'Regitz diazo transfer onto 1,3-dicarbonyl.',
            'reactants_text': '1-benzoyl-propan-2-one (1,3-dicarbonyl)',
            'products_text': 'α-diazo dicarbonyl',
            'reagents_text': 'MsN3, Et3N',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase23_shallow_top10_v1 sprint for Regitz Diazo Transfer. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(CC(=O)C)c1ccccc1', '1-benzoyl-propan-2-one (1,3-dicarbonyl)', 'reactants_text', 1),
                ('product', 'O=C(C(=[N+]=[N-])C(=O)C)c1ccccc1', 'α-diazo dicarbonyl', 'products_text', 1),
            ],
        },
        {
            'family': 'Regitz Diazo Transfer',
            'extract_kind': 'application_example',
            'transformation_text': 'Regitz diazo transfer on simple ester (trifluoromethanesulfonyl azide).',
            'reactants_text': 'ethyl butanoate',
            'products_text': 'ethyl 2-diazobutanoate',
            'reagents_text': 'TfN3, DBU',
            'conditions_text': 'CH3CN, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase23_shallow_top10_v1 sprint for Regitz Diazo Transfer. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)CC', 'ethyl butanoate', 'reactants_text', 1),
                ('product', 'CCOC(=O)C(CC)=[N+]=[N-]', 'ethyl 2-diazobutanoate', 'products_text', 1),
            ],
        },
        # === Reimer-Tiemann Reaction ===
        {
            'family': 'Reimer-Tiemann Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Reimer-Tiemann: phenolate + CHCl3/NaOH → dichlorocarbene → ortho-formylation gives salicylaldehyde after hydrolysis.',
            'reactants_text': 'phenol',
            'products_text': 'salicylaldehyde',
            'reagents_text': 'CHCl3, NaOH',
            'conditions_text': 'aq. NaOH, 60°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase23_shallow_top10_v1 sprint for Reimer-Tiemann Reaction. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Oc1ccccc1', 'phenol', 'reactants_text', 1),
                ('product', 'O=Cc1ccccc1O', 'salicylaldehyde', 'products_text', 1),
            ],
        },
        {
            'family': 'Reimer-Tiemann Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Reimer-Tiemann on p-cresol gives ortho-aldehyde.',
            'reactants_text': 'p-cresol',
            'products_text': '2-hydroxy-5-methylbenzaldehyde',
            'reagents_text': 'CHCl3, KOH',
            'conditions_text': 'aq., reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase23_shallow_top10_v1 sprint for Reimer-Tiemann Reaction. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Oc1ccc(C)cc1', 'p-cresol', 'reactants_text', 1),
                ('product', 'Cc1ccc(O)c(C=O)c1', '2-hydroxy-5-methylbenzaldehyde', 'products_text', 1),
            ],
        },
        {
            'family': 'Reimer-Tiemann Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Reimer-Tiemann formylation of indole (abnormal site, α-CH of pyrrole).',
            'reactants_text': 'indole',
            'products_text': 'indole-2-carbaldehyde',
            'reagents_text': 'CHCl3, NaOH',
            'conditions_text': 'aq. NaOH, heat',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase23_shallow_top10_v1 sprint for Reimer-Tiemann Reaction. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccc2[nH]ccc2c1', 'indole', 'reactants_text', 1),
                ('product', 'c1ccc2[nH]c(C=O)cc2c1', 'indole-2-carbaldehyde', 'products_text', 1),
            ],
        },
        # === Retro-Claisen Reaction ===
        {
            'family': 'Retro-Claisen Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Retro-Claisen: β-keto ester fragmented by alkoxide gives two ester/alcohol fragments (reverse Claisen).',
            'reactants_text': 'diethyl malonate (under strong base)',
            'products_text': 'ethyl acetate + ethanol',
            'reagents_text': 'NaOEt, EtOH (excess)',
            'conditions_text': 'EtOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase23_shallow_top10_v1 sprint for Retro-Claisen Reaction. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)CC(=O)OCC', 'diethyl malonate (under strong base)', 'reactants_text', 1),
                ('product', 'CCOC(=O)C.CCO', 'ethyl acetate + ethanol', 'products_text', 1),
            ],
        },
        {
            'family': 'Retro-Claisen Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Retro-Claisen of β-ketoester under basic conditions.',
            'reactants_text': 'ethyl 3-oxo-3-phenyl-propanoate',
            'products_text': 'ethyl acetate + benzaldehyde (cleavage)',
            'reagents_text': 'NaOH (aq.)',
            'conditions_text': 'EtOH/H2O, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase23_shallow_top10_v1 sprint for Retro-Claisen Reaction. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)CC(=O)c1ccccc1', 'ethyl 3-oxo-3-phenyl-propanoate', 'reactants_text', 1),
                ('product', 'CCOC(=O)C.O=Cc1ccccc1', 'ethyl acetate + benzaldehyde (cleavage)', 'products_text', 1),
            ],
        },
        {
            'family': 'Retro-Claisen Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Retro-Claisen cleavage of β-diketone in presence of alkoxide.',
            'reactants_text': '3-hexanone-1-one (β-diketone)',
            'products_text': 'acetic acid + pentan-3-one',
            'reagents_text': 'NaOEt',
            'conditions_text': 'EtOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase23_shallow_top10_v1 sprint for Retro-Claisen Reaction. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(=O)CC(=O)CC', '3-hexanone-1-one (β-diketone)', 'reactants_text', 1),
                ('product', 'CC(=O)O.CCC(=O)CC', 'acetic acid + pentan-3-one', 'products_text', 1),
            ],
        },
        # === Riley Selenium Dioxide Oxidation ===
        {
            'family': 'Riley Selenium Dioxide Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Riley SeO2 oxidation: allylic C-H oxidized to aldehyde at terminal methyl.',
            'reactants_text': '2-butene',
            'products_text': 'crotonaldehyde',
            'reagents_text': 'SeO2 (cat.), t-BuOOH',
            'conditions_text': 'dioxane, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase23_shallow_top10_v1 sprint for Riley Selenium Dioxide Oxidation. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC=CC', '2-butene', 'reactants_text', 1),
                ('product', 'O=CC=CC', 'crotonaldehyde', 'products_text', 1),
            ],
        },
        {
            'family': 'Riley Selenium Dioxide Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Riley allylic oxidation of α-methyl alkene.',
            'reactants_text': '2-methyl-1-butene',
            'products_text': '2-methyl-butenal',
            'reagents_text': 'SeO2, TBHP',
            'conditions_text': 'dioxane, 80°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase23_shallow_top10_v1 sprint for Riley Selenium Dioxide Oxidation. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(=C)CC', '2-methyl-1-butene', 'reactants_text', 1),
                ('product', 'CC(=C)CC=O', '2-methyl-butenal', 'products_text', 1),
            ],
        },
        {
            'family': 'Riley Selenium Dioxide Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Riley oxidation of allylic methyl group on cyclic alkene to aldehyde.',
            'reactants_text': '1-methylcyclohexene',
            'products_text': 'cyclohex-1-ene-1-carbaldehyde',
            'reagents_text': 'SeO2, TBHP',
            'conditions_text': 'dioxane/CH2Cl2, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase23_shallow_top10_v1 sprint for Riley Selenium Dioxide Oxidation. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC1=CCCCC1', '1-methylcyclohexene', 'reactants_text', 1),
                ('product', 'O=CC1=CCCCC1', 'cyclohex-1-ene-1-carbaldehyde', 'products_text', 1),
            ],
        },
        # === Ring-Closing Alkyne Metathesis ===
        {
            'family': 'Ring-Closing Alkyne Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Ring-closing alkyne metathesis: diyne → cyclic alkyne + small alkyne byproduct via W or Mo alkylidyne catalyst.',
            'reactants_text': '14,16-diyne linear (simplified)',
            'products_text': 'cycloalkyne (cyclodecyne)',
            'reagents_text': 'Mo(CO)6, 4-nitrophenol (Fürstner conditions)',
            'conditions_text': 'chlorobenzene, 140°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase23_shallow_top10_v1 sprint for Ring-Closing Alkyne Metathesis. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC#CCCCCCCCC#CC', '14,16-diyne linear (simplified)', 'reactants_text', 1),
                ('product', 'C1CCCCCCCC#CC1', 'cycloalkyne (cyclodecyne)', 'products_text', 1),
            ],
        },
        {
            'family': 'Ring-Closing Alkyne Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'RCAM to macrocyclic alkyne for natural product scaffolds.',
            'reactants_text': 'longer α,ω-diyne',
            'products_text': 'macrocyclic alkyne',
            'reagents_text': 'Mo alkylidyne catalyst (Fürstner / Schrock)',
            'conditions_text': 'toluene, 80°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase23_shallow_top10_v1 sprint for Ring-Closing Alkyne Metathesis. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC#CCCCCCCCCCC#CC', 'longer α,ω-diyne', 'reactants_text', 1),
                ('product', 'C1CCCCCCCCCC#CC1', 'macrocyclic alkyne', 'products_text', 1),
            ],
        },
        {
            'family': 'Ring-Closing Alkyne Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'RCAM to construct macrolactone-containing alkyne ring.',
            'reactants_text': 'diyne-lactone precursor',
            'products_text': 'cyclic alkyne-lactone (ring-closed)',
            'reagents_text': '[Mo]≡CR catalyst, mol. sieve',
            'conditions_text': 'toluene, 80°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase23_shallow_top10_v1 sprint for Ring-Closing Alkyne Metathesis. [phase23_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC#CCCC(=O)OCCCCC#CC', 'diyne-lactone precursor', 'reactants_text', 1),
                ('product', 'O=C1CCCC#CCCCCO1', 'cyclic alkyne-lactone (ring-closed)', 'products_text', 1),
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
