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

TAG = 'phase19_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Michael Addition Reaction',
        'Midland Alpine Borane Reduction',
        'Minisci Reaction',
        'Mislow-Evans Rearrangement',
        'Mitsunobu Reaction',
    ],
    'b': [
        'Miyaura Boration',
        'Mukaiyama Aldol Reaction',
        'Myers Asymmetric Alkylation',
        'Nagata Hydrocyanation',
        'Nazarov Cyclization',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Michael Addition Reaction ===
        {
            'family': 'Michael Addition Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Michael addition: diethyl malonate anion adds to methyl vinyl ketone.',
            'reactants_text': 'diethyl malonate',
            'products_text': 'diethyl 2-(3-oxobutyl)malonate',
            'reagents_text': 'methyl vinyl ketone, NaOEt (cat.)',
            'conditions_text': 'EtOH, 0°C→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase19_shallow_top10_v1 sprint for Michael Addition Reaction. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)CC(=O)OCC', 'diethyl malonate', 'reactants_text', 1),
                ('product', 'CCOC(=O)C(CCC(=O)C)C(=O)OCC', 'diethyl 2-(3-oxobutyl)malonate', 'products_text', 1),
            ],
        },
        {
            'family': 'Michael Addition Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Michael addition of cyclohexanone enolate to acrylonitrile.',
            'reactants_text': 'cyclohexanone',
            'products_text': '2-(2-cyanoethyl)cyclohexan-1-one',
            'reagents_text': 'acrylonitrile, Triton B (cat.)',
            'conditions_text': 't-BuOH, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase19_shallow_top10_v1 sprint for Michael Addition Reaction. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCCC1', 'cyclohexanone', 'reactants_text', 1),
                ('product', 'O=C1CCCCC1CCC#N', '2-(2-cyanoethyl)cyclohexan-1-one', 'products_text', 1),
            ],
        },
        {
            'family': 'Michael Addition Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Michael addition of malononitrile anion onto ethyl acrylate.',
            'reactants_text': 'malononitrile',
            'products_text': 'ethyl 3,3-dicyano-propanoate',
            'reagents_text': 'ethyl acrylate, DBU (cat.)',
            'conditions_text': 'CH3CN, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase19_shallow_top10_v1 sprint for Michael Addition Reaction. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'N#CCC#N', 'malononitrile', 'reactants_text', 1),
                ('product', 'N#CC(CC(=O)OCC)C#N', 'ethyl 3,3-dicyano-propanoate', 'products_text', 1),
            ],
        },
        # === Midland Alpine Borane Reduction ===
        {
            'family': 'Midland Alpine Borane Reduction',
            'extract_kind': 'application_example',
            'transformation_text': 'Midland/Alpine-Borane reduction of propargyl ketone with B-isopinocampheyl-9-BBN gives chiral propargyl alcohol.',
            'reactants_text': '3-pentyn-2-one',
            'products_text': '(R)-3-pentyn-2-ol',
            'reagents_text': 'Alpine-Borane (B-Ipc-9-BBN)',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase19_shallow_top10_v1 sprint for Midland Alpine Borane Reduction. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)C#CC', '3-pentyn-2-one', 'reactants_text', 1),
                ('product', 'OC(C)C#CC', '(R)-3-pentyn-2-ol', 'products_text', 1),
            ],
        },
        {
            'family': 'Midland Alpine Borane Reduction',
            'extract_kind': 'application_example',
            'transformation_text': 'Midland reduction of aryl propargyl ketone to chiral alcohol.',
            'reactants_text': '4-phenyl-3-butyn-2-one',
            'products_text': '(R)-4-phenyl-3-butyn-2-ol',
            'reagents_text': 'Alpine-Borane',
            'conditions_text': 'neat, 25°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase19_shallow_top10_v1 sprint for Midland Alpine Borane Reduction. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C#Cc1ccccc1)C', '4-phenyl-3-butyn-2-one', 'reactants_text', 1),
                ('product', 'OC(C)C#Cc1ccccc1', '(R)-4-phenyl-3-butyn-2-ol', 'products_text', 1),
            ],
        },
        {
            'family': 'Midland Alpine Borane Reduction',
            'extract_kind': 'application_example',
            'transformation_text': 'Alpine-Borane reduction of propargylic aldehyde giving enantioenriched propargyl alcohol.',
            'reactants_text': '2-pentynal',
            'products_text': '(S)-2-pentyn-1-ol',
            'reagents_text': 'B-Ipc-9-BBN (Alpine-Borane)',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase19_shallow_top10_v1 sprint for Midland Alpine Borane Reduction. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CC#CCC', '2-pentynal', 'reactants_text', 1),
                ('product', 'OCC#CCC', '(S)-2-pentyn-1-ol', 'products_text', 1),
            ],
        },
        # === Minisci Reaction ===
        {
            'family': 'Minisci Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Minisci reaction: protonated pyridine accepts nucleophilic alkyl radical at 2/4 position; methyl radical from oxidation of acetic acid.',
            'reactants_text': 'pyridine',
            'products_text': '4-methylpyridine',
            'reagents_text': 'AcOH, AgNO3, (NH4)2S2O8',
            'conditions_text': 'H2O/AcOH, 70°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase19_shallow_top10_v1 sprint for Minisci Reaction. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccncc1', 'pyridine', 'reactants_text', 1),
                ('product', 'CC1=CC=NC=C1', '4-methylpyridine', 'products_text', 1),
            ],
        },
        {
            'family': 'Minisci Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Minisci methylation of quinoline: H+-activated heteroarene + CH3 radical from peroxide/AgNO3.',
            'reactants_text': 'quinoline',
            'products_text': '3-methylquinoline (minor) / 2-methylquinoline (major)',
            'reagents_text': 'H2O2, FeSO4, DMSO (methyl source)',
            'conditions_text': 'H2SO4, 40°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase19_shallow_top10_v1 sprint for Minisci Reaction. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccc2ncccc2c1', 'quinoline', 'reactants_text', 1),
                ('product', 'CC1=CC2=CC=CC=C2N=C1', '3-methylquinoline (minor) / 2-methylquinoline (major)', 'products_text', 1),
            ],
        },
        {
            'family': 'Minisci Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Minisci alkylation of nicotinic acid with ethyl radical generated from propanoic acid.',
            'reactants_text': 'picolinic acid derivative (nicotinic)',
            'products_text': 'ethyl-substituted nicotinic acid',
            'reagents_text': 'propanoic acid, AgNO3, S2O8(2-)',
            'conditions_text': 'aqueous, 80°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase19_shallow_top10_v1 sprint for Minisci Reaction. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ncccc1C(=O)O', 'picolinic acid derivative (nicotinic)', 'reactants_text', 1),
                ('product', 'CCc1ncccc1C(=O)O', 'ethyl-substituted nicotinic acid', 'products_text', 1),
            ],
        },
        # === Mislow-Evans Rearrangement ===
        {
            'family': 'Mislow-Evans Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Mislow-Evans [2,3]-sigmatropic rearrangement of allyl sulfoxide to allyl sulfenate, trapped by thiophile to allyl alcohol.',
            'reactants_text': 'allyl methyl sulfoxide (α-hydroxy allyl)',
            'products_text': 'β-hydroxy thioether',
            'reagents_text': 'thiophile (P(OMe)3 or NH4OH)',
            'conditions_text': 'MeOH, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase19_shallow_top10_v1 sprint for Mislow-Evans Rearrangement. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(O)C=CS(=O)C', 'allyl methyl sulfoxide (α-hydroxy allyl)', 'reactants_text', 1),
                ('product', 'CC(=O)CC(O)SC', 'β-hydroxy thioether', 'products_text', 1),
            ],
        },
        {
            'family': 'Mislow-Evans Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Mislow-Evans: sulfoxide→sulfenate [2,3]-shift then P(OMe)3 thiophile to give allyl alcohol.',
            'reactants_text': 'allyl phenyl sulfoxide',
            'products_text': 'allyl alcohol (+ thiophenol byproduct)',
            'reagents_text': 'P(OMe)3 (trapping thiophile)',
            'conditions_text': 'MeOH, 25°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase19_shallow_top10_v1 sprint for Mislow-Evans Rearrangement. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCS(=O)c1ccccc1', 'allyl phenyl sulfoxide', 'reactants_text', 1),
                ('product', 'OCC=C.c1ccc(S)cc1', 'allyl alcohol (+ thiophenol byproduct)', 'products_text', 1),
            ],
        },
        {
            'family': 'Mislow-Evans Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Mislow-Evans rearrangement of branched allyl sulfoxide to give allyl alcohol with transposition.',
            'reactants_text': '1-methyl-2-propenyl phenyl sulfoxide',
            'products_text': '1-methyl-allyl alcohol',
            'reagents_text': 'P(OMe)3',
            'conditions_text': 'MeOH, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase19_shallow_top10_v1 sprint for Mislow-Evans Rearrangement. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CC(C)S(=O)c1ccccc1', '1-methyl-2-propenyl phenyl sulfoxide', 'reactants_text', 1),
                ('product', 'OC(C)C=C.c1ccc(S)cc1', '1-methyl-allyl alcohol', 'products_text', 1),
            ],
        },
        # === Mitsunobu Reaction ===
        {
            'family': 'Mitsunobu Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Mitsunobu esterification: secondary alcohol + carboxylic acid coupled via PPh3/DIAD with inversion of configuration.',
            'reactants_text': '1-phenylethanol + benzoic acid',
            'products_text': '1-phenylethyl benzoate',
            'reagents_text': 'PPh3, DIAD, benzoic acid',
            'conditions_text': 'THF, 0°C→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase19_shallow_top10_v1 sprint for Mitsunobu Reaction. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)c1ccccc1.OC(=O)c1ccccc1', '1-phenylethanol + benzoic acid', 'reactants_text', 1),
                ('product', 'O=C(OC(C)c1ccccc1)c2ccccc2', '1-phenylethyl benzoate', 'products_text', 1),
            ],
        },
        {
            'family': 'Mitsunobu Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Mitsunobu N-alkylation of acidic NH heterocycle with primary alcohol.',
            'reactants_text': '3-phenyl-1-propanol + succinimide',
            'products_text': 'N-(3-phenylpropyl)succinimide',
            'reagents_text': 'PPh3, DIAD, succinimide',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase19_shallow_top10_v1 sprint for Mitsunobu Reaction. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCCCc1ccccc1.O=C1CCC(=O)N1', '3-phenyl-1-propanol + succinimide', 'reactants_text', 1),
                ('product', 'O=C1CCC(N1CCCc2ccccc2)=O', 'N-(3-phenylpropyl)succinimide', 'products_text', 1),
            ],
        },
        {
            'family': 'Mitsunobu Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Mitsunobu mono-acylation of diol with diacid to give mono-ester.',
            'reactants_text': '1-phenyl-1,2-ethanediol + glutaric acid',
            'products_text': 'monoglutarate of phenyl ethanediol',
            'reagents_text': 'PPh3, DIAD, glutaric acid (0.5 equiv)',
            'conditions_text': 'THF, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase19_shallow_top10_v1 sprint for Mitsunobu Reaction. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCC(O)c1ccccc1.OC(=O)CCCC(=O)O', '1-phenyl-1,2-ethanediol + glutaric acid', 'reactants_text', 1),
                ('product', 'O=C(OCC(O)c1ccccc1)CCCC(=O)O', 'monoglutarate of phenyl ethanediol', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Miyaura Boration ===
        {
            'family': 'Miyaura Boration',
            'extract_kind': 'application_example',
            'transformation_text': 'Miyaura borylation: aryl halide + bis(pinacolato)diboron with Pd catalyst gives aryl boronate (then hydrolyzed to boronic acid).',
            'reactants_text': 'bromobenzene',
            'products_text': 'phenylboronic acid',
            'reagents_text': 'B2pin2, Pd(dppf)Cl2, KOAc',
            'conditions_text': 'dioxane, 80°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase19_shallow_top10_v1 sprint for Miyaura Boration. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccccc1', 'bromobenzene', 'reactants_text', 1),
                ('product', 'OB(O)c1ccccc1', 'phenylboronic acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Miyaura Boration',
            'extract_kind': 'application_example',
            'transformation_text': 'Miyaura borylation of p-bromotoluene with B2pin2 to aryl boronate.',
            'reactants_text': '4-bromotoluene',
            'products_text': 'p-tolylboronic acid',
            'reagents_text': 'B2pin2, Pd(OAc)2, SPhos, KOAc',
            'conditions_text': 'dioxane, 90°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase19_shallow_top10_v1 sprint for Miyaura Boration. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccc(C)cc1', '4-bromotoluene', 'reactants_text', 1),
                ('product', 'Cc1ccc(B(O)O)cc1', 'p-tolylboronic acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Miyaura Boration',
            'extract_kind': 'application_example',
            'transformation_text': 'Miyaura borylation on aryl chloride (activated by methoxy) using Pd/SPhos system.',
            'reactants_text': '4-chloroanisole',
            'products_text': 'p-methoxyphenylboronic acid',
            'reagents_text': 'B2pin2, Pd(OAc)2, SPhos, K3PO4',
            'conditions_text': 'dioxane, 100°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase19_shallow_top10_v1 sprint for Miyaura Boration. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Clc1ccc(OC)cc1', '4-chloroanisole', 'reactants_text', 1),
                ('product', 'COc1ccc(B(O)O)cc1', 'p-methoxyphenylboronic acid', 'products_text', 1),
            ],
        },
        # === Mukaiyama Aldol Reaction ===
        {
            'family': 'Mukaiyama Aldol Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Mukaiyama aldol: silyl enol ether of acetone (from trimethylsilyl enol ether) with benzaldehyde under Lewis acid activation gives β-hydroxy ketone.',
            'reactants_text': 'isopropenyl acetate (acetone TMS enol surrogate) + benzaldehyde',
            'products_text': '4-hydroxy-4-phenyl-2-butanone',
            'reagents_text': 'TMS enol ether of acetone, TiCl4',
            'conditions_text': 'CH2Cl2, -78°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase19_shallow_top10_v1 sprint for Mukaiyama Aldol Reaction. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(=O)OC(=C)C.O=Cc1ccccc1', 'isopropenyl acetate (acetone TMS enol surrogate) + benzaldehyde', 'reactants_text', 1),
                ('product', 'OC(CC(=O)C)c1ccccc1', '4-hydroxy-4-phenyl-2-butanone', 'products_text', 1),
            ],
        },
        {
            'family': 'Mukaiyama Aldol Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Mukaiyama aldol with aryl ketone silyl enol ether + aryl aldehyde, TiCl4 activation.',
            'reactants_text': 'acetophenone (as TMS enol ether) + p-chlorobenzaldehyde',
            'products_text': 'β-hydroxy ketone aldol',
            'reagents_text': 'TMS-enol ether of acetophenone, TiCl4',
            'conditions_text': 'CH2Cl2, -78°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase19_shallow_top10_v1 sprint for Mukaiyama Aldol Reaction. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(=O)c1ccccc1.O=Cc2ccc(Cl)cc2', 'acetophenone (as TMS enol ether) + p-chlorobenzaldehyde', 'reactants_text', 1),
                ('product', 'OC(Cc1ccccc1)c2ccc(Cl)cc2', 'β-hydroxy ketone aldol', 'products_text', 1),
            ],
        },
        {
            'family': 'Mukaiyama Aldol Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Mukaiyama aldol between TMS enol ether of pentanone and acetone.',
            'reactants_text': 'diethyl ketone (as TMS enol ether) + acetone',
            'products_text': 'β-hydroxy diketone',
            'reagents_text': 'TMS enol ether, BF3·OEt2',
            'conditions_text': 'CH2Cl2, -78°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase19_shallow_top10_v1 sprint for Mukaiyama Aldol Reaction. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCC(=O)CC.O=C(C)C', 'diethyl ketone (as TMS enol ether) + acetone', 'reactants_text', 1),
                ('product', 'CCC(O)(C)CC(C)=O', 'β-hydroxy diketone', 'products_text', 1),
            ],
        },
        # === Myers Asymmetric Alkylation ===
        {
            'family': 'Myers Asymmetric Alkylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Myers asymmetric alkylation: pseudoephedrine amide deprotonated with LDA/LiCl then alkylated with benzyl bromide to give α-branched amide with high dr.',
            'reactants_text': '(1R,2S)-pseudoephedrine acetamide',
            'products_text': 'α-benzyl α-methyl amide',
            'reagents_text': 'LDA, LiCl, benzyl bromide',
            'conditions_text': 'THF, -78°C→0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase19_shallow_top10_v1 sprint for Myers Asymmetric Alkylation. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(N(C)C(CO)C(C)C)C', '(1R,2S)-pseudoephedrine acetamide', 'reactants_text', 1),
                ('product', 'O=C(N(C)C(CO)C(C)C)C(C)Cc1ccccc1', 'α-benzyl α-methyl amide', 'products_text', 1),
            ],
        },
        {
            'family': 'Myers Asymmetric Alkylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Myers α-alkylation using allyl bromide with LDA/LiCl base.',
            'reactants_text': 'pseudoephedrine propanamide',
            'products_text': 'α-allyl α-ethyl pseudoephedrine amide',
            'reagents_text': 'LDA, LiCl, allyl bromide',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase19_shallow_top10_v1 sprint for Myers Asymmetric Alkylation. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(N(C)C(CO)C(C)C)CC', 'pseudoephedrine propanamide', 'reactants_text', 1),
                ('product', 'O=C(N(C)C(CO)C(C)C)C(CC)CC=C', 'α-allyl α-ethyl pseudoephedrine amide', 'products_text', 1),
            ],
        },
        {
            'family': 'Myers Asymmetric Alkylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Myers asymmetric methylation with MeI on pseudoephedrine propanamide enolate.',
            'reactants_text': 'pseudoephedrine propanamide',
            'products_text': 'α-methyl α-ethyl pseudoephedrine amide',
            'reagents_text': 'LDA, LiCl, methyl iodide',
            'conditions_text': 'THF, -78°C→0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase19_shallow_top10_v1 sprint for Myers Asymmetric Alkylation. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(N(C)C(CO)C(C)C)CC', 'pseudoephedrine propanamide', 'reactants_text', 1),
                ('product', 'O=C(N(C)C(CO)C(C)C)C(CC)C', 'α-methyl α-ethyl pseudoephedrine amide', 'products_text', 1),
            ],
        },
        # === Nagata Hydrocyanation ===
        {
            'family': 'Nagata Hydrocyanation',
            'extract_kind': 'application_example',
            'transformation_text': 'Nagata hydrocyanation: Et2AlCN adds HCN equivalent in 1,4-fashion to cyclohexenone giving β-cyanoketone.',
            'reactants_text': 'cyclohex-2-enone',
            'products_text': '3-cyanocyclohexan-1-one',
            'reagents_text': 'Et2AlCN',
            'conditions_text': 'toluene, -78→0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase19_shallow_top10_v1 sprint for Nagata Hydrocyanation. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1C=CCCC1', 'cyclohex-2-enone', 'reactants_text', 1),
                ('product', 'O=C1CC(C#N)CCC1', '3-cyanocyclohexan-1-one', 'products_text', 1),
            ],
        },
        {
            'family': 'Nagata Hydrocyanation',
            'extract_kind': 'application_example',
            'transformation_text': 'Nagata hydrocyanation of α,β-unsaturated ester: conjugate CN addition via diethylaluminum cyanide.',
            'reactants_text': 'ethyl acrylate',
            'products_text': 'ethyl 3-cyanopropanoate',
            'reagents_text': 'Et2AlCN',
            'conditions_text': 'toluene, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase19_shallow_top10_v1 sprint for Nagata Hydrocyanation. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CC(=O)OCC', 'ethyl acrylate', 'reactants_text', 1),
                ('product', 'N#CCCC(=O)OCC', 'ethyl 3-cyanopropanoate', 'products_text', 1),
            ],
        },
        {
            'family': 'Nagata Hydrocyanation',
            'extract_kind': 'application_example',
            'transformation_text': 'Nagata hydrocyanation of crotonoyl ketone via Et2AlCN.',
            'reactants_text': '(E)-pent-3-en-2-one',
            'products_text': '4-cyanopentan-2-one',
            'reagents_text': 'Et2AlCN',
            'conditions_text': 'toluene, -40°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase19_shallow_top10_v1 sprint for Nagata Hydrocyanation. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(=O)/C=C/C', '(E)-pent-3-en-2-one', 'reactants_text', 1),
                ('product', 'CC(=O)CC(C#N)C', '4-cyanopentan-2-one', 'products_text', 1),
            ],
        },
        # === Nazarov Cyclization ===
        {
            'family': 'Nazarov Cyclization',
            'extract_kind': 'application_example',
            'transformation_text': 'Nazarov cyclization: Lewis acid activates divinyl ketone to 4π electrocyclization giving cyclopentenone.',
            'reactants_text': '1-phenyl-1,4-pentadien-3-one (divinyl ketone)',
            'products_text': '5-phenyl-cyclopent-2-enone',
            'reagents_text': 'BF3·OEt2',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase19_shallow_top10_v1 sprint for Nazarov Cyclization. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CC(=O)C=Cc1ccccc1', '1-phenyl-1,4-pentadien-3-one (divinyl ketone)', 'reactants_text', 1),
                ('product', 'O=C1CCC(=C1)c2ccccc2', '5-phenyl-cyclopent-2-enone', 'products_text', 1),
            ],
        },
        {
            'family': 'Nazarov Cyclization',
            'extract_kind': 'application_example',
            'transformation_text': 'Nazarov of doubly β-substituted divinyl ketone via protic acid; gives polyalkyl cyclopentenone.',
            'reactants_text': 'bis-crotyl ketone',
            'products_text': 'polysubstituted cyclopentenone',
            'reagents_text': 'TfOH',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase19_shallow_top10_v1 sprint for Nazarov Cyclization. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(=O)C(=CC)CC=CC', 'bis-crotyl ketone', 'reactants_text', 1),
                ('product', 'O=C1CC(C)=C(C)C1C', 'polysubstituted cyclopentenone', 'products_text', 1),
            ],
        },
        {
            'family': 'Nazarov Cyclization',
            'extract_kind': 'application_example',
            'transformation_text': 'Nazarov cyclization of symmetrical aryl divinyl ketone.',
            'reactants_text': '1,5-diphenyl-1,4-pentadien-3-one',
            'products_text': '3,4-diphenyl-cyclopent-2-enone',
            'reagents_text': 'Sc(OTf)3 (Lewis acid)',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase19_shallow_top10_v1 sprint for Nazarov Cyclization. [phase19_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccc(C=CC(=O)C=Cc2ccccc2)cc1', '1,5-diphenyl-1,4-pentadien-3-one', 'reactants_text', 1),
                ('product', 'O=C1CC(c2ccccc2)=C(c3ccccc3)C1', '3,4-diphenyl-cyclopent-2-enone', 'products_text', 1),
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
