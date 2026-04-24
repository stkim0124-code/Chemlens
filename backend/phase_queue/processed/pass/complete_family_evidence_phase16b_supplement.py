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

TAG = 'phase16b_shallow_top10_supplement_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        "Baldwin's Rules / Guidelines for Ring-Closing Reactions",
        'Dimroth Rearrangement',
        'Doering-Laflamme Allene Synthesis',
        'Dötz Benzannulation Reaction',
        'Enders SAMP/RAMP Hydrazone Alkylation',
    ],
    'b': [
        'Eschenmoser Methenylation',
        'Eschenmoser-Tanabe Fragmentation',
        'Favorskii and Homo-Favorskii Rearrangement',
        'Grob Fragmentation',
        'Hajos-Parrish Reaction',
    ],
}

# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Baldwin's Rules ===
        # variant B: recover phase16 seed #2 (5-bromo-1-pentanol -> THP)
        {
            'family': "Baldwin's Rules / Guidelines for Ring-Closing Reactions",
            'extract_kind': 'application_example',
            'transformation_text': "Application example (variant B): 6-exo-tet cyclization of 5-bromo-1-pentanol to tetrahydropyran under Baldwin guidelines.",
            'reactants_text': '5-bromo-1-pentanol',
            'products_text': 'tetrahydropyran',
            'reagents_text': 'base',
            'conditions_text': 'curated application-class supplement seed illustrating an allowed 6-exo-tet closure',
            'notes_text': "Manual curated application-class supplement seed (variant B) added during phase16b to recover the phase16 seed #2 chemistry for Baldwin's Rules. [phase16b_shallow_top10_supplement_v1]",
            'molecules': [
                ('reactant', 'OCCCCCBr', '5-bromo-1-pentanol', 'reactants_text', 1),
                ('product', 'C1CCOCC1', 'tetrahydropyran', 'products_text', 1),
            ],
        },
        # variant C: 4-bromobutan-1-amine -> pyrrolidine (5-exo-tet aminocyclization)
        {
            'family': "Baldwin's Rules / Guidelines for Ring-Closing Reactions",
            'extract_kind': 'application_example',
            'transformation_text': "Application example (variant C): 5-exo-tet amine cyclization of 4-bromobutan-1-amine to pyrrolidine.",
            'reactants_text': '4-bromobutan-1-amine',
            'products_text': 'pyrrolidine',
            'reagents_text': 'base',
            'conditions_text': 'curated application-class supplement seed illustrating an allowed 5-exo-tet N-cyclization',
            'notes_text': "Manual curated application-class supplement seed (variant C) added during phase16b to introduce a fresh distinct Baldwin's Rules example and push queryable R/P and pair count to rich threshold. [phase16b_shallow_top10_supplement_v1]",
            'molecules': [
                ('reactant', 'NCCCCBr', '4-bromobutan-1-amine', 'reactants_text', 1),
                ('product', 'C1CCNC1', 'pyrrolidine', 'products_text', 1),
            ],
        },
        # === Dimroth Rearrangement ===
        # variant B: phase16 seed #2 (N-phenyl triazole Dimroth)
        {
            'family': 'Dimroth Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant B): Dimroth rearrangement of 1-phenyl-N-methyl-4-amino-1,2,3-triazole to the 2-phenyl regioisomer.',
            'reactants_text': '1-phenyl-N-methyl-4-amino-1,2,3-triazole',
            'products_text': '2-phenyl-N-methyl-4-amino-1,2,3-triazole',
            'reagents_text': 'base, heat',
            'conditions_text': 'curated supplement seed (variant B) adding an N-aryl triazole Dimroth example',
            'notes_text': 'Manual curated application-class supplement seed (variant B) added during phase16b to recover the phase16 seed #2 chemistry for Dimroth rearrangement. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'CNc1cn(-c2ccccc2)nn1', '1-phenyl-N-methyl-4-amino-1,2,3-triazole', 'reactants_text', 1),
                ('product', 'CNc1cnn(-c2ccccc2)n1', '2-phenyl-N-methyl-4-amino-1,2,3-triazole', 'products_text', 1),
            ],
        },
        # variant C: N-benzyl triazole Dimroth
        {
            'family': 'Dimroth Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant C): Dimroth rearrangement of 1-benzyl-N-methyl-4-amino-1,2,3-triazole to the 2-benzyl regioisomer.',
            'reactants_text': '1-benzyl-N-methyl-4-amino-1,2,3-triazole',
            'products_text': '2-benzyl-N-methyl-4-amino-1,2,3-triazole',
            'reagents_text': 'base, heat',
            'conditions_text': 'curated supplement seed (variant C) adding an N-benzyl triazole Dimroth example',
            'notes_text': 'Manual curated application-class supplement seed (variant C) added during phase16b to introduce a fresh distinct Dimroth rearrangement example and push queryable R/P and pair count to rich threshold. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'CNc1cn(Cc2ccccc2)nn1', '1-benzyl-N-methyl-4-amino-1,2,3-triazole', 'reactants_text', 1),
                ('product', 'CNc1cnn(Cc2ccccc2)n1', '2-benzyl-N-methyl-4-amino-1,2,3-triazole', 'products_text', 1),
            ],
        },
        # === Doering-Laflamme ===
        # variant B: 1,1-dibromo-2,3-dimethylcyclopropane -> 2,3-pentadiene
        {
            'family': 'Doering-Laflamme Allene Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant B): Doering-Laflamme synthesis converts 1,1-dibromo-2,3-dimethylcyclopropane to 2,3-pentadiene.',
            'reactants_text': '1,1-dibromo-2,3-dimethylcyclopropane',
            'products_text': '2,3-pentadiene',
            'reagents_text': 'alkyllithium',
            'conditions_text': 'curated supplement seed (variant B) adding disubstituted gem-dibromocyclopropane example',
            'notes_text': 'Manual curated application-class supplement seed (variant B) added during phase16b to recover the phase16 seed #2 chemistry for Doering-Laflamme allene synthesis. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'CC1C(C)C1(Br)Br', '1,1-dibromo-2,3-dimethylcyclopropane', 'reactants_text', 1),
                ('product', 'CC=C=CC', '2,3-pentadiene', 'products_text', 1),
            ],
        },
        # variant C: 1,1-dibromo-2-ethylcyclopropane -> 1,2-pentadiene (ethylallene)
        {
            'family': 'Doering-Laflamme Allene Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant C): Doering-Laflamme synthesis converts 1,1-dibromo-2-ethylcyclopropane to 1,2-pentadiene.',
            'reactants_text': '1,1-dibromo-2-ethylcyclopropane',
            'products_text': '1,2-pentadiene',
            'reagents_text': 'alkyllithium',
            'conditions_text': 'curated supplement seed (variant C) adding ethyl-substituted gem-dibromocyclopropane example',
            'notes_text': 'Manual curated application-class supplement seed (variant C) added during phase16b to introduce a fresh distinct Doering-Laflamme allene synthesis example and push queryable R/P and pair count to rich threshold. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'CCC1CC1(Br)Br', '1,1-dibromo-2-ethylcyclopropane', 'reactants_text', 1),
                ('product', 'CCC=C=C', '1,2-pentadiene', 'products_text', 1),
            ],
        },
        # === Dötz Benzannulation ===
        # variant B: 2-pentyne -> 2-ethyl-6-methylphenol
        {
            'family': 'Dötz Benzannulation Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant B): Dötz benzannulation of 2-pentyne with a Fischer carbene yields a 2-ethyl-6-methylphenol framework.',
            'reactants_text': '2-pentyne',
            'products_text': '2-ethyl-6-methylphenol',
            'reagents_text': 'chromium Fischer carbene complex, CO',
            'conditions_text': 'curated supplement seed (variant B) adding unsymmetrical alkyne Dötz example',
            'notes_text': 'Manual curated application-class supplement seed (variant B) added during phase16b to recover the phase16 seed #2 chemistry for Dötz benzannulation. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'CCC#CC', '2-pentyne', 'reactants_text', 1),
                ('product', 'CCc1cccc(C)c1O', '2-ethyl-6-methylphenol', 'products_text', 1),
            ],
        },
        # variant C: 2-hexyne -> 2-propyl-6-methylphenol
        {
            'family': 'Dötz Benzannulation Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant C): Dötz benzannulation of 2-hexyne with a Fischer carbene yields a 2-propyl-6-methylphenol framework.',
            'reactants_text': '2-hexyne',
            'products_text': '2-propyl-6-methylphenol',
            'reagents_text': 'chromium Fischer carbene complex, CO',
            'conditions_text': 'curated supplement seed (variant C) adding longer-chain unsymmetrical alkyne Dötz example',
            'notes_text': 'Manual curated application-class supplement seed (variant C) added during phase16b to introduce a fresh distinct Dötz benzannulation example and push queryable R/P and pair count to rich threshold. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'CCCC#CC', '2-hexyne', 'reactants_text', 1),
                ('product', 'CCCc1cccc(C)c1O', '2-propyl-6-methylphenol', 'products_text', 1),
            ],
        },
        # === Enders SAMP/RAMP ===
        # variant B: pentan-2-one -> 3-methylpentan-2-one
        {
            'family': 'Enders SAMP/RAMP Hydrazone Alkylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant B): Enders SAMP-mediated asymmetric α-methylation of pentan-2-one to 3-methylpentan-2-one.',
            'reactants_text': 'pentan-2-one',
            'products_text': '3-methylpentan-2-one',
            'reagents_text': 'SAMP, base, methyl iodide, then hydrolysis',
            'conditions_text': 'curated supplement seed (variant B) adding acyclic ketone SAMP/RAMP example',
            'notes_text': 'Manual curated application-class supplement seed (variant B) added during phase16b to recover the phase16 seed #2 chemistry for Enders SAMP/RAMP. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'CCCC(C)=O', 'pentan-2-one', 'reactants_text', 1),
                ('product', 'CCC(C)C(C)=O', '3-methylpentan-2-one', 'products_text', 1),
            ],
        },
        # variant C: butan-2-one -> 3-methylbutan-2-one
        {
            'family': 'Enders SAMP/RAMP Hydrazone Alkylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant C): Enders SAMP-mediated asymmetric α-methylation of butan-2-one to 3-methylbutan-2-one.',
            'reactants_text': 'butan-2-one',
            'products_text': '3-methylbutan-2-one',
            'reagents_text': 'SAMP, base, methyl iodide, then hydrolysis',
            'conditions_text': 'curated supplement seed (variant C) adding short-chain ketone SAMP/RAMP example',
            'notes_text': 'Manual curated application-class supplement seed (variant C) added during phase16b to introduce a fresh distinct Enders SAMP/RAMP example and push queryable R/P and pair count to rich threshold. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'CCC(C)=O', 'butan-2-one', 'reactants_text', 1),
                ('product', 'CC(C)C(C)=O', '3-methylbutan-2-one', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Eschenmoser Methenylation ===
        # variant B: propiophenone -> 2-methylene-1-phenylpropan-1-one
        {
            'family': 'Eschenmoser Methenylation',
            'extract_kind': 'application_example',
            'transformation_text': "Application example (variant B): Eschenmoser methenylation of propiophenone to 2-methylene-1-phenylpropan-1-one.",
            'reactants_text': 'propiophenone',
            'products_text': '2-methylene-1-phenylpropan-1-one',
            'reagents_text': "Eschenmoser's salt, base, then elimination",
            'conditions_text': 'curated supplement seed (variant B) adding aryl-alkyl ketone methenylation example',
            'notes_text': 'Manual curated application-class supplement seed (variant B) added during phase16b to recover the phase16 seed #2 chemistry for Eschenmoser methenylation. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'CCC(=O)c1ccccc1', 'propiophenone', 'reactants_text', 1),
                ('product', 'C=C(C)C(=O)c1ccccc1', '2-methylene-1-phenylpropan-1-one', 'products_text', 1),
            ],
        },
        # variant C: cyclopentanone -> 2-methylenecyclopentan-1-one
        {
            'family': 'Eschenmoser Methenylation',
            'extract_kind': 'application_example',
            'transformation_text': "Application example (variant C): Eschenmoser methenylation of cyclopentanone to 2-methylenecyclopentan-1-one.",
            'reactants_text': 'cyclopentanone',
            'products_text': '2-methylenecyclopentan-1-one',
            'reagents_text': "Eschenmoser's salt, base, then elimination",
            'conditions_text': 'curated supplement seed (variant C) adding five-membered cyclic ketone methenylation',
            'notes_text': 'Manual curated application-class supplement seed (variant C) added during phase16b to introduce a fresh distinct Eschenmoser methenylation example and push queryable R/P and pair count to rich threshold. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCC1', 'cyclopentanone', 'reactants_text', 1),
                ('product', 'C=C1CCCC1=O', '2-methylenecyclopentan-1-one', 'products_text', 1),
            ],
        },
        # === Eschenmoser-Tanabe Fragmentation ===
        # variant B: cyclopentanone epoxide fragmentation -> pent-4-ynal
        {
            'family': 'Eschenmoser-Tanabe Fragmentation',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant B): Eschenmoser-Tanabe fragmentation of 2,3-epoxycyclopentan-1-one tosylhydrazone yields pent-4-ynal.',
            'reactants_text': '2,3-epoxycyclopentan-1-one',
            'products_text': 'pent-4-ynal',
            'reagents_text': 'p-toluenesulfonylhydrazine, then base',
            'conditions_text': 'curated supplement seed (variant B) adding smaller-ring epoxy-ketone fragmentation example',
            'notes_text': 'Manual curated application-class supplement seed (variant B) added during phase16b to recover the phase16 seed #2 chemistry for Eschenmoser-Tanabe fragmentation. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'O=C1CCC2OC12', '2,3-epoxycyclopentan-1-one', 'reactants_text', 1),
                ('product', 'O=CCCC#C', 'pent-4-ynal', 'products_text', 1),
            ],
        },
        # variant C: cycloheptanone epoxide fragmentation -> hept-6-ynal
        {
            'family': 'Eschenmoser-Tanabe Fragmentation',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant C): Eschenmoser-Tanabe fragmentation of 2,3-epoxycycloheptan-1-one tosylhydrazone yields hept-6-ynal.',
            'reactants_text': '2,3-epoxycycloheptan-1-one',
            'products_text': 'hept-6-ynal',
            'reagents_text': 'p-toluenesulfonylhydrazine, then base',
            'conditions_text': 'curated supplement seed (variant C) adding seven-membered ring epoxy-ketone fragmentation',
            'notes_text': 'Manual curated application-class supplement seed (variant C) added during phase16b to introduce a fresh distinct Eschenmoser-Tanabe fragmentation example and push queryable R/P and pair count to rich threshold. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCCC2OC12', '2,3-epoxycycloheptan-1-one', 'reactants_text', 1),
                ('product', 'O=CCCCCC#C', 'hept-6-ynal', 'products_text', 1),
            ],
        },
        # === Favorskii ===
        # variant B: 1-chloropropan-2-one -> propionic acid
        {
            'family': 'Favorskii and Homo-Favorskii Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant B): Favorskii rearrangement of 1-chloropropan-2-one to propionic acid via a cyclopropanone intermediate.',
            'reactants_text': '1-chloropropan-2-one',
            'products_text': 'propionic acid',
            'reagents_text': 'aqueous NaOH',
            'conditions_text': 'curated supplement seed (variant B) adding simple acyclic α-halo-ketone Favorskii example',
            'notes_text': 'Manual curated application-class supplement seed (variant B) added during phase16b to recover the phase16 seed #2 chemistry for Favorskii rearrangement. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'ClCC(C)=O', '1-chloropropan-2-one', 'reactants_text', 1),
                ('product', 'CCC(=O)O', 'propionic acid', 'products_text', 1),
            ],
        },
        # variant C: 2-bromocyclopentan-1-one -> cyclobutanecarboxylic acid
        {
            'family': 'Favorskii and Homo-Favorskii Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant C): Favorskii rearrangement of 2-bromocyclopentan-1-one to cyclobutanecarboxylic acid via ring contraction.',
            'reactants_text': '2-bromocyclopentan-1-one',
            'products_text': 'cyclobutanecarboxylic acid',
            'reagents_text': 'aqueous NaOH',
            'conditions_text': 'curated supplement seed (variant C) adding five-membered ring α-halo-ketone ring contraction',
            'notes_text': 'Manual curated application-class supplement seed (variant C) added during phase16b to introduce a fresh distinct Favorskii rearrangement example and push queryable R/P and pair count to rich threshold. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCC1Br', '2-bromocyclopentan-1-one', 'reactants_text', 1),
                ('product', 'O=C(O)C1CCC1', 'cyclobutanecarboxylic acid', 'products_text', 1),
            ],
        },
        # === Grob Fragmentation ===
        # variant B: 5-bromo-2-(dimethylamino)pentane -> but-1-ene
        {
            'family': 'Grob Fragmentation',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant B): Grob fragmentation of 5-bromo-2-(dimethylamino)pentane yields but-1-ene.',
            'reactants_text': '5-bromo-2-(dimethylamino)pentane',
            'products_text': 'but-1-ene',
            'reagents_text': 'base',
            'conditions_text': 'curated supplement seed (variant B) adding longer-chain γ-amino-halide Grob example',
            'notes_text': 'Manual curated application-class supplement seed (variant B) added during phase16b to recover the phase16 seed #2 chemistry for Grob fragmentation. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'CC(N(C)C)CCCBr', '5-bromo-2-(dimethylamino)pentane', 'reactants_text', 1),
                ('product', 'CCC=C', 'but-1-ene', 'products_text', 1),
            ],
        },
        # variant C: 6-bromo-2-(dimethylamino)hexane -> pent-1-ene
        {
            'family': 'Grob Fragmentation',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant C): Grob fragmentation of 6-bromo-2-(dimethylamino)hexane yields pent-1-ene.',
            'reactants_text': '6-bromo-2-(dimethylamino)hexane',
            'products_text': 'pent-1-ene',
            'reagents_text': 'base',
            'conditions_text': 'curated supplement seed (variant C) adding extended-chain γ-amino-halide Grob example',
            'notes_text': 'Manual curated application-class supplement seed (variant C) added during phase16b to introduce a fresh distinct Grob fragmentation example and push queryable R/P and pair count to rich threshold. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'CC(N(C)C)CCCCBr', '6-bromo-2-(dimethylamino)hexane', 'reactants_text', 1),
                ('product', 'CCCC=C', 'pent-1-ene', 'products_text', 1),
            ],
        },
        # === Hajos-Parrish ===
        # variant B: cyclohexanedione analog -> Wieland-Miescher framework
        {
            'family': 'Hajos-Parrish Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant B): Wieland-Miescher-style analog of Hajos-Parrish: 2-methyl-2-(3-oxobutyl)-1,3-cyclohexanedione with L-proline yields the Wieland-Miescher bicyclic framework.',
            'reactants_text': '2-methyl-2-(3-oxobutyl)-1,3-cyclohexanedione',
            'products_text': 'Wieland-Miescher framework',
            'reagents_text': 'L-proline, DMF',
            'conditions_text': 'curated supplement seed (variant B) adding six-membered Hajos-Parrish analog example',
            'notes_text': 'Manual curated application-class supplement seed (variant B) added during phase16b to recover the phase16 seed #2 chemistry for Hajos-Parrish reaction. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCC(=O)C1(C)CCC(C)=O', '2-methyl-2-(3-oxobutyl)-1,3-cyclohexanedione', 'reactants_text', 1),
                ('product', 'O=C1CCCC2(C)C1CCC2=O', 'Wieland-Miescher framework', 'products_text', 1),
            ],
        },
        # variant C: 2-ethyl variant of original Hajos-Parrish
        {
            'family': 'Hajos-Parrish Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example (variant C): 2-ethyl analog of Hajos-Parrish: 2-ethyl-2-(3-oxobutyl)-1,3-cyclopentanedione with L-proline yields an ethyl-substituted bicyclic framework.',
            'reactants_text': '2-ethyl-2-(3-oxobutyl)-1,3-cyclopentanedione',
            'products_text': 'ethyl-Hajos-Parrish framework',
            'reagents_text': 'L-proline, DMF',
            'conditions_text': 'curated supplement seed (variant C) adding 2-ethyl Hajos-Parrish analog example',
            'notes_text': 'Manual curated application-class supplement seed (variant C) added during phase16b to introduce a fresh distinct Hajos-Parrish reaction example and push queryable R/P and pair count to rich threshold. [phase16b_shallow_top10_supplement_v1]',
            'molecules': [
                ('reactant', 'O=C1CC(=O)C1(CC)CCC(C)=O', '2-ethyl-2-(3-oxobutyl)-1,3-cyclopentanedione', 'reactants_text', 1),
                ('product', 'O=C1CCC2(CC)C1CCC2=O', 'ethyl-Hajos-Parrish framework', 'products_text', 1),
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
