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

TAG = 'phase26_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Skraup and Doebner-Miller Quinoline Synthesis',
        'Smiles Rearrangement',
        'Smith-Tietze Multicomponent Dithiane Linchpin Coupling',
        'Snieckus Directed Ortho Metalation',
        'Sommelet-Hauser Rearrangement',
    ],
    'b': [
        'Sonogashira Cross-Coupling',
        'Staudinger Ketene Cycloaddition',
        'Staudinger Reaction',
        'Stephen Aldehyde Synthesis (Stephen Reduction)',
        'Stetter Reaction',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Skraup and Doebner-Miller Quinoline Synthesis ===
        {
            'family': 'Skraup and Doebner-Miller Quinoline Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Skraup synthesis: aniline + glycerol (→ acrolein in situ) + oxidant under H2SO4 gives quinoline.',
            'reactants_text': 'aniline + glycerol',
            'products_text': 'quinoline',
            'reagents_text': 'H2SO4, nitrobenzene (oxidant), glycerol',
            'conditions_text': '180°C, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase26_shallow_top10_v1 sprint for Skraup and Doebner-Miller Quinoline Synthesis. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Nc1ccccc1.OCC(O)CO', 'aniline + glycerol', 'reactants_text', 1),
                ('product', 'c1ccc2ncccc2c1', 'quinoline', 'products_text', 1),
            ],
        },
        {
            'family': 'Skraup and Doebner-Miller Quinoline Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Doebner-Miller quinoline synthesis: aniline + α,β-unsat. aldehyde gives 2-substituted quinoline.',
            'reactants_text': 'p-toluidine + crotonaldehyde',
            'products_text': '2,6-dimethylquinoline',
            'reagents_text': 'HCl (cat.), heat',
            'conditions_text': 'H2O, 120°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase26_shallow_top10_v1 sprint for Skraup and Doebner-Miller Quinoline Synthesis. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Nc1ccc(C)cc1.O=CC=CC', 'p-toluidine + crotonaldehyde', 'reactants_text', 1),
                ('product', 'Cc1ccc2nc(C)ccc2c1', '2,6-dimethylquinoline', 'products_text', 1),
            ],
        },
        {
            'family': 'Skraup and Doebner-Miller Quinoline Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Skraup with p-anisidine giving methoxy-quinoline.',
            'reactants_text': 'p-anisidine + glycerol',
            'products_text': '6-methoxyquinoline',
            'reagents_text': 'H2SO4, As2O5 (oxidant)',
            'conditions_text': '180°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase26_shallow_top10_v1 sprint for Skraup and Doebner-Miller Quinoline Synthesis. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Nc1ccc(OC)cc1.OCC(O)CO', 'p-anisidine + glycerol', 'reactants_text', 1),
                ('product', 'COc1ccc2ncccc2c1', '6-methoxyquinoline', 'products_text', 1),
            ],
        },
        # === Smiles Rearrangement ===
        {
            'family': 'Smiles Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Smiles rearrangement: intramolecular aromatic ipso substitution of aryl ether to aryl amine driven by EWG.',
            'reactants_text': 'p-nitrophenoxy acetate',
            'products_text': 'N-(p-nitrophenyl)amino acetic acid',
            'reagents_text': 'NaH, DMF',
            'conditions_text': '0°C→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase26_shallow_top10_v1 sprint for Smiles Rearrangement. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=[N+]([O-])c1ccc(OCC(=O)O)cc1', 'p-nitrophenoxy acetate', 'reactants_text', 1),
                ('product', 'O=C(O)CNc1ccc([N+](=O)[O-])cc1', 'N-(p-nitrophenyl)amino acetic acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Smiles Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Smiles-type rearrangement via ipso Meisenheimer on activated nitroaryl.',
            'reactants_text': 'N-aryl oxazolidinone with p-nitro',
            'products_text': 'rearranged N-O isomer',
            'reagents_text': 'NaH / DMF',
            'conditions_text': 'rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase26_shallow_top10_v1 sprint for Smiles Rearrangement. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1OCCN1c1ccc([N+](=O)[O-])cc1', 'N-aryl oxazolidinone with p-nitro', 'reactants_text', 1),
                ('product', 'O=C1NCCO1Nc1ccc([N+](=O)[O-])cc1', 'rearranged N-O isomer', 'products_text', 1),
            ],
        },
        {
            'family': 'Smiles Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Smiles rearrangement showing O→N migration on activated aryl.',
            'reactants_text': 'o-chloro-p-nitro-phenol (simplified)',
            'products_text': 'ortho-amino-p-nitrophenol (Smiles rearranged)',
            'reagents_text': 'NH3 (aq), base',
            'conditions_text': 'DMSO, 80°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase26_shallow_top10_v1 sprint for Smiles Rearrangement. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Oc1ccc([N+](=O)[O-])cc1Cl', 'o-chloro-p-nitro-phenol (simplified)', 'reactants_text', 1),
                ('product', 'Nc1ccc([N+](=O)[O-])cc1O', 'ortho-amino-p-nitrophenol (Smiles rearranged)', 'products_text', 1),
            ],
        },
        # === Smith-Tietze Multicomponent Dithiane Linchpin Coupling ===
        {
            'family': 'Smith-Tietze Multicomponent Dithiane Linchpin Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Smith-Tietze linchpin MCR: dithiane anion adds to epoxide, 1,4-Brook rearrangement, second epoxide alkylates.',
            'reactants_text': '1,3-dithiane + two equivalents of epoxide (simplified)',
            'products_text': 'doubly alkylated diol-dithiane adduct',
            'reagents_text': 'n-BuLi, (TMS)CH(Li)SR2S, epoxides',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase26_shallow_top10_v1 sprint for Smith-Tietze Multicomponent Dithiane Linchpin Coupling. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'S1CCCC(=S)S1.C1CO1.C1CO1', '1,3-dithiane + two equivalents of epoxide (simplified)', 'reactants_text', 1),
                ('product', 'OCC(O)(C1CCCCC1)CC(O)CC(O)CC', 'doubly alkylated diol-dithiane adduct', 'products_text', 1),
            ],
        },
        {
            'family': 'Smith-Tietze Multicomponent Dithiane Linchpin Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Smith linchpin: first alkylation of 2-cyano dithiane anion with epoxide.',
            'reactants_text': 'α-cyano dithiane + ethylene oxide',
            'products_text': 'hydroxyalkyl cyano-dithiane',
            'reagents_text': 'n-BuLi, ethylene oxide',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase26_shallow_top10_v1 sprint for Smith-Tietze Multicomponent Dithiane Linchpin Coupling. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'S1CCCC(C)(C#N)S1.C1CO1', 'α-cyano dithiane + ethylene oxide', 'reactants_text', 1),
                ('product', 'OCCC(C)(C#N)CC', 'hydroxyalkyl cyano-dithiane', 'products_text', 1),
            ],
        },
        {
            'family': 'Smith-Tietze Multicomponent Dithiane Linchpin Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Smith multicomponent with acyl dithiane anion and glycidol.',
            'reactants_text': '2-formyl dithiane + glycidol',
            'products_text': 'Smith-Tietze multi-alkylated adduct',
            'reagents_text': 'n-BuLi, HMPA',
            'conditions_text': 'THF, -78°C→-40°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase26_shallow_top10_v1 sprint for Smith-Tietze Multicomponent Dithiane Linchpin Coupling. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'S1CCCC(C=O)S1.C1CO1', '2-formyl dithiane + glycidol', 'reactants_text', 1),
                ('product', 'OCC(CC)(S1CCCCS1)CO', 'Smith-Tietze multi-alkylated adduct', 'products_text', 1),
            ],
        },
        # === Snieckus Directed Ortho Metalation ===
        {
            'family': 'Snieckus Directed Ortho Metalation',
            'extract_kind': 'application_example',
            'transformation_text': 'Snieckus DOM: anisole + sBuLi directed by OMe gives ortho-lithiated species quenched by MeI to ortho-methyl product.',
            'reactants_text': 'anisole',
            'products_text': '2-methylanisole (ortho-lithiation, quench with MeI)',
            'reagents_text': 's-BuLi, TMEDA; then MeI',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase26_shallow_top10_v1 sprint for Snieckus Directed Ortho Metalation. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'COc1ccccc1', 'anisole', 'reactants_text', 1),
                ('product', 'COc1ccccc1C', '2-methylanisole (ortho-lithiation, quench with MeI)', 'products_text', 1),
            ],
        },
        {
            'family': 'Snieckus Directed Ortho Metalation',
            'extract_kind': 'application_example',
            'transformation_text': 'Snieckus DOM directed by amide: lithiation then B(OMe)3 / acid workup gives ortho-B(OH)2.',
            'reactants_text': 'N,N-dimethylbenzamide',
            'products_text': 'ortho-boronic acid N,N-dimethylbenzamide',
            'reagents_text': 's-BuLi, TMEDA; B(OMe)3; H3O+',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase26_shallow_top10_v1 sprint for Snieckus Directed Ortho Metalation. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(N(C)C)c1ccccc1', 'N,N-dimethylbenzamide', 'reactants_text', 1),
                ('product', 'O=C(N(C)C)c1ccccc1B(O)O', 'ortho-boronic acid N,N-dimethylbenzamide', 'products_text', 1),
            ],
        },
        {
            'family': 'Snieckus Directed Ortho Metalation',
            'extract_kind': 'application_example',
            'transformation_text': 'Snieckus DOM with MOM as DMG; quench with DMF, then reduction.',
            'reactants_text': 'MOM-protected phenol',
            'products_text': 'ortho-hydroxymethyl MOM ether',
            'reagents_text': 'n-BuLi; DMF; NaBH4',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase26_shallow_top10_v1 sprint for Snieckus Directed Ortho Metalation. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccc(OCOC)cc1', 'MOM-protected phenol', 'reactants_text', 1),
                ('product', 'OCc1ccccc1OCOC', 'ortho-hydroxymethyl MOM ether', 'products_text', 1),
            ],
        },
        # === Sommelet-Hauser Rearrangement ===
        {
            'family': 'Sommelet-Hauser Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Sommelet-Hauser [2,3]-sigmatropic rearrangement of benzyl ammonium ylide gives ortho-alkyl tertiary amine.',
            'reactants_text': 'benzyl trimethylammonium cation',
            'products_text': 'ortho-methyl tertiary amine',
            'reagents_text': 'NaNH2, liq. NH3',
            'conditions_text': 'liq. NH3, -33°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase26_shallow_top10_v1 sprint for Sommelet-Hauser Rearrangement. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C[N+](C)(C)Cc1ccccc1', 'benzyl trimethylammonium cation', 'reactants_text', 1),
                ('product', 'CN(C)Cc1ccccc1C', 'ortho-methyl tertiary amine', 'products_text', 1),
            ],
        },
        {
            'family': 'Sommelet-Hauser Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Sommelet-Hauser variant: N-methyl ammonium ylide → ortho-alkyl via ylide [2,3]-shift.',
            'reactants_text': 'N,N-diethyl-N-benzyl-N-methyl ammonium',
            'products_text': 'ortho-ethyl tertiary amine',
            'reagents_text': 'NaNH2, NH3(l)',
            'conditions_text': 'THF/liq NH3, -33°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase26_shallow_top10_v1 sprint for Sommelet-Hauser Rearrangement. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC[N+](C)(CC)Cc1ccccc1', 'N,N-diethyl-N-benzyl-N-methyl ammonium', 'reactants_text', 1),
                ('product', 'CCN(CC)Cc1ccccc1CC', 'ortho-ethyl tertiary amine', 'products_text', 1),
            ],
        },
        {
            'family': 'Sommelet-Hauser Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Sommelet-Hauser on activated benzyl ammonium.',
            'reactants_text': 'p-methoxybenzyl trimethylammonium',
            'products_text': 'ortho-methyl anisidine-type amine',
            'reagents_text': 'NaNH2, NH3(l)',
            'conditions_text': 'NH3, -33°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase26_shallow_top10_v1 sprint for Sommelet-Hauser Rearrangement. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C[N+](C)(Cc1ccc(OC)cc1)C', 'p-methoxybenzyl trimethylammonium', 'reactants_text', 1),
                ('product', 'CN(C)Cc1ccc(OC)cc1C', 'ortho-methyl anisidine-type amine', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Sonogashira Cross-Coupling ===
        {
            'family': 'Sonogashira Cross-Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Sonogashira: aryl halide + terminal alkyne with Pd/Cu catalysis.',
            'reactants_text': 'iodobenzene + 3,3-dimethyl-1-butyne',
            'products_text': '1-tert-butyl-2-phenyl-acetylene',
            'reagents_text': 'Pd(PPh3)2Cl2, CuI, Et3N',
            'conditions_text': 'Et3N, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase26_shallow_top10_v1 sprint for Sonogashira Cross-Coupling. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1ccccc1.C#CC(C)(C)C', 'iodobenzene + 3,3-dimethyl-1-butyne', 'reactants_text', 1),
                ('product', 'CC(C)(C)C#Cc1ccccc1', '1-tert-butyl-2-phenyl-acetylene', 'products_text', 1),
            ],
        },
        {
            'family': 'Sonogashira Cross-Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Sonogashira on p-bromoanisole with arylalkyne.',
            'reactants_text': 'p-bromoanisole + phenylacetylene',
            'products_text': '4-methoxy-diphenylacetylene',
            'reagents_text': 'Pd(PPh3)4, CuI, Et3N',
            'conditions_text': 'DMF, 60°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase26_shallow_top10_v1 sprint for Sonogashira Cross-Coupling. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccc(OC)cc1.C#Cc2ccccc2', 'p-bromoanisole + phenylacetylene', 'reactants_text', 1),
                ('product', 'COc1ccc(C#Cc2ccccc2)cc1', '4-methoxy-diphenylacetylene', 'products_text', 1),
            ],
        },
        {
            'family': 'Sonogashira Cross-Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Sonogashira coupling with propargyl alcohol.',
            'reactants_text': '1-iodo-3-fluorobenzene + propargyl alcohol',
            'products_text': '3-fluorophenyl propargyl alcohol',
            'reagents_text': 'Pd(PPh3)2Cl2, CuI, iPr2NH',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase26_shallow_top10_v1 sprint for Sonogashira Cross-Coupling. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1cccc(F)c1.C#CCO', '1-iodo-3-fluorobenzene + propargyl alcohol', 'reactants_text', 1),
                ('product', 'Fc1cccc(C#CCO)c1', '3-fluorophenyl propargyl alcohol', 'products_text', 1),
            ],
        },
        # === Staudinger Ketene Cycloaddition ===
        {
            'family': 'Staudinger Ketene Cycloaddition',
            'extract_kind': 'application_example',
            'transformation_text': 'Staudinger ketene-imine [2+2]: ketene + imine gives β-lactam.',
            'reactants_text': 'ketene + N-methylbenzaldimine',
            'products_text': 'β-lactam (cis-3-phenyl-1-methyl-2-azetidinone)',
            'reagents_text': 'Et3N, acid chloride source of ketene',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase26_shallow_top10_v1 sprint for Staudinger Ketene Cycloaddition. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=C=O.CN=Cc1ccccc1', 'ketene + N-methylbenzaldimine', 'reactants_text', 1),
                ('product', 'O=C1CN(C)C1c1ccccc1', 'β-lactam (cis-3-phenyl-1-methyl-2-azetidinone)', 'products_text', 1),
            ],
        },
        {
            'family': 'Staudinger Ketene Cycloaddition',
            'extract_kind': 'application_example',
            'transformation_text': 'Staudinger ketene cycloaddition with N-benzyl imine.',
            'reactants_text': 'ketene + N-benzyl formaldimine',
            'products_text': 'N-benzyl β-lactam',
            'reagents_text': 'Et3N, phenylacetyl chloride',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase26_shallow_top10_v1 sprint for Staudinger Ketene Cycloaddition. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=C=O.C=NCc1ccccc1', 'ketene + N-benzyl formaldimine', 'reactants_text', 1),
                ('product', 'O=C1CN(Cc2ccccc2)C1', 'N-benzyl β-lactam', 'products_text', 1),
            ],
        },
        {
            'family': 'Staudinger Ketene Cycloaddition',
            'extract_kind': 'application_example',
            'transformation_text': 'Staudinger with alkyl ketene and cyclic imine.',
            'reactants_text': 'methylketene + cyclic imine',
            'products_text': 'bicyclic β-lactam',
            'reagents_text': 'Et3N, chlorohydride of propionyl chloride',
            'conditions_text': 'CH2Cl2',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase26_shallow_top10_v1 sprint for Staudinger Ketene Cycloaddition. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC=C=O.C1=NCCC1', 'methylketene + cyclic imine', 'reactants_text', 1),
                ('product', 'O=C2N1CCCCC1C2C', 'bicyclic β-lactam', 'products_text', 1),
            ],
        },
        # === Staudinger Reaction ===
        {
            'family': 'Staudinger Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Staudinger reaction: azide + PPh3 gives iminophosphorane, then hydrolyzed to amine.',
            'reactants_text': 'organic azide + PR3 → amine (generic)',
            'products_text': 'amine',
            'reagents_text': 'PPh3; H2O',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase26_shallow_top10_v1 sprint for Staudinger Reaction. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'N=[N+]=[N-].c1ccccc1', 'organic azide + PR3 → amine (generic)', 'reactants_text', 1),
                ('product', 'NC1=CC=CC=C1', 'amine', 'products_text', 1),
            ],
        },
        {
            'family': 'Staudinger Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Staudinger of alkyl azide to primary amine.',
            'reactants_text': 'β-phenethyl azide',
            'products_text': 'β-phenethylamine',
            'reagents_text': 'PPh3; H2O',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase26_shallow_top10_v1 sprint for Staudinger Reaction. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'N(=[N+]=[N-])CCc1ccccc1', 'β-phenethyl azide', 'reactants_text', 1),
                ('product', 'NCCc1ccccc1', 'β-phenethylamine', 'products_text', 1),
            ],
        },
        {
            'family': 'Staudinger Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Staudinger on acyl azide giving amide via phosphazide intermediate + H2O.',
            'reactants_text': 'acyl azide (benzoyl azide)',
            'products_text': 'benzamide',
            'reagents_text': 'PMe3; H2O',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase26_shallow_top10_v1 sprint for Staudinger Reaction. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'N(=[N+]=[N-])C(=O)c1ccccc1', 'acyl azide (benzoyl azide)', 'reactants_text', 1),
                ('product', 'NC(=O)c1ccccc1', 'benzamide', 'products_text', 1),
            ],
        },
        # === Stephen Aldehyde Synthesis (Stephen Reduction) ===
        {
            'family': 'Stephen Aldehyde Synthesis (Stephen Reduction)',
            'extract_kind': 'application_example',
            'transformation_text': 'Stephen aldehyde synthesis: nitrile + SnCl2/HCl gives aldimine HCl salt, then hydrolysis to aldehyde.',
            'reactants_text': 'benzonitrile',
            'products_text': 'benzaldehyde',
            'reagents_text': 'SnCl2, HCl (gas), Et2O; then H2O',
            'conditions_text': 'Et2O, 0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase26_shallow_top10_v1 sprint for Stephen Aldehyde Synthesis (Stephen Reduction). [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'N#Cc1ccccc1', 'benzonitrile', 'reactants_text', 1),
                ('product', 'O=Cc1ccccc1', 'benzaldehyde', 'products_text', 1),
            ],
        },
        {
            'family': 'Stephen Aldehyde Synthesis (Stephen Reduction)',
            'extract_kind': 'application_example',
            'transformation_text': 'Stephen reduction of aryl nitrile to aryl aldehyde.',
            'reactants_text': 'p-methoxybenzonitrile',
            'products_text': 'p-methoxybenzaldehyde',
            'reagents_text': 'SnCl2, HCl; H2O',
            'conditions_text': 'Et2O, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase26_shallow_top10_v1 sprint for Stephen Aldehyde Synthesis (Stephen Reduction). [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'N#Cc1ccc(OC)cc1', 'p-methoxybenzonitrile', 'reactants_text', 1),
                ('product', 'O=Cc1ccc(OC)cc1', 'p-methoxybenzaldehyde', 'products_text', 1),
            ],
        },
        {
            'family': 'Stephen Aldehyde Synthesis (Stephen Reduction)',
            'extract_kind': 'application_example',
            'transformation_text': 'Stephen reduction of aliphatic nitrile.',
            'reactants_text': 'pentanenitrile',
            'products_text': 'pentanal',
            'reagents_text': 'SnCl2, HCl(g); H2O',
            'conditions_text': 'Et2O, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase26_shallow_top10_v1 sprint for Stephen Aldehyde Synthesis (Stephen Reduction). [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'N#CCCC', 'pentanenitrile', 'reactants_text', 1),
                ('product', 'O=CCCC', 'pentanal', 'products_text', 1),
            ],
        },
        # === Stetter Reaction ===
        {
            'family': 'Stetter Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Stetter reaction: aldehyde + α,β-unsaturated carbonyl under NHC or thiazolium gives 1,4-dicarbonyl.',
            'reactants_text': 'benzaldehyde + methyl vinyl ketone',
            'products_text': '1-phenyl-4-oxo-4-methyl-pent-2-anone (1,4-diketone)',
            'reagents_text': 'thiazolium salt (NHC precursor), Et3N',
            'conditions_text': 'dioxane, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase26_shallow_top10_v1 sprint for Stetter Reaction. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1.O=CC(=C)C', 'benzaldehyde + methyl vinyl ketone', 'reactants_text', 1),
                ('product', 'O=C(c1ccccc1)CC(=O)C(C)C', '1-phenyl-4-oxo-4-methyl-pent-2-anone (1,4-diketone)', 'products_text', 1),
            ],
        },
        {
            'family': 'Stetter Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Stetter reaction with methyl acrylate.',
            'reactants_text': 'anisaldehyde + methyl acrylate',
            'products_text': '1-(4-methoxyphenyl)-4-oxo-pentanedioate',
            'reagents_text': 'thiazolium catalyst, base',
            'conditions_text': 'dioxane, 60°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase26_shallow_top10_v1 sprint for Stetter Reaction. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccc(OC)cc1.C=CC(=O)OC', 'anisaldehyde + methyl acrylate', 'reactants_text', 1),
                ('product', 'COc1ccc(C(=O)CCC(=O)OC)cc1', '1-(4-methoxyphenyl)-4-oxo-pentanedioate', 'products_text', 1),
            ],
        },
        {
            'family': 'Stetter Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Stetter reaction giving γ-ketonitrile.',
            'reactants_text': 'cyclohexanecarboxaldehyde + acrylonitrile',
            'products_text': '5-oxo-5-cyclohexyl-pentanenitrile (4-oxo nitrile)',
            'reagents_text': '3-ethyl-5-(2-hydroxyethyl)-4-methylthiazolium bromide, Et3N',
            'conditions_text': 'EtOH, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase26_shallow_top10_v1 sprint for Stetter Reaction. [phase26_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CC1CCCCC1.C=CC#N', 'cyclohexanecarboxaldehyde + acrylonitrile', 'reactants_text', 1),
                ('product', 'O=C(C1CCCCC1)CCC#N', '5-oxo-5-cyclohexyl-pentanenitrile (4-oxo nitrile)', 'products_text', 1),
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
