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

TAG = 'phase28_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Takai-Utimoto Olefination (Takai Reaction)',
        'Tebbe Olefination/Petasis-Tebbe Olefination',
        'Tishchenko Reaction',
        'Tsuji-Trost Reaction / Allylation',
        'Tsuji-Wilkinson Decarbonylation Reaction',
    ],
    'b': [
        'Ugi Multicomponent Reaction',
        'Ullmann Biaryl Ether and Biaryl Amine Synthesis / Condensation',
        'Ullmann Reaction / Coupling / Biaryl Synthesis',
        'Vilsmeier-Haack Formylation',
        'Vinylcyclopropane-Cyclopentene Rearrangement',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Takai-Utimoto Olefination (Takai Reaction) ===
        {
            'family': 'Takai-Utimoto Olefination (Takai Reaction)',
            'extract_kind': 'application_example',
            'transformation_text': 'Takai reaction: aldehyde + CHI3/CrCl2 gives (E)-vinyl iodide.',
            'reactants_text': 'benzaldehyde',
            'products_text': '(E)-β-iodostyrene',
            'reagents_text': 'CHI3, CrCl2 (excess)',
            'conditions_text': 'THF/dioxane, 0°C→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase28_shallow_top10_v1 sprint for Takai-Utimoto Olefination (Takai Reaction). [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1),
                ('product', 'C(=Cc1ccccc1)I', '(E)-β-iodostyrene', 'products_text', 1),
            ],
        },
        {
            'family': 'Takai-Utimoto Olefination (Takai Reaction)',
            'extract_kind': 'application_example',
            'transformation_text': 'Takai-Utimoto olefination of aliphatic aldehyde to (E)-vinyl iodide.',
            'reactants_text': 'propanal',
            'products_text': '(E)-1-iodo-1-butene',
            'reagents_text': 'CHI3, CrCl2',
            'conditions_text': 'THF, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase28_shallow_top10_v1 sprint for Takai-Utimoto Olefination (Takai Reaction). [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CCC', 'propanal', 'reactants_text', 1),
                ('product', 'C(=CCC)I', '(E)-1-iodo-1-butene', 'products_text', 1),
            ],
        },
        {
            'family': 'Takai-Utimoto Olefination (Takai Reaction)',
            'extract_kind': 'application_example',
            'transformation_text': 'Takai olefination giving (E)-vinyl iodide from branched aliphatic aldehyde.',
            'reactants_text': 'cyclohexanecarboxaldehyde',
            'products_text': '(E)-1-iodo-2-cyclohexyl-ethene',
            'reagents_text': 'CHI3, CrCl2',
            'conditions_text': 'dioxane/THF, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase28_shallow_top10_v1 sprint for Takai-Utimoto Olefination (Takai Reaction). [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CC1CCCCC1', 'cyclohexanecarboxaldehyde', 'reactants_text', 1),
                ('product', 'C(=CC1CCCCC1)I', '(E)-1-iodo-2-cyclohexyl-ethene', 'products_text', 1),
            ],
        },
        # === Tebbe Olefination/Petasis-Tebbe Olefination ===
        {
            'family': 'Tebbe Olefination/Petasis-Tebbe Olefination',
            'extract_kind': 'application_example',
            'transformation_text': 'Tebbe olefination: ester/ketone + Cp2Ti=CH2 (Tebbe reagent) gives methylene substitution of C=O.',
            'reactants_text': 'methyl benzoate',
            'products_text': 'styrene-type methylidene (isopropenyl-aryl product represented as α-methyleneenol ether)',
            'reagents_text': 'Tebbe reagent (Cp2Ti(Cl)CH2AlMe2), DMAP',
            'conditions_text': 'toluene, -40°C→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase28_shallow_top10_v1 sprint for Tebbe Olefination/Petasis-Tebbe Olefination. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(OC)c1ccccc1', 'methyl benzoate', 'reactants_text', 1),
                ('product', 'C(=C)c1ccccc1', 'styrene-type methylidene (isopropenyl-aryl product represented as α-methyleneenol ether)', 'products_text', 1),
            ],
        },
        {
            'family': 'Tebbe Olefination/Petasis-Tebbe Olefination',
            'extract_kind': 'application_example',
            'transformation_text': 'Tebbe/Petasis-Tebbe methylenation of aryl methyl ketone.',
            'reactants_text': 'acetophenone',
            'products_text': 'α-methylene styrene (methylenation of C=O)',
            'reagents_text': 'Petasis reagent (Cp2TiMe2)',
            'conditions_text': 'toluene, 60°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase28_shallow_top10_v1 sprint for Tebbe Olefination/Petasis-Tebbe Olefination. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)c1ccccc1', 'acetophenone', 'reactants_text', 1),
                ('product', 'C(=C)c1ccccc1', 'α-methylene styrene (methylenation of C=O)', 'products_text', 1),
            ],
        },
        {
            'family': 'Tebbe Olefination/Petasis-Tebbe Olefination',
            'extract_kind': 'application_example',
            'transformation_text': 'Petasis-Tebbe methylenation of cyclic ketone.',
            'reactants_text': 'cyclohexanone',
            'products_text': 'methylenecyclohexane',
            'reagents_text': 'Cp2TiMe2',
            'conditions_text': 'toluene, 65°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase28_shallow_top10_v1 sprint for Tebbe Olefination/Petasis-Tebbe Olefination. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCCC1', 'cyclohexanone', 'reactants_text', 1),
                ('product', 'C1=CCCCC1', 'methylenecyclohexane', 'products_text', 1),
            ],
        },
        # === Tishchenko Reaction ===
        {
            'family': 'Tishchenko Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Tishchenko reaction: two aldehydes combine under aluminum alkoxide to give ester (disproportionation).',
            'reactants_text': 'benzaldehyde (disproportionation)',
            'products_text': 'benzyl benzoate',
            'reagents_text': 'Al(OEt)3 (cat.)',
            'conditions_text': 'benzene, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase28_shallow_top10_v1 sprint for Tishchenko Reaction. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1', 'benzaldehyde (disproportionation)', 'reactants_text', 1),
                ('product', 'O=C(OCc1ccccc1)c2ccccc2', 'benzyl benzoate', 'products_text', 1),
            ],
        },
        {
            'family': 'Tishchenko Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Tishchenko coupling of propanal to propyl propanoate.',
            'reactants_text': 'propanal (disproportionation)',
            'products_text': 'propyl propanoate',
            'reagents_text': 'Al(OEt)3',
            'conditions_text': 'neat, 50°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase28_shallow_top10_v1 sprint for Tishchenko Reaction. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CCC', 'propanal (disproportionation)', 'reactants_text', 1),
                ('product', 'O=C(OCCC)CC', 'propyl propanoate', 'products_text', 1),
            ],
        },
        {
            'family': 'Tishchenko Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Tishchenko disproportionation of branched aldehyde.',
            'reactants_text': 'cyclohexanecarboxaldehyde (disproportionation)',
            'products_text': 'cyclohexylmethyl cyclohexanecarboxylate',
            'reagents_text': 'Al(OEt)3 or Sm(II)',
            'conditions_text': 'THF or toluene, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase28_shallow_top10_v1 sprint for Tishchenko Reaction. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CC1CCCCC1', 'cyclohexanecarboxaldehyde (disproportionation)', 'reactants_text', 1),
                ('product', 'O=C(OCC1CCCCC1)C2CCCCC2', 'cyclohexylmethyl cyclohexanecarboxylate', 'products_text', 1),
            ],
        },
        # === Tsuji-Trost Reaction / Allylation ===
        {
            'family': 'Tsuji-Trost Reaction / Allylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Tsuji-Trost allylation: Pd π-allyl + soft nucleophile (malonate) gives allylation with loss of leaving group.',
            'reactants_text': 'allyl acetate + diethyl malonate',
            'products_text': 'diethyl 2-allyl-malonate',
            'reagents_text': 'Pd(PPh3)4, NaH',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase28_shallow_top10_v1 sprint for Tsuji-Trost Reaction / Allylation. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCOC(=O)C.CCOC(=O)CC(=O)OCC', 'allyl acetate + diethyl malonate', 'reactants_text', 1),
                ('product', 'CCOC(=O)C(CC=C)C(=O)OCC', 'diethyl 2-allyl-malonate', 'products_text', 1),
            ],
        },
        {
            'family': 'Tsuji-Trost Reaction / Allylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Tsuji-Trost amination with secondary amine.',
            'reactants_text': 'allyl acetate + piperidine',
            'products_text': 'N-allylpiperidine',
            'reagents_text': 'Pd(PPh3)4',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase28_shallow_top10_v1 sprint for Tsuji-Trost Reaction / Allylation. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCOC(=O)C.N1CCCCC1', 'allyl acetate + piperidine', 'reactants_text', 1),
                ('product', 'N1(CC=C)CCCCC1', 'N-allylpiperidine', 'products_text', 1),
            ],
        },
        {
            'family': 'Tsuji-Trost Reaction / Allylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Tsuji-Trost allylation with dimethyl malonate.',
            'reactants_text': 'allyl acetate + dimethyl malonate',
            'products_text': 'dimethyl 2-allyl-malonate',
            'reagents_text': 'Pd(PPh3)4, BSA',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase28_shallow_top10_v1 sprint for Tsuji-Trost Reaction / Allylation. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCOC(=O)C.C(C(=O)OC)C(=O)OC', 'allyl acetate + dimethyl malonate', 'reactants_text', 1),
                ('product', 'C=CCC(C(=O)OC)C(=O)OC', 'dimethyl 2-allyl-malonate', 'products_text', 1),
            ],
        },
        # === Tsuji-Wilkinson Decarbonylation Reaction ===
        {
            'family': 'Tsuji-Wilkinson Decarbonylation Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Tsuji-Wilkinson decarbonylation: aldehyde + Rh(PPh3)3Cl removes CO to give arene (or alkane).',
            'reactants_text': 'benzaldehyde',
            'products_text': 'benzene',
            'reagents_text': "RhCl(PPh3)3 (Wilkinson's)",
            'conditions_text': 'xylene, 130°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase28_shallow_top10_v1 sprint for Tsuji-Wilkinson Decarbonylation Reaction. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1),
                ('product', 'c1ccccc1', 'benzene', 'products_text', 1),
            ],
        },
        {
            'family': 'Tsuji-Wilkinson Decarbonylation Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Tsuji-Wilkinson decarbonylation of aliphatic aldehyde.',
            'reactants_text': 'heptanal',
            'products_text': 'hexane',
            'reagents_text': 'RhCl(PPh3)3, dppp',
            'conditions_text': 'diglyme, 180°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase28_shallow_top10_v1 sprint for Tsuji-Wilkinson Decarbonylation Reaction. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CCCCCCC', 'heptanal', 'reactants_text', 1),
                ('product', 'CCCCCCC', 'hexane', 'products_text', 1),
            ],
        },
        {
            'family': 'Tsuji-Wilkinson Decarbonylation Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Tsuji-Wilkinson decarbonylation of cyclic aldehyde.',
            'reactants_text': 'cyclohexanecarboxaldehyde',
            'products_text': 'cyclohexane',
            'reagents_text': 'RhCl(PPh3)3',
            'conditions_text': 'xylene, 150°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase28_shallow_top10_v1 sprint for Tsuji-Wilkinson Decarbonylation Reaction. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CC1CCCCC1', 'cyclohexanecarboxaldehyde', 'reactants_text', 1),
                ('product', 'C1CCCCC1', 'cyclohexane', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Ugi Multicomponent Reaction ===
        {
            'family': 'Ugi Multicomponent Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Ugi four-component reaction: carbonyl + amine + acid + isocyanide gives α-acylamino amide.',
            'reactants_text': 'benzaldehyde + aniline + acetic acid + tert-butyl isocyanide',
            'products_text': 'Ugi product α-acyloxy-α-amino amide',
            'reagents_text': 'MeOH',
            'conditions_text': 'rt, 24–48 h',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase28_shallow_top10_v1 sprint for Ugi Multicomponent Reaction. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1.Nc2ccccc2.OC(=O)C.[C-]#[N+]C(C)(C)C', 'benzaldehyde + aniline + acetic acid + tert-butyl isocyanide', 'reactants_text', 1),
                ('product', 'O=C(NC(=O)C(c1ccccc1)NC2=CC=CC=C2)NC(C)(C)C', 'Ugi product α-acyloxy-α-amino amide', 'products_text', 1),
            ],
        },
        {
            'family': 'Ugi Multicomponent Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Ugi-4CR with ketone, benzylamine, benzoic acid, isocyanide.',
            'reactants_text': 'butanone + benzylamine + benzoic acid + butyl isocyanide',
            'products_text': 'Ugi MCR product (tri-substituted peptide mimic)',
            'reagents_text': 'MeOH',
            'conditions_text': 'rt, 48 h',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase28_shallow_top10_v1 sprint for Ugi Multicomponent Reaction. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)C.NCc1ccccc1.OC(=O)c2ccccc2.[C-]#[N+]CCCC', 'butanone + benzylamine + benzoic acid + butyl isocyanide', 'reactants_text', 1),
                ('product', 'O=C(NC(CC)(C)C(=O)NCCCC)NCc1ccccc1', 'Ugi MCR product (tri-substituted peptide mimic)', 'products_text', 1),
            ],
        },
        {
            'family': 'Ugi Multicomponent Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Ugi-4CR with primary amine = ammonia (gives α-amino amide).',
            'reactants_text': 'cyclohexanecarboxaldehyde + NH3 + propanoic acid + phenyl isocyanide',
            'products_text': 'Ugi-4CR scaffold',
            'reagents_text': 'MeOH',
            'conditions_text': 'rt, 24 h',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase28_shallow_top10_v1 sprint for Ugi Multicomponent Reaction. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CC1CCCCC1.N.OC(=O)CC.[C-]#[N+]c1ccccc1', 'cyclohexanecarboxaldehyde + NH3 + propanoic acid + phenyl isocyanide', 'reactants_text', 1),
                ('product', 'O=C(NC(=O)C(NC)(C1CCCCC1))Nc1ccccc1', 'Ugi-4CR scaffold', 'products_text', 1),
            ],
        },
        # === Ullmann Biaryl Ether and Biaryl Amine Synthesis / Condensation ===
        {
            'family': 'Ullmann Biaryl Ether and Biaryl Amine Synthesis / Condensation',
            'extract_kind': 'application_example',
            'transformation_text': 'Ullmann-type ether synthesis: aryl halide + phenol with Cu catalysis gives diaryl ether.',
            'reactants_text': 'iodobenzene + phenol',
            'products_text': 'diphenyl ether',
            'reagents_text': 'Cu powder, K2CO3',
            'conditions_text': 'DMF, 150°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase28_shallow_top10_v1 sprint for Ullmann Biaryl Ether and Biaryl Amine Synthesis / Condensation. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1ccccc1.Oc2ccccc2', 'iodobenzene + phenol', 'reactants_text', 1),
                ('product', 'c1ccc(Oc2ccccc2)cc1', 'diphenyl ether', 'products_text', 1),
            ],
        },
        {
            'family': 'Ullmann Biaryl Ether and Biaryl Amine Synthesis / Condensation',
            'extract_kind': 'application_example',
            'transformation_text': 'Ullmann aminodehalogenation to diaryl amine.',
            'reactants_text': 'p-bromoanisole + aniline',
            'products_text': '4-methoxy-N-phenylaniline',
            'reagents_text': 'CuI, DMEDA, Cs2CO3',
            'conditions_text': 'dioxane, 110°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase28_shallow_top10_v1 sprint for Ullmann Biaryl Ether and Biaryl Amine Synthesis / Condensation. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccc(OC)cc1.Nc2ccccc2', 'p-bromoanisole + aniline', 'reactants_text', 1),
                ('product', 'COc1ccc(Nc2ccccc2)cc1', '4-methoxy-N-phenylaniline', 'products_text', 1),
            ],
        },
        {
            'family': 'Ullmann Biaryl Ether and Biaryl Amine Synthesis / Condensation',
            'extract_kind': 'application_example',
            'transformation_text': 'Ullmann diaryl amine synthesis.',
            'reactants_text': 'iodobenzene + p-toluidine',
            'products_text': 'N-phenyl-p-toluidine',
            'reagents_text': 'CuI, L-proline, K2CO3',
            'conditions_text': 'DMSO, 100°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase28_shallow_top10_v1 sprint for Ullmann Biaryl Ether and Biaryl Amine Synthesis / Condensation. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1ccccc1.Nc2ccc(C)cc2', 'iodobenzene + p-toluidine', 'reactants_text', 1),
                ('product', 'Cc1ccc(Nc2ccccc2)cc1', 'N-phenyl-p-toluidine', 'products_text', 1),
            ],
        },
        # === Ullmann Reaction / Coupling / Biaryl Synthesis ===
        {
            'family': 'Ullmann Reaction / Coupling / Biaryl Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Ullmann biaryl coupling: aryl iodide homocoupling with activated Cu.',
            'reactants_text': 'iodobenzene (dimerization)',
            'products_text': 'biphenyl',
            'reagents_text': 'Cu bronze',
            'conditions_text': 'DMF, 200°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase28_shallow_top10_v1 sprint for Ullmann Reaction / Coupling / Biaryl Synthesis. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1ccccc1', 'iodobenzene (dimerization)', 'reactants_text', 1),
                ('product', 'c1ccc(-c2ccccc2)cc1', 'biphenyl', 'products_text', 1),
            ],
        },
        {
            'family': 'Ullmann Reaction / Coupling / Biaryl Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Ullmann homocoupling of aryl halide to symmetric biphenyl.',
            'reactants_text': 'p-bromotoluene (homocoupling)',
            'products_text': "4,4'-dimethylbiphenyl",
            'reagents_text': 'Cu powder',
            'conditions_text': 'DMF, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase28_shallow_top10_v1 sprint for Ullmann Reaction / Coupling / Biaryl Synthesis. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccc(C)cc1', 'p-bromotoluene (homocoupling)', 'reactants_text', 1),
                ('product', 'Cc1ccc(-c2ccc(C)cc2)cc1', "4,4'-dimethylbiphenyl", 'products_text', 1),
            ],
        },
        {
            'family': 'Ullmann Reaction / Coupling / Biaryl Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Ullmann coupling of electron-poor aryl iodide.',
            'reactants_text': 'p-iodonitrobenzene (homocoupling)',
            'products_text': "4,4'-dinitrobiphenyl",
            'reagents_text': 'Cu bronze',
            'conditions_text': '200°C, neat',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase28_shallow_top10_v1 sprint for Ullmann Reaction / Coupling / Biaryl Synthesis. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1ccc([N+](=O)[O-])cc1', 'p-iodonitrobenzene (homocoupling)', 'reactants_text', 1),
                ('product', 'O=[N+]([O-])c1ccc(-c2ccc([N+](=O)[O-])cc2)cc1', "4,4'-dinitrobiphenyl", 'products_text', 1),
            ],
        },
        # === Vilsmeier-Haack Formylation ===
        {
            'family': 'Vilsmeier-Haack Formylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Vilsmeier-Haack: DMF + POCl3 gives chloroiminium electrophile that formylates electron-rich arene.',
            'reactants_text': 'anisole',
            'products_text': '4-methoxybenzaldehyde (p-anisaldehyde)',
            'reagents_text': 'DMF, POCl3',
            'conditions_text': 'DMF, 60°C; then H2O workup',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase28_shallow_top10_v1 sprint for Vilsmeier-Haack Formylation. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'COc1ccccc1', 'anisole', 'reactants_text', 1),
                ('product', 'COc1ccccc1C=O', '4-methoxybenzaldehyde (p-anisaldehyde)', 'products_text', 1),
            ],
        },
        {
            'family': 'Vilsmeier-Haack Formylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Vilsmeier-Haack formylation of indole at C3.',
            'reactants_text': 'indole',
            'products_text': 'indole-3-carbaldehyde',
            'reagents_text': 'DMF, POCl3',
            'conditions_text': 'DMF, 25°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase28_shallow_top10_v1 sprint for Vilsmeier-Haack Formylation. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccc2[nH]ccc2c1', 'indole', 'reactants_text', 1),
                ('product', 'O=Cc1cc2ccccc2[nH]1', 'indole-3-carbaldehyde', 'products_text', 1),
            ],
        },
        {
            'family': 'Vilsmeier-Haack Formylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Vilsmeier-Haack formylation of activated aromatic amine.',
            'reactants_text': 'N,N-dimethylaniline',
            'products_text': '4-(dimethylamino)benzaldehyde',
            'reagents_text': 'DMF, POCl3',
            'conditions_text': 'neat, 60°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase28_shallow_top10_v1 sprint for Vilsmeier-Haack Formylation. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccc(N(C)C)cc1', 'N,N-dimethylaniline', 'reactants_text', 1),
                ('product', 'O=Cc1ccc(N(C)C)cc1', '4-(dimethylamino)benzaldehyde', 'products_text', 1),
            ],
        },
        # === Vinylcyclopropane-Cyclopentene Rearrangement ===
        {
            'family': 'Vinylcyclopropane-Cyclopentene Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Vinylcyclopropane-cyclopentene rearrangement: thermal or metal-catalyzed ring expansion giving cyclopentene.',
            'reactants_text': 'vinylcyclopropane',
            'products_text': 'cyclopentene',
            'reagents_text': 'thermal (ΔT) or Rh(I) catalyst',
            'conditions_text': 'sealed tube, 350°C or Rh(I)',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase28_shallow_top10_v1 sprint for Vinylcyclopropane-Cyclopentene Rearrangement. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CC1CC1', 'vinylcyclopropane', 'reactants_text', 1),
                ('product', 'C1=CCCC1', 'cyclopentene', 'products_text', 1),
            ],
        },
        {
            'family': 'Vinylcyclopropane-Cyclopentene Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Thermal vinylcyclopropane→cyclopentene rearrangement of α-methylated substrate.',
            'reactants_text': '1-methyl-vinylcyclopropane',
            'products_text': '1-methylcyclopentene',
            'reagents_text': 'ΔT',
            'conditions_text': 'neat, 350°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase28_shallow_top10_v1 sprint for Vinylcyclopropane-Cyclopentene Rearrangement. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CC1(C)CC1', '1-methyl-vinylcyclopropane', 'reactants_text', 1),
                ('product', 'CC1=CCCC1', '1-methylcyclopentene', 'products_text', 1),
            ],
        },
        {
            'family': 'Vinylcyclopropane-Cyclopentene Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Vinylcyclopropane rearrangement with aryl substitution.',
            'reactants_text': 'α-phenyl-vinylcyclopropane',
            'products_text': '1-phenyl-cyclopentene',
            'reagents_text': 'Rh(PPh3)3Cl or thermal',
            'conditions_text': 'xylene, 250°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase28_shallow_top10_v1 sprint for Vinylcyclopropane-Cyclopentene Rearrangement. [phase28_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccc(C(=C)C2CC2)cc1', 'α-phenyl-vinylcyclopropane', 'reactants_text', 1),
                ('product', 'C1(c2ccccc2)=CCCC1', '1-phenyl-cyclopentene', 'products_text', 1),
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
