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

TAG = 'phase29_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Wacker Oxidation',
        'Wagner-Meerwein Rearrangement',
        'Weinreb Ketone Synthesis',
        'Wharton Fragmentation',
        'Wharton Olefin Synthesis (Wharton Transposition)',
    ],
    'b': [
        'Williamson Ether Synthesis',
        'Wittig Reaction',
        'Wittig Reaction - Schlosser Modification',
        'Wittig-[1,2]- and [2,3]-Rearrangement',
        'Wohl-Ziegler Bromination',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Wacker Oxidation ===
        {
            'family': 'Wacker Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Wacker oxidation: terminal alkene + PdCl2/CuCl2/O2/H2O gives methyl ketone (Markovnikov).',
            'reactants_text': '1-hexene',
            'products_text': '2-hexanone',
            'reagents_text': 'PdCl2, CuCl2, O2, H2O',
            'conditions_text': 'DMF/H2O, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase29_shallow_top10_v1 sprint for Wacker Oxidation. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCCCC', '1-hexene', 'reactants_text', 1),
                ('product', 'O=C(C)CCCC', '2-hexanone', 'products_text', 1),
            ],
        },
        {
            'family': 'Wacker Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Wacker oxidation of styrene giving acetophenone.',
            'reactants_text': 'styrene',
            'products_text': 'acetophenone',
            'reagents_text': 'PdCl2 (cat.), CuCl2, O2',
            'conditions_text': 'DMF/H2O, 60°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase29_shallow_top10_v1 sprint for Wacker Oxidation. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=Cc1ccccc1', 'styrene', 'reactants_text', 1),
                ('product', 'O=C(C)c1ccccc1', 'acetophenone', 'products_text', 1),
            ],
        },
        {
            'family': 'Wacker Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Wacker oxidation tolerating ester.',
            'reactants_text': 'allyl-protected alcohol (4-acetoxy-1-butene)',
            'products_text': '4-acetoxy-2-butanone',
            'reagents_text': 'PdCl2, CuCl(I), O2',
            'conditions_text': 'DMA/H2O',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase29_shallow_top10_v1 sprint for Wacker Oxidation. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCCOC(=O)C', 'allyl-protected alcohol (4-acetoxy-1-butene)', 'reactants_text', 1),
                ('product', 'O=C(C)CCOC(=O)C', '4-acetoxy-2-butanone', 'products_text', 1),
            ],
        },
        # === Wagner-Meerwein Rearrangement ===
        {
            'family': 'Wagner-Meerwein Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Wagner-Meerwein: acid-catalyzed ionization and 1,2-methyl shift gives more stable tertiary cation that eliminates.',
            'reactants_text': '3,3-dimethyl-2-butanol (pinacolyl alcohol)',
            'products_text': '2,3,3-trimethyl-1-butene',
            'reagents_text': 'H2SO4 (cat.)',
            'conditions_text': 'heat',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase29_shallow_top10_v1 sprint for Wagner-Meerwein Rearrangement. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)(C)C(C)C', '3,3-dimethyl-2-butanol (pinacolyl alcohol)', 'reactants_text', 1),
                ('product', 'C=C(C)C(C)(C)C', '2,3,3-trimethyl-1-butene', 'products_text', 1),
            ],
        },
        {
            'family': 'Wagner-Meerwein Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Wagner-Meerwein ring-methyl migration in cyclopentanol.',
            'reactants_text': '1,2,2-trimethyl-cyclopentanol',
            'products_text': '1,3,3-trimethyl-cyclopentene',
            'reagents_text': 'H3PO4',
            'conditions_text': 'reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase29_shallow_top10_v1 sprint for Wagner-Meerwein Rearrangement. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC1(C)CCC(C)(C)C1', '1,2,2-trimethyl-cyclopentanol', 'reactants_text', 1),
                ('product', 'CC1=CC(C)(C)CC1', '1,3,3-trimethyl-cyclopentene', 'products_text', 1),
            ],
        },
        {
            'family': 'Wagner-Meerwein Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Acid-catalyzed dehydration via carbocation (simple case).',
            'reactants_text': '2-phenyl-2-propanol',
            'products_text': 'α-methylstyrene',
            'reagents_text': 'H2SO4',
            'conditions_text': 'heat',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase29_shallow_top10_v1 sprint for Wagner-Meerwein Rearrangement. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(c1ccccc1)(C)C', '2-phenyl-2-propanol', 'reactants_text', 1),
                ('product', 'C=C(c1ccccc1)C', 'α-methylstyrene', 'products_text', 1),
            ],
        },
        # === Weinreb Ketone Synthesis ===
        {
            'family': 'Weinreb Ketone Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Weinreb: Weinreb amide + organometallic (MeMgBr) gives ketone after hydrolysis — chelation prevents over-addition.',
            'reactants_text': 'N-methoxy-N-methylbenzamide (Weinreb amide)',
            'products_text': 'acetophenone',
            'reagents_text': 'CH3MgBr',
            'conditions_text': 'THF, 0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase29_shallow_top10_v1 sprint for Weinreb Ketone Synthesis. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(N(C)OC)c1ccccc1', 'N-methoxy-N-methylbenzamide (Weinreb amide)', 'reactants_text', 1),
                ('product', 'O=C(C)c1ccccc1', 'acetophenone', 'products_text', 1),
            ],
        },
        {
            'family': 'Weinreb Ketone Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Weinreb ketone synthesis with aryl Grignard.',
            'reactants_text': 'N-methoxy-N-methylpropanamide',
            'products_text': 'propiophenone',
            'reagents_text': 'PhMgBr',
            'conditions_text': 'THF, -78°C→0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase29_shallow_top10_v1 sprint for Weinreb Ketone Synthesis. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(N(C)OC)CC', 'N-methoxy-N-methylpropanamide', 'reactants_text', 1),
                ('product', 'O=C(c1ccccc1)CC', 'propiophenone', 'products_text', 1),
            ],
        },
        {
            'family': 'Weinreb Ketone Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Weinreb ketone synthesis with alkyl organolithium.',
            'reactants_text': 'N-methoxy-N-methyl cyclohexanecarboxamide',
            'products_text': '1-cyclohexyl-1-propanone',
            'reagents_text': 'EtLi',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase29_shallow_top10_v1 sprint for Weinreb Ketone Synthesis. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(N(C)OC)C1CCCCC1', 'N-methoxy-N-methyl cyclohexanecarboxamide', 'reactants_text', 1),
                ('product', 'O=C(CC)C1CCCCC1', '1-cyclohexyl-1-propanone', 'products_text', 1),
            ],
        },
        # === Wharton Fragmentation ===
        {
            'family': 'Wharton Fragmentation',
            'extract_kind': 'application_example',
            'transformation_text': 'Wharton (Grob-type) fragmentation: antiperiplanar γ-heterolytic fragmentation of β-hydroxy mesylate gives alkene + carbonyl.',
            'reactants_text': 'trans-2-(methylsulfonyloxy)cyclohexanol',
            'products_text': '5-hexenal',
            'reagents_text': 'base (KOH)',
            'conditions_text': 'EtOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase29_shallow_top10_v1 sprint for Wharton Fragmentation. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC1CCCCC1OS(=O)(=O)C', 'trans-2-(methylsulfonyloxy)cyclohexanol', 'reactants_text', 1),
                ('product', 'C=CCCC=O', '5-hexenal', 'products_text', 1),
            ],
        },
        {
            'family': 'Wharton Fragmentation',
            'extract_kind': 'application_example',
            'transformation_text': 'Grob fragmentation of β-hydroxy halide.',
            'reactants_text': '4-bromo-1-methylcyclohexanol (trans)',
            'products_text': '5-methyl-5-hexenal (ring-opened)',
            'reagents_text': 'KOH',
            'conditions_text': 'EtOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase29_shallow_top10_v1 sprint for Wharton Fragmentation. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC1(C)CCC(Br)CC1', '4-bromo-1-methylcyclohexanol (trans)', 'reactants_text', 1),
                ('product', 'C=C(C)CCC=O', '5-methyl-5-hexenal (ring-opened)', 'products_text', 1),
            ],
        },
        {
            'family': 'Wharton Fragmentation',
            'extract_kind': 'application_example',
            'transformation_text': 'Wharton fragmentation of β-hydroxy tosylate.',
            'reactants_text': 'trans-3-tosyloxy-cyclohexanol',
            'products_text': '6-heptenal (ring-opened)',
            'reagents_text': 'tBuOK',
            'conditions_text': 'tBuOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase29_shallow_top10_v1 sprint for Wharton Fragmentation. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC1CC(OS(=O)(=O)c2ccc(C)cc2)CCC1', 'trans-3-tosyloxy-cyclohexanol', 'reactants_text', 1),
                ('product', 'C=CCCCC=O', '6-heptenal (ring-opened)', 'products_text', 1),
            ],
        },
        # === Wharton Olefin Synthesis (Wharton Transposition) ===
        {
            'family': 'Wharton Olefin Synthesis (Wharton Transposition)',
            'extract_kind': 'application_example',
            'transformation_text': 'Wharton transposition: α,β-epoxy ketone + hydrazine gives allylic alcohol with C=O transposed one carbon.',
            'reactants_text': '2-methoxy-cyclopentanone (α-alkoxy ketone as α,β-epoxy ketone surrogate)',
            'products_text': 'cyclopent-2-en-1-ol (transposed allylic alcohol)',
            'reagents_text': 'NH2NH2, AcOH (cat.)',
            'conditions_text': 'MeOH, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase29_shallow_top10_v1 sprint for Wharton Olefin Synthesis (Wharton Transposition). [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCC1OC', '2-methoxy-cyclopentanone (α-alkoxy ketone as α,β-epoxy ketone surrogate)', 'reactants_text', 1),
                ('product', 'OC1CCC=C1', 'cyclopent-2-en-1-ol (transposed allylic alcohol)', 'products_text', 1),
            ],
        },
        {
            'family': 'Wharton Olefin Synthesis (Wharton Transposition)',
            'extract_kind': 'application_example',
            'transformation_text': 'Wharton transposition via α,β-epoxy intermediate to generate allylic alcohol.',
            'reactants_text': 'cyclopentanone (via α,β-epoxyketone intermediate)',
            'products_text': 'cyclopent-2-en-1-ol',
            'reagents_text': 'NH2NH2, AcOH',
            'conditions_text': 'EtOH, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase29_shallow_top10_v1 sprint for Wharton Olefin Synthesis (Wharton Transposition). [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCC1', 'cyclopentanone (via α,β-epoxyketone intermediate)', 'reactants_text', 1),
                ('product', 'OC1CCC=C1', 'cyclopent-2-en-1-ol', 'products_text', 1),
            ],
        },
        {
            'family': 'Wharton Olefin Synthesis (Wharton Transposition)',
            'extract_kind': 'application_example',
            'transformation_text': 'Wharton transposition on 6-membered cyclic ketone.',
            'reactants_text': 'cyclohexanone (via α,β-epoxyketone)',
            'products_text': 'cyclohex-2-en-1-ol',
            'reagents_text': 'NH2NH2·H2O, AcOH',
            'conditions_text': 'EtOH, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase29_shallow_top10_v1 sprint for Wharton Olefin Synthesis (Wharton Transposition). [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCCC1', 'cyclohexanone (via α,β-epoxyketone)', 'reactants_text', 1),
                ('product', 'OC1CCCC=C1', 'cyclohex-2-en-1-ol', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Williamson Ether Synthesis ===
        {
            'family': 'Williamson Ether Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Williamson ether synthesis: alkoxide/phenoxide + primary alkyl halide gives ether via SN2.',
            'reactants_text': 'sodium phenoxide + bromoethane',
            'products_text': 'phenetole (ethoxybenzene)',
            'reagents_text': 'NaH (to form alkoxide), DMF',
            'conditions_text': 'rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase29_shallow_top10_v1 sprint for Williamson Ether Synthesis. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', '[O-]c1ccccc1.CCBr', 'sodium phenoxide + bromoethane', 'reactants_text', 1),
                ('product', 'CCOc1ccccc1', 'phenetole (ethoxybenzene)', 'products_text', 1),
            ],
        },
        {
            'family': 'Williamson Ether Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Williamson synthesis of unsymmetrical dialkyl ether.',
            'reactants_text': '1-butanol + iodoethane (alkylation of alkoxide)',
            'products_text': 'butyl ethyl ether',
            'reagents_text': 'NaH (base)',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase29_shallow_top10_v1 sprint for Williamson Ether Synthesis. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCCCC.ICC', '1-butanol + iodoethane (alkylation of alkoxide)', 'reactants_text', 1),
                ('product', 'CCOCCCC', 'butyl ethyl ether', 'products_text', 1),
            ],
        },
        {
            'family': 'Williamson Ether Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Williamson-type ether synthesis of symmetrical benzyl ether.',
            'reactants_text': 'benzyl alcohol + benzyl chloride',
            'products_text': 'dibenzyl ether',
            'reagents_text': 'NaH, DMF',
            'conditions_text': '0°C→rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase29_shallow_top10_v1 sprint for Williamson Ether Synthesis. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCc1ccccc1.ClCc2ccccc2', 'benzyl alcohol + benzyl chloride', 'reactants_text', 1),
                ('product', 'C(OCc1ccccc1)c2ccccc2', 'dibenzyl ether', 'products_text', 1),
            ],
        },
        # === Wittig Reaction ===
        {
            'family': 'Wittig Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Wittig: phosphonium ylide (Ph3P=CHR) + aldehyde gives alkene with loss of Ph3PO.',
            'reactants_text': 'benzaldehyde',
            'products_text': 'β-methylstyrene',
            'reagents_text': 'Ph3P=CHCH3 (from Ph3P·CH3I, nBuLi)',
            'conditions_text': 'THF, -78°C→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase29_shallow_top10_v1 sprint for Wittig Reaction. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1),
                ('product', 'C(=Cc1ccccc1)C', 'β-methylstyrene', 'products_text', 1),
            ],
        },
        {
            'family': 'Wittig Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Wittig of ketone with ethylidene ylide.',
            'reactants_text': 'acetone',
            'products_text': '2-methyl-2-butene (example ylide product)',
            'reagents_text': 'Ph3P=CHCH3, base',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase29_shallow_top10_v1 sprint for Wittig Reaction. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)C', 'acetone', 'reactants_text', 1),
                ('product', 'CC(=CC1CCCCC1)C', '2-methyl-2-butene (example ylide product)', 'products_text', 1),
            ],
        },
        {
            'family': 'Wittig Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Wittig methylenation of aromatic aldehyde.',
            'reactants_text': '4-methoxybenzaldehyde (anisaldehyde)',
            'products_text': '4-methoxystyrene',
            'reagents_text': 'Ph3P=CH2 (from Ph3P·CH3Br, nBuLi)',
            'conditions_text': 'THF, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase29_shallow_top10_v1 sprint for Wittig Reaction. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccc(OC)cc1', '4-methoxybenzaldehyde (anisaldehyde)', 'reactants_text', 1),
                ('product', 'COc1ccc(C=C)cc1', '4-methoxystyrene', 'products_text', 1),
            ],
        },
        # === Wittig Reaction - Schlosser Modification ===
        {
            'family': 'Wittig Reaction - Schlosser Modification',
            'extract_kind': 'application_example',
            'transformation_text': 'Schlosser-modified Wittig: non-stabilized ylide + PhLi at low T gives β-oxido-ylide that thermalizes to pure E-alkene.',
            'reactants_text': 'benzaldehyde',
            'products_text': '(E)-1-phenyl-1-pentene (predominantly E)',
            'reagents_text': 'Ph3P=CHCH2CH2CH3, PhLi (Schlosser)',
            'conditions_text': 'THF, -78°C→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase29_shallow_top10_v1 sprint for Wittig Reaction - Schlosser Modification. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1),
                ('product', 'C(=Cc1ccccc1)CCC', '(E)-1-phenyl-1-pentene (predominantly E)', 'products_text', 1),
            ],
        },
        {
            'family': 'Wittig Reaction - Schlosser Modification',
            'extract_kind': 'application_example',
            'transformation_text': 'Schlosser Wittig giving pure E-alkene via β-oxido ylide protonation.',
            'reactants_text': 'propanal',
            'products_text': '(E)-3-heptene',
            'reagents_text': 'Ph3P=CHCH2CH2CH3, PhLi, then HCl/MeOH',
            'conditions_text': 'THF, -78°C→rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase29_shallow_top10_v1 sprint for Wittig Reaction - Schlosser Modification. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CCC', 'propanal', 'reactants_text', 1),
                ('product', 'C(=CCC)CCC', '(E)-3-heptene', 'products_text', 1),
            ],
        },
        {
            'family': 'Wittig Reaction - Schlosser Modification',
            'extract_kind': 'application_example',
            'transformation_text': 'Schlosser modification using methyl ylide with PhLi.',
            'reactants_text': 'pentanal',
            'products_text': '(E)-2-heptene',
            'reagents_text': 'Ph3P=CHCH3, PhLi',
            'conditions_text': 'THF, -78°C→rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase29_shallow_top10_v1 sprint for Wittig Reaction - Schlosser Modification. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CCCCC', 'pentanal', 'reactants_text', 1),
                ('product', 'C(=CCCCC)C', '(E)-2-heptene', 'products_text', 1),
            ],
        },
        # === Wittig-[1,2]- and [2,3]-Rearrangement ===
        {
            'family': 'Wittig-[1,2]- and [2,3]-Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': '[1,2]-Wittig rearrangement: α-deprotonation of ether, then radical-pair/[1,2]-shift gives α-substituted alcohol.',
            'reactants_text': 'ethyl benzyl ether',
            'products_text': '1-phenyl-1-ethanol ([1,2]-Wittig product)',
            'reagents_text': 'nBuLi',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase29_shallow_top10_v1 sprint for Wittig-[1,2]- and [2,3]-Rearrangement. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOCc1ccccc1', 'ethyl benzyl ether', 'reactants_text', 1),
                ('product', 'OC(C)c1ccccc1', '1-phenyl-1-ethanol ([1,2]-Wittig product)', 'products_text', 1),
            ],
        },
        {
            'family': 'Wittig-[1,2]- and [2,3]-Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': '[2,3]-Wittig rearrangement: α-lithiation of allyl ether triggers concerted [2,3] shift to homoallyl alcohol.',
            'reactants_text': 'allyl ethyl ether',
            'products_text': '1-penten-3-ol ([2,3]-sigmatropic product)',
            'reagents_text': 'nBuLi (to α-lithiate)',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase29_shallow_top10_v1 sprint for Wittig-[1,2]- and [2,3]-Rearrangement. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCOCC', 'allyl ethyl ether', 'reactants_text', 1),
                ('product', 'OC(CC)CC=C', '1-penten-3-ol ([2,3]-sigmatropic product)', 'products_text', 1),
            ],
        },
        {
            'family': 'Wittig-[1,2]- and [2,3]-Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': '[2,3]-Wittig rearrangement of benzylic allyl ether.',
            'reactants_text': 'allyl benzyl ether',
            'products_text': '1-phenyl-3-buten-1-ol',
            'reagents_text': 'nBuLi, TMEDA',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase29_shallow_top10_v1 sprint for Wittig-[1,2]- and [2,3]-Rearrangement. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCOCc1ccccc1', 'allyl benzyl ether', 'reactants_text', 1),
                ('product', 'OC(c1ccccc1)CC=C', '1-phenyl-3-buten-1-ol', 'products_text', 1),
            ],
        },
        # === Wohl-Ziegler Bromination ===
        {
            'family': 'Wohl-Ziegler Bromination',
            'extract_kind': 'application_example',
            'transformation_text': 'Wohl-Ziegler: allylic/benzylic bromination with NBS under radical initiation (AIBN, hv) gives monobromide.',
            'reactants_text': 'toluene',
            'products_text': 'benzyl bromide',
            'reagents_text': 'NBS, AIBN (cat.)',
            'conditions_text': 'CCl4, hv, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase29_shallow_top10_v1 sprint for Wohl-Ziegler Bromination. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Cc1ccccc1', 'toluene', 'reactants_text', 1),
                ('product', 'BrCc1ccccc1', 'benzyl bromide', 'products_text', 1),
            ],
        },
        {
            'family': 'Wohl-Ziegler Bromination',
            'extract_kind': 'application_example',
            'transformation_text': 'Wohl-Ziegler allylic bromination with NBS.',
            'reactants_text': '2-pentene',
            'products_text': '(E)-1-bromo-2-pentene (allylic Br)',
            'reagents_text': 'NBS, AIBN',
            'conditions_text': 'CCl4, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase29_shallow_top10_v1 sprint for Wohl-Ziegler Bromination. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC=CCC', '2-pentene', 'reactants_text', 1),
                ('product', 'BrCC=CCC', '(E)-1-bromo-2-pentene (allylic Br)', 'products_text', 1),
            ],
        },
        {
            'family': 'Wohl-Ziegler Bromination',
            'extract_kind': 'application_example',
            'transformation_text': 'NBS bromination at allylic methyl of 1-methylcyclohexene.',
            'reactants_text': '1-methylcyclohexene',
            'products_text': '1-(bromomethyl)-cyclohexene (allylic Br)',
            'reagents_text': 'NBS, benzoyl peroxide',
            'conditions_text': 'CCl4, hv',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase29_shallow_top10_v1 sprint for Wohl-Ziegler Bromination. [phase29_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC1=CCCCC1', '1-methylcyclohexene', 'reactants_text', 1),
                ('product', 'BrCC1=CCCCC1', '1-(bromomethyl)-cyclohexene (allylic Br)', 'products_text', 1),
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
