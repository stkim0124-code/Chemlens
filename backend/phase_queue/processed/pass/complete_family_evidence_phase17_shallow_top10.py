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

TAG = 'phase17_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Heine Reaction',
        'Intramolecular Nitrile Oxide Cycloaddition',
        'Kagan-Molander Samarium Diiodide-Mediated Coupling',
        'Kahne Glycosidation',
        'Keck Asymmetric Allylation',
    ],
    'b': [
        'Keck Macrolactonization',
        'Keck Radical Allylation',
        'Koenigs-Knorr Glycosidation',
        'Kolbe-Schmitt Reaction',
        'Kröhnke Pyridine Synthesis',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Heine Reaction ===
        {
            'family': 'Heine Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Heine reaction: N-benzoyl-2-methylaziridine rearranges to 4-methyl-2-phenyl-oxazoline via N→O acyl shift with ring expansion.',
            'reactants_text': 'N-benzoyl-2-methylaziridine',
            'products_text': '4-methyl-2-phenyl-oxazoline',
            'reagents_text': 'BF3·OEt2 (Lewis acid)',
            'conditions_text': 'N-acyl aziridine Lewis-acid-catalyzed rearrangement, refluxing benzene',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase17_shallow_top10_v1 sprint for Heine Reaction. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(N1CC1C)c1ccccc1', 'N-benzoyl-2-methylaziridine', 'reactants_text', 1),
                ('product', 'CC1COC(=N1)c1ccccc1', '4-methyl-2-phenyl-oxazoline', 'products_text', 1),
            ],
        },
        {
            'family': 'Heine Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Heine reaction: N-acetyl-2-phenylaziridine rearranges to 2-methyl-4-phenyl-oxazoline via iodide-assisted ring opening.',
            'reactants_text': 'N-acetyl-2-phenylaziridine',
            'products_text': '2-methyl-4-phenyl-oxazoline',
            'reagents_text': 'NaI (catalytic), acetone',
            'conditions_text': 'NaI-catalyzed Heine rearrangement at reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase17_shallow_top10_v1 sprint for Heine Reaction. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(=O)N1CC1c1ccccc1', 'N-acetyl-2-phenylaziridine', 'reactants_text', 1),
                ('product', 'CC1=NC(c2ccccc2)CO1', '2-methyl-4-phenyl-oxazoline', 'products_text', 1),
            ],
        },
        {
            'family': 'Heine Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Heine reaction: parent case, N-benzoyl aziridine rearranges to 2-phenyl-oxazoline.',
            'reactants_text': 'N-benzoyl aziridine',
            'products_text': '2-phenyl-oxazoline',
            'reagents_text': 'Lewis acid or iodide catalyst',
            'conditions_text': 'thermal rearrangement',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase17_shallow_top10_v1 sprint for Heine Reaction. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(N1CC1)c1ccccc1', 'N-benzoyl aziridine', 'reactants_text', 1),
                ('product', 'C1COC(=N1)c1ccccc1', '2-phenyl-oxazoline', 'products_text', 1),
            ],
        },
        # === Intramolecular Nitrile Oxide Cycloaddition ===
        {
            'family': 'Intramolecular Nitrile Oxide Cycloaddition',
            'extract_kind': 'application_example',
            'transformation_text': 'INOC: aldoxime oxidized to nitrile oxide then intramolecular [3+2] to tethered alkene gives fused isoxazoline.',
            'reactants_text': '(E)-6-hydroxyiminohex-1-ene with methyl terminus',
            'products_text': 'fused bicyclic isoxazoline',
            'reagents_text': 'NaOCl (generates nitrile oxide from oxime)',
            'conditions_text': 'NaOCl/CH2Cl2, 0°C, slow addition',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase17_shallow_top10_v1 sprint for Intramolecular Nitrile Oxide Cycloaddition. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C(=C\\CCC/C=N/O)/C', '(E)-6-hydroxyiminohex-1-ene with methyl terminus', 'reactants_text', 1),
                ('product', 'CC1CCCC2CC(=NO2)C1', 'fused bicyclic isoxazoline', 'products_text', 1),
            ],
        },
        {
            'family': 'Intramolecular Nitrile Oxide Cycloaddition',
            'extract_kind': 'application_example',
            'transformation_text': 'INOC: methyl ketoxime dehydrogenated in situ to nitrile oxide, intramolecular [3+2] onto pendant alkene.',
            'reactants_text': '5-hexenyl methyl ketoxime',
            'products_text': 'fused isoxazoline from methyl ketoxime',
            'reagents_text': 'PhNCO, Et3N (dehydrating)',
            'conditions_text': 'Mukaiyama-Hoshino conditions, toluene reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase17_shallow_top10_v1 sprint for Intramolecular Nitrile Oxide Cycloaddition. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCCCCC(=NO)C', '5-hexenyl methyl ketoxime', 'reactants_text', 1),
                ('product', 'CC1=NOC2CCCCC12', 'fused isoxazoline from methyl ketoxime', 'products_text', 1),
            ],
        },
        {
            'family': 'Intramolecular Nitrile Oxide Cycloaddition',
            'extract_kind': 'application_example',
            'transformation_text': 'INOC: α-keto oxime activated to nitrile oxide, intramolecular cycloaddition to terminal olefin.',
            'reactants_text': '6-heptenoyl oxime with keto tether',
            'products_text': 'bicyclic isoxazoline with α-keto substitution',
            'reagents_text': 'NCS, Et3N',
            'conditions_text': 'chlorination-dehydrochlorination route',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase17_shallow_top10_v1 sprint for Intramolecular Nitrile Oxide Cycloaddition. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(/C=N/O)CCCC=C', '6-heptenoyl oxime with keto tether', 'reactants_text', 1),
                ('product', 'O=C1CC2CC(=NO2)CCC1', 'bicyclic isoxazoline with α-keto substitution', 'products_text', 1),
            ],
        },
        # === Kagan-Molander Samarium Diiodide-Mediated Coupling ===
        {
            'family': 'Kagan-Molander Samarium Diiodide-Mediated Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'SmI2-mediated pinacol cross-coupling: acetophenone and acetone couple to a 1,2-diol.',
            'reactants_text': 'acetophenone + acetone',
            'products_text': 'pinacol-type diol',
            'reagents_text': 'SmI2 (2 equiv), HMPA',
            'conditions_text': 'THF, rt, anhydrous',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase17_shallow_top10_v1 sprint for Kagan-Molander Samarium Diiodide-Mediated Coupling. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)c1ccccc1.O=C(C)C', 'acetophenone + acetone', 'reactants_text', 1),
                ('product', 'CC(O)(c1ccccc1)C(O)(C)C', 'pinacol-type diol', 'products_text', 1),
            ],
        },
        {
            'family': 'Kagan-Molander Samarium Diiodide-Mediated Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Kagan-Molander intramolecular pinacol: 1,6-dialdehyde cyclized to cis-1,2-diol by SmI2.',
            'reactants_text': 'hexanedial',
            'products_text': 'cyclohexane-1,2-diol',
            'reagents_text': 'SmI2, HMPA',
            'conditions_text': 'THF, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase17_shallow_top10_v1 sprint for Kagan-Molander Samarium Diiodide-Mediated Coupling. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CCCCCC=O', 'hexanedial', 'reactants_text', 1),
                ('product', 'OC1CCCCC1O', 'cyclohexane-1,2-diol', 'products_text', 1),
            ],
        },
        {
            'family': 'Kagan-Molander Samarium Diiodide-Mediated Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'SmI2 ketyl-olefin radical cyclization (Molander variant): aryl ketone to cyclopentanol.',
            'reactants_text': '1-phenyl-5-hexen-1-one',
            'products_text': '1-phenyl-2-methylcyclopentanol',
            'reagents_text': 'SmI2, HMPA, t-BuOH',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase17_shallow_top10_v1 sprint for Kagan-Molander Samarium Diiodide-Mediated Coupling. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(CCCC=C)c1ccccc1', '1-phenyl-5-hexen-1-one', 'reactants_text', 1),
                ('product', 'OC1(c2ccccc2)CCCC1', '1-phenyl-2-methylcyclopentanol', 'products_text', 1),
            ],
        },
        # === Kahne Glycosidation ===
        {
            'family': 'Kahne Glycosidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Kahne sulfoxide glycosidation: anomeric phenyl sulfoxide activated by triflic anhydride to give oxocarbenium trapped by ethanol.',
            'reactants_text': 'phenyl sulfoxide glycosyl donor + ethanol',
            'products_text': 'ethyl glycoside',
            'reagents_text': 'Tf2O, 2,6-di-tert-butylpyridine',
            'conditions_text': 'CH2Cl2, -78°C, 4Å MS',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase17_shallow_top10_v1 sprint for Kahne Glycosidation. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC1OC(S(=O)c2ccccc2)C(O)C(O)C1O.OCC', 'phenyl sulfoxide glycosyl donor + ethanol', 'reactants_text', 1),
                ('product', 'CCOC1OC(C)C(O)C(O)C1O', 'ethyl glycoside', 'products_text', 1),
            ],
        },
        {
            'family': 'Kahne Glycosidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Kahne glycosidation of fully acetylated sulfoxide donor with methanol acceptor.',
            'reactants_text': 'peracetylated phenyl-sulfoxide glycosyl donor + methanol',
            'products_text': 'methyl 2,3,4-tri-O-acetyl glycoside',
            'reagents_text': 'Tf2O, DTBMP',
            'conditions_text': 'CH2Cl2, -78→-40°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase17_shallow_top10_v1 sprint for Kahne Glycosidation. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC1OC(S(=O)c2ccccc2)C(OC(=O)C)C(OC(=O)C)C1OC(=O)C.OC', 'peracetylated phenyl-sulfoxide glycosyl donor + methanol', 'reactants_text', 1),
                ('product', 'COC1OC(C)C(OC(=O)C)C(OC(=O)C)C1OC(=O)C', 'methyl 2,3,4-tri-O-acetyl glycoside', 'products_text', 1),
            ],
        },
        {
            'family': 'Kahne Glycosidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Kahne glycosidation delivering benzyl glycoside from sulfoxide donor and BnOH.',
            'reactants_text': 'phenyl sulfoxide donor + benzyl alcohol',
            'products_text': 'benzyl glycoside',
            'reagents_text': 'Tf2O, 2,6-lutidine',
            'conditions_text': 'CH2Cl2, -60°C, MS',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase17_shallow_top10_v1 sprint for Kahne Glycosidation. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC1OC(S(=O)c2ccccc2)C(O)C(O)C1O.OCc1ccccc1', 'phenyl sulfoxide donor + benzyl alcohol', 'reactants_text', 1),
                ('product', 'OCc1ccccc1OC2OC(C)C(O)C(O)C2O', 'benzyl glycoside', 'products_text', 1),
            ],
        },
        # === Keck Asymmetric Allylation ===
        {
            'family': 'Keck Asymmetric Allylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Keck asymmetric allylation: aromatic aldehyde with allylstannane and chiral Ti/BINOL catalyst gives homoallyl alcohol in high ee.',
            'reactants_text': 'benzaldehyde',
            'products_text': '(S)-1-phenylbut-3-en-1-ol',
            'reagents_text': 'Ti(OiPr)4, (S)-BINOL, allyltributylstannane',
            'conditions_text': 'CH2Cl2, -20°C, 4Å MS',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase17_shallow_top10_v1 sprint for Keck Asymmetric Allylation. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1),
                ('product', 'OC(CC=C)c1ccccc1', '(S)-1-phenylbut-3-en-1-ol', 'products_text', 1),
            ],
        },
        {
            'family': 'Keck Asymmetric Allylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Keck allylation of aliphatic aldehyde with chiral Ti-BINOL catalyst and allyltributylstannane.',
            'reactants_text': 'pentanal',
            'products_text': '(S)-oct-1-en-4-ol',
            'reagents_text': 'Ti(OiPr)4, (S)-BINOL, allyltributylstannane',
            'conditions_text': 'CH2Cl2, -20°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase17_shallow_top10_v1 sprint for Keck Asymmetric Allylation. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCCCC=O', 'pentanal', 'reactants_text', 1),
                ('product', 'CCCCC(O)CC=C', '(S)-oct-1-en-4-ol', 'products_text', 1),
            ],
        },
        {
            'family': 'Keck Asymmetric Allylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Keck asymmetric allylation of branched alkyl aldehyde giving chiral homoallyl alcohol.',
            'reactants_text': 'cyclohexanecarboxaldehyde',
            'products_text': '1-cyclohexyl-3-buten-1-ol',
            'reagents_text': 'Ti(OiPr)4, (R)-BINOL, allyltributylstannane',
            'conditions_text': 'CH2Cl2, -20°C, 4Å MS',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase17_shallow_top10_v1 sprint for Keck Asymmetric Allylation. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CC1CCCCC1', 'cyclohexanecarboxaldehyde', 'reactants_text', 1),
                ('product', 'OC(CC=C)C1CCCCC1', '1-cyclohexyl-3-buten-1-ol', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Keck Macrolactonization ===
        {
            'family': 'Keck Macrolactonization',
            'extract_kind': 'application_example',
            'transformation_text': 'Keck macrolactonization: seco-acid cyclized to macrolactone under Steglich DMAP·HCl/DCC conditions.',
            'reactants_text': '11-hydroxyundecanoic acid',
            'products_text': 'undecanolide (12-membered macrolactone)',
            'reagents_text': 'DCC, DMAP, DMAP·HCl',
            'conditions_text': 'CHCl3, high dilution, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase17_shallow_top10_v1 sprint for Keck Macrolactonization. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCCCCCCCCCCC(=O)O', '11-hydroxyundecanoic acid', 'reactants_text', 1),
                ('product', 'O=C1CCCCCCCCCCO1', 'undecanolide (12-membered macrolactone)', 'products_text', 1),
            ],
        },
        {
            'family': 'Keck Macrolactonization',
            'extract_kind': 'application_example',
            'transformation_text': 'Keck macrolactonization of 15-hydroxyacid to musk-ring macrolactone.',
            'reactants_text': '15-hydroxypentadecanoic acid',
            'products_text': 'pentadecanolide (16-membered)',
            'reagents_text': 'DCC, DMAP·HCl, DMAP',
            'conditions_text': 'CHCl3, slow addition, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase17_shallow_top10_v1 sprint for Keck Macrolactonization. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCCCCCCCCCCCCCCC(=O)O', '15-hydroxypentadecanoic acid', 'reactants_text', 1),
                ('product', 'O=C1CCCCCCCCCCCCCCO1', 'pentadecanolide (16-membered)', 'products_text', 1),
            ],
        },
        {
            'family': 'Keck Macrolactonization',
            'extract_kind': 'application_example',
            'transformation_text': 'Keck macrolactonization with substituted seco-acid to form methylated macrolactone.',
            'reactants_text': '11-hydroxy-10-methylundecanoic acid',
            'products_text': 'methyl-substituted macrolactone',
            'reagents_text': 'DCC, DMAP, DMAP·HCl',
            'conditions_text': 'CHCl3, 0.002 M',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase17_shallow_top10_v1 sprint for Keck Macrolactonization. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCC(C)CCCCCCCCCC(=O)O', '11-hydroxy-10-methylundecanoic acid', 'reactants_text', 1),
                ('product', 'O=C1CCCCCCCCCCC(C)CO1', 'methyl-substituted macrolactone', 'products_text', 1),
            ],
        },
        # === Keck Radical Allylation ===
        {
            'family': 'Keck Radical Allylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Keck radical allylation: α-bromoester radical intercepted by allylstannane to install allyl group at the tertiary carbon.',
            'reactants_text': 'ethyl 2-bromo-2-methylpropanoate',
            'products_text': 'ethyl 2,2-dimethylpent-4-enoate',
            'reagents_text': 'allyltributylstannane, AIBN (cat.)',
            'conditions_text': 'benzene, 80°C, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase17_shallow_top10_v1 sprint for Keck Radical Allylation. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'BrC(C)(C)C(=O)OCC', 'ethyl 2-bromo-2-methylpropanoate', 'reactants_text', 1),
                ('product', 'C=CCC(C)(C)C(=O)OCC', 'ethyl 2,2-dimethylpent-4-enoate', 'products_text', 1),
            ],
        },
        {
            'family': 'Keck Radical Allylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Keck radical allylation of α-bromo lactone with allyltributylstannane via carbon radical chain.',
            'reactants_text': 'α-bromo-δ-valerolactone',
            'products_text': 'α-allyl-δ-valerolactone',
            'reagents_text': 'allyltributylstannane, AIBN',
            'conditions_text': 'benzene, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase17_shallow_top10_v1 sprint for Keck Radical Allylation. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'BrC1CCCC(=O)O1', 'α-bromo-δ-valerolactone', 'reactants_text', 1),
                ('product', 'C=CCC1CCCC(=O)O1', 'α-allyl-δ-valerolactone', 'products_text', 1),
            ],
        },
        {
            'family': 'Keck Radical Allylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Keck allylation via α-iodoketone radical captured by allylstannane.',
            'reactants_text': 'α-iodo propiophenone',
            'products_text': 'α-allyl propiophenone',
            'reagents_text': 'allyltributylstannane, AIBN (cat.)',
            'conditions_text': 'benzene, 80°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase17_shallow_top10_v1 sprint for Keck Radical Allylation. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'IC(C)C(=O)c1ccccc1', 'α-iodo propiophenone', 'reactants_text', 1),
                ('product', 'C=CCC(C)C(=O)c1ccccc1', 'α-allyl propiophenone', 'products_text', 1),
            ],
        },
        # === Koenigs-Knorr Glycosidation ===
        {
            'family': 'Koenigs-Knorr Glycosidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Koenigs-Knorr: glycosyl bromide activated by silver salt gives β-glycoside with methanol acceptor.',
            'reactants_text': 'α-glucosyl bromide + methanol',
            'products_text': 'methyl β-D-glucoside',
            'reagents_text': 'Ag2CO3 or AgOTf',
            'conditions_text': 'CH2Cl2, 4Å MS, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase17_shallow_top10_v1 sprint for Koenigs-Knorr Glycosidation. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'BrC1OC(CO)C(O)C(O)C1O.OC', 'α-glucosyl bromide + methanol', 'reactants_text', 1),
                ('product', 'COC1OC(CO)C(O)C(O)C1O', 'methyl β-D-glucoside', 'products_text', 1),
            ],
        },
        {
            'family': 'Koenigs-Knorr Glycosidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Koenigs-Knorr glycosidation of tetra-O-acetyl-α-D-glucopyranosyl bromide with ethanol.',
            'reactants_text': 'acetobromoglucose + ethanol',
            'products_text': 'ethyl 2,3,4,6-tetra-O-acetyl-β-D-glucopyranoside',
            'reagents_text': 'Ag2CO3, Drierite',
            'conditions_text': 'CH2Cl2, rt, dark',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase17_shallow_top10_v1 sprint for Koenigs-Knorr Glycosidation. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'BrC1OC(COC(=O)C)C(OC(=O)C)C(OC(=O)C)C1OC(=O)C.OCC', 'acetobromoglucose + ethanol', 'reactants_text', 1),
                ('product', 'CCOC1OC(COC(=O)C)C(OC(=O)C)C(OC(=O)C)C1OC(=O)C', 'ethyl 2,3,4,6-tetra-O-acetyl-β-D-glucopyranoside', 'products_text', 1),
            ],
        },
        {
            'family': 'Koenigs-Knorr Glycosidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Koenigs-Knorr glycosidation giving benzyl glycoside using silver-salt promoter.',
            'reactants_text': 'glycosyl bromide + benzyl alcohol',
            'products_text': 'benzyl β-D-glycoside',
            'reagents_text': 'AgOTf, 2,6-lutidine',
            'conditions_text': 'CH2Cl2, -20°C, 4Å MS',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase17_shallow_top10_v1 sprint for Koenigs-Knorr Glycosidation. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'BrC1OC(CO)C(O)C(O)C1O.OCc1ccccc1', 'glycosyl bromide + benzyl alcohol', 'reactants_text', 1),
                ('product', 'OCC1OC(OCc2ccccc2)C(O)C(O)C1O', 'benzyl β-D-glycoside', 'products_text', 1),
            ],
        },
        # === Kolbe-Schmitt Reaction ===
        {
            'family': 'Kolbe-Schmitt Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Kolbe-Schmitt: sodium phenolate undergoes carboxylation with CO2 at elevated temperature/pressure to give salicylic acid after acidic workup.',
            'reactants_text': 'phenol + CO2',
            'products_text': 'salicylic acid',
            'reagents_text': 'NaOH (form phenolate), CO2 (high pressure)',
            'conditions_text': '125°C, 100 atm CO2',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase17_shallow_top10_v1 sprint for Kolbe-Schmitt Reaction. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Oc1ccccc1.O=C=O', 'phenol + CO2', 'reactants_text', 1),
                ('product', 'O=C(O)c1ccccc1O', 'salicylic acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Kolbe-Schmitt Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Kolbe-Schmitt carboxylation of p-cresolate to give 5-methyl salicylic acid.',
            'reactants_text': 'p-cresol + CO2',
            'products_text': '5-methyl salicylic acid',
            'reagents_text': 'KOH, CO2 pressurized',
            'conditions_text': '180°C, 80 atm',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase17_shallow_top10_v1 sprint for Kolbe-Schmitt Reaction. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Oc1ccc(C)cc1.O=C=O', 'p-cresol + CO2', 'reactants_text', 1),
                ('product', 'O=C(O)c1cc(C)ccc1O', '5-methyl salicylic acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Kolbe-Schmitt Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Kolbe-Schmitt on hydroquinone disodium salt delivers gentisic acid.',
            'reactants_text': 'hydroquinone + CO2',
            'products_text': '2,5-dihydroxybenzoic acid (gentisic acid)',
            'reagents_text': 'NaOH, CO2',
            'conditions_text': '125°C, high CO2 pressure',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase17_shallow_top10_v1 sprint for Kolbe-Schmitt Reaction. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Oc1ccc(O)cc1.O=C=O', 'hydroquinone + CO2', 'reactants_text', 1),
                ('product', 'O=C(O)c1cc(O)ccc1O', '2,5-dihydroxybenzoic acid (gentisic acid)', 'products_text', 1),
            ],
        },
        # === Kröhnke Pyridine Synthesis ===
        {
            'family': 'Kröhnke Pyridine Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Kröhnke pyridine synthesis: phenacyl pyridinium + α,β-unsaturated ketone + NH4OAc gives trisubstituted pyridine.',
            'reactants_text': 'trans-chalcone',
            'products_text': '2,4,6-triphenylpyridine',
            'reagents_text': 'phenacylpyridinium bromide, NH4OAc, AcOH',
            'conditions_text': 'AcOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase17_shallow_top10_v1 sprint for Kröhnke Pyridine Synthesis. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C=Cc1ccccc1)c2ccccc2', 'trans-chalcone', 'reactants_text', 1),
                ('product', 'c1ccc(-c2cc(-c3ccccc3)nc(-c4ccccc4)c2)cc1', '2,4,6-triphenylpyridine', 'products_text', 1),
            ],
        },
        {
            'family': 'Kröhnke Pyridine Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Kröhnke cyclocondensation from 1,3-diketone, aldehyde, and ammonia source.',
            'reactants_text': 'dibenzoylmethane',
            'products_text': '2,6-diphenyl-4-methylpyridine',
            'reagents_text': 'acetaldehyde, NH4OAc, AcOH',
            'conditions_text': 'AcOH, 100°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase17_shallow_top10_v1 sprint for Kröhnke Pyridine Synthesis. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(CC(=O)c1ccccc1)c2ccccc2', 'dibenzoylmethane', 'reactants_text', 1),
                ('product', 'Cc1cc(-c2ccccc2)nc(-c3ccccc3)c1', '2,6-diphenyl-4-methylpyridine', 'products_text', 1),
            ],
        },
        {
            'family': 'Kröhnke Pyridine Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Kröhnke synthesis of heteroaryl-substituted pyridine from furyl enone.',
            'reactants_text': '1-phenyl-3-(furan-2-yl)prop-2-en-1-one',
            'products_text': '2,6-diphenyl-4-(furan-2-yl)pyridine',
            'reagents_text': 'N-phenacyl pyridinium bromide, NH4OAc, AcOH',
            'conditions_text': 'reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase17_shallow_top10_v1 sprint for Kröhnke Pyridine Synthesis. [phase17_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C=Cc1ccco1)c2ccccc2', '1-phenyl-3-(furan-2-yl)prop-2-en-1-one', 'reactants_text', 1),
                ('product', 'c1ccc(-c2cc(-c3ccco3)nc(-c4ccccc4)c2)cc1', '2,6-diphenyl-4-(furan-2-yl)pyridine', 'products_text', 1),
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
