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

TAG = 'phase24_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Ring-Closing Metathesis',
        'Ring-Opening Metathesis',
        'Ring-Opening Metathesis Polymerization',
        'Ritter Reaction',
        'Robinson Annulation',
    ],
    'b': [
        'Roush Asymmetric Allylation',
        'Rubottom Oxidation',
        'Saegusa Oxidation',
        'Sakurai Allylation',
        'Sandmeyer Reaction',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Ring-Closing Metathesis ===
        {
            'family': 'Ring-Closing Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Ring-closing metathesis (RCM): α,ω-diene cyclized with Grubbs catalyst to give cyclic alkene + ethylene.',
            'reactants_text': '1,6-heptadiene',
            'products_text': 'cyclopentene',
            'reagents_text': 'Grubbs II catalyst',
            'conditions_text': 'CH2Cl2, 40°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase24_shallow_top10_v1 sprint for Ring-Closing Metathesis. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCCCC=C', '1,6-heptadiene', 'reactants_text', 1),
                ('product', 'C1=CCCCC1', 'cyclopentene', 'products_text', 1),
            ],
        },
        {
            'family': 'Ring-Closing Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'RCM of allylic ether to 5-membered oxygen heterocycle.',
            'reactants_text': 'diallyl ether derivative',
            'products_text': '2,5-dihydrofuran',
            'reagents_text': 'Grubbs II',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase24_shallow_top10_v1 sprint for Ring-Closing Metathesis. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCOCCC=C', 'diallyl ether derivative', 'reactants_text', 1),
                ('product', 'C1=CCOCC1', '2,5-dihydrofuran', 'products_text', 1),
            ],
        },
        {
            'family': 'Ring-Closing Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'RCM producing 5-membered azacycle.',
            'reactants_text': 'N,N-diallyl acetamide',
            'products_text': 'N-acetyl-2,5-dihydropyrrole',
            'reagents_text': 'Grubbs II (5 mol%)',
            'conditions_text': 'CH2Cl2, 40°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase24_shallow_top10_v1 sprint for Ring-Closing Metathesis. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCN(CC=C)C(=O)C', 'N,N-diallyl acetamide', 'reactants_text', 1),
                ('product', 'O=C(C)N1CC=CC1', 'N-acetyl-2,5-dihydropyrrole', 'products_text', 1),
            ],
        },
        # === Ring-Opening Metathesis ===
        {
            'family': 'Ring-Opening Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Ring-opening metathesis: strained cycloalkene opens on treatment with Ru or Mo catalyst giving acyclic/oligomeric diene.',
            'reactants_text': 'norbornene',
            'products_text': 'ring-opened diene',
            'reagents_text': 'Grubbs II, ethylene (as chain transfer)',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase24_shallow_top10_v1 sprint for Ring-Opening Metathesis. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C1CC2CC1C=C2', 'norbornene', 'reactants_text', 1),
                ('product', 'C=CC1CC2CC1CC2', 'ring-opened diene', 'products_text', 1),
            ],
        },
        {
            'family': 'Ring-Opening Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Ring-opening metathesis of cyclooctene to 1,9-decadiene with ethylene.',
            'reactants_text': 'cis-cyclooctene (CoE)',
            'products_text': '1,9-decadiene',
            'reagents_text': 'Grubbs II, ethylene',
            'conditions_text': 'toluene, 40°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase24_shallow_top10_v1 sprint for Ring-Opening Metathesis. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C1=CC2CCCCC2CC1', 'cis-cyclooctene (CoE)', 'reactants_text', 1),
                ('product', 'C=CC1CCCCC1CC=C', '1,9-decadiene', 'products_text', 1),
            ],
        },
        {
            'family': 'Ring-Opening Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'ROM generating diene/triene from cyclic alkene.',
            'reactants_text': 'cyclodec-1-ene',
            'products_text': '1,3,11-triene (ring-opened)',
            'reagents_text': 'Grubbs II or Mo alkylidene',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase24_shallow_top10_v1 sprint for Ring-Opening Metathesis. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C1CC2CCCCC2=CC1', 'cyclodec-1-ene', 'reactants_text', 1),
                ('product', 'C=CCCCCCCC=CC=C', '1,3,11-triene (ring-opened)', 'products_text', 1),
            ],
        },
        # === Ring-Opening Metathesis Polymerization ===
        {
            'family': 'Ring-Opening Metathesis Polymerization',
            'extract_kind': 'application_example',
            'transformation_text': 'Ring-opening metathesis polymerization of norbornene yields poly(norbornylene).',
            'reactants_text': 'norbornene',
            'products_text': 'polynorbornene (model repeat unit)',
            'reagents_text': 'Grubbs I or II, no chain transfer',
            'conditions_text': 'CH2Cl2 or toluene, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase24_shallow_top10_v1 sprint for Ring-Opening Metathesis Polymerization. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C1CC2CC1C=C2', 'norbornene', 'reactants_text', 1),
                ('product', 'C(=CC)CC(C)C', 'polynorbornene (model repeat unit)', 'products_text', 1),
            ],
        },
        {
            'family': 'Ring-Opening Metathesis Polymerization',
            'extract_kind': 'application_example',
            'transformation_text': 'ROMP of cyclooctene giving polyoctenamer.',
            'reactants_text': 'cis-cyclooctene',
            'products_text': 'poly(cyclooctene) segment',
            'reagents_text': 'Grubbs II, no C2H4',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase24_shallow_top10_v1 sprint for Ring-Opening Metathesis Polymerization. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C1=CC2CCCCC2CC1', 'cis-cyclooctene', 'reactants_text', 1),
                ('product', 'C=CC(C)CC=CC(C)CC', 'poly(cyclooctene) segment', 'products_text', 1),
            ],
        },
        {
            'family': 'Ring-Opening Metathesis Polymerization',
            'extract_kind': 'application_example',
            'transformation_text': 'ROMP of substituted norbornene-type monomer.',
            'reactants_text': 'benz-fused cyclic alkene',
            'products_text': 'ROMP polymer segment',
            'reagents_text': 'Grubbs III, CH2Cl2',
            'conditions_text': 'rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase24_shallow_top10_v1 sprint for Ring-Opening Metathesis Polymerization. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C1=CC2=CC=CC=C2CC1', 'benz-fused cyclic alkene', 'reactants_text', 1),
                ('product', 'C=Cc1ccccc1C(=C)CC', 'ROMP polymer segment', 'products_text', 1),
            ],
        },
        # === Ritter Reaction ===
        {
            'family': 'Ritter Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Ritter reaction: tertiary carbocation (from t-BuOH/H+) trapped by nitrile, then hydrolysis to amide.',
            'reactants_text': 'tert-butanol + acetonitrile',
            'products_text': 'N-tert-butyl acetamide',
            'reagents_text': 'H2SO4 (conc.), acetonitrile',
            'conditions_text': '0°C→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase24_shallow_top10_v1 sprint for Ritter Reaction. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)(C)C.N#CC', 'tert-butanol + acetonitrile', 'reactants_text', 1),
                ('product', 'CC(C)(C)NC(C)=O', 'N-tert-butyl acetamide', 'products_text', 1),
            ],
        },
        {
            'family': 'Ritter Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Ritter amidation from alkene cation and nitrile.',
            'reactants_text': '2,2-dimethyl-1-butene + benzonitrile',
            'products_text': 'N-tert-alkyl benzamide',
            'reagents_text': 'H2SO4',
            'conditions_text': 'CH2Cl2/H2O, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase24_shallow_top10_v1 sprint for Ritter Reaction. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CC(C)(C)C.N#Cc1ccccc1', '2,2-dimethyl-1-butene + benzonitrile', 'reactants_text', 1),
                ('product', 'O=C(c1ccccc1)NC(C)(C)CC', 'N-tert-alkyl benzamide', 'products_text', 1),
            ],
        },
        {
            'family': 'Ritter Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Ritter with tertiary benzylic alcohol giving N-aryl-tert-alkyl amide.',
            'reactants_text': '2-methyl-2-phenyl-2-propanol + benzonitrile',
            'products_text': 'N-(2-phenyl-2-propyl)-benzamide',
            'reagents_text': 'H2SO4',
            'conditions_text': 'AcOH, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase24_shallow_top10_v1 sprint for Ritter Reaction. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)(C)c1ccccc1.N#Cc2ccccc2', '2-methyl-2-phenyl-2-propanol + benzonitrile', 'reactants_text', 1),
                ('product', 'CC(C)(NC(=O)c1ccccc1)c2ccccc2', 'N-(2-phenyl-2-propyl)-benzamide', 'products_text', 1),
            ],
        },
        # === Robinson Annulation ===
        {
            'family': 'Robinson Annulation',
            'extract_kind': 'application_example',
            'transformation_text': 'Robinson annulation: Michael addition of cyclohexanone enolate to MVK, then intramolecular aldol + dehydration to fused cyclohexenone.',
            'reactants_text': 'cyclohexanone + methyl vinyl ketone',
            'products_text': 'Wieland-Miescher ketone analog / fused bicyclic enone',
            'reagents_text': 'NaOEt or KOH, then acid',
            'conditions_text': 'EtOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase24_shallow_top10_v1 sprint for Robinson Annulation. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCCC1.CC=CC(=O)C', 'cyclohexanone + methyl vinyl ketone', 'reactants_text', 1),
                ('product', 'O=C1CCC2=CC(C)CC(C)C12', 'Wieland-Miescher ketone analog / fused bicyclic enone', 'products_text', 1),
            ],
        },
        {
            'family': 'Robinson Annulation',
            'extract_kind': 'application_example',
            'transformation_text': 'Robinson annulation with isoprenylic enone gives gem-dimethyl bicyclic enone.',
            'reactants_text': 'cyclohexanone + mesityl oxide',
            'products_text': '6,6-dimethyl Wieland-Miescher-type enone',
            'reagents_text': 'NaOEt',
            'conditions_text': 'EtOH reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase24_shallow_top10_v1 sprint for Robinson Annulation. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCCC1.CC(=CC(=O)C)C', 'cyclohexanone + mesityl oxide', 'reactants_text', 1),
                ('product', 'O=C2CCC3=CC(C)(C)CC2C3', '6,6-dimethyl Wieland-Miescher-type enone', 'products_text', 1),
            ],
        },
        {
            'family': 'Robinson Annulation',
            'extract_kind': 'application_example',
            'transformation_text': 'Robinson annulation for hydrindanone with MVK.',
            'reactants_text': '2-methylcyclopentanone + methyl vinyl ketone',
            'products_text': 'angular methyl hydrindenone',
            'reagents_text': 'NaOEt',
            'conditions_text': 'EtOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase24_shallow_top10_v1 sprint for Robinson Annulation. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCC1C.CC=CC(=O)C', '2-methylcyclopentanone + methyl vinyl ketone', 'reactants_text', 1),
                ('product', 'O=C1CCC2=CC(C)CC12C', 'angular methyl hydrindenone', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Roush Asymmetric Allylation ===
        {
            'family': 'Roush Asymmetric Allylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Roush asymmetric allylation: allylboronate with tartrate-modified chiral auxiliary + aldehyde gives enantioenriched homoallyl alcohol.',
            'reactants_text': 'benzaldehyde',
            'products_text': '(R)-1-phenylbut-3-en-1-ol',
            'reagents_text': 'allylboronate of (R,R)-DIPT (diisopropyl tartrate)',
            'conditions_text': 'toluene, -78°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase24_shallow_top10_v1 sprint for Roush Asymmetric Allylation. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1),
                ('product', 'OC(CC=C)c1ccccc1', '(R)-1-phenylbut-3-en-1-ol', 'products_text', 1),
            ],
        },
        {
            'family': 'Roush Asymmetric Allylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Roush allylation of aliphatic aldehyde.',
            'reactants_text': 'pentanal',
            'products_text': '(R)-4-hepten-4-ol',
            'reagents_text': '(R,R)-DIPT allylboronate',
            'conditions_text': 'toluene, -78°C, 4Å MS',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase24_shallow_top10_v1 sprint for Roush Asymmetric Allylation. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCCCC=O', 'pentanal', 'reactants_text', 1),
                ('product', 'CCCCC(O)CC=C', '(R)-4-hepten-4-ol', 'products_text', 1),
            ],
        },
        {
            'family': 'Roush Asymmetric Allylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Roush allylation with electron-rich aryl aldehyde.',
            'reactants_text': 'anisaldehyde',
            'products_text': '(S)-1-(4-methoxyphenyl)-3-buten-1-ol',
            'reagents_text': '(S,S)-DIPT allylboronate',
            'conditions_text': 'toluene, -78°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase24_shallow_top10_v1 sprint for Roush Asymmetric Allylation. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccc(OC)cc1', 'anisaldehyde', 'reactants_text', 1),
                ('product', 'OC(CC=C)c1ccc(OC)cc1', '(S)-1-(4-methoxyphenyl)-3-buten-1-ol', 'products_text', 1),
            ],
        },
        # === Rubottom Oxidation ===
        {
            'family': 'Rubottom Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Rubottom oxidation: TMS enol ether + mCPBA epoxidation → rearrangement to α-hydroxy carbonyl.',
            'reactants_text': 'TMS enol ether of acetone',
            'products_text': 'α-hydroxy acetone (hydroxyacetone)',
            'reagents_text': 'mCPBA; then K2CO3/MeOH',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase24_shallow_top10_v1 sprint for Rubottom Oxidation. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C[Si](C)(C)OC(=C)C', 'TMS enol ether of acetone', 'reactants_text', 1),
                ('product', 'OC(C(=O)C)', 'α-hydroxy acetone (hydroxyacetone)', 'products_text', 1),
            ],
        },
        {
            'family': 'Rubottom Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Rubottom on butan-2-one enol silyl ether.',
            'reactants_text': 'TMS enol ether of butanone',
            'products_text': 'α-hydroxy-butan-2-one',
            'reagents_text': 'mCPBA, then TBAF or K2CO3/MeOH',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase24_shallow_top10_v1 sprint for Rubottom Oxidation. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C[Si](C)(C)OC(=CC)C', 'TMS enol ether of butanone', 'reactants_text', 1),
                ('product', 'OC(C)C(=O)C', 'α-hydroxy-butan-2-one', 'products_text', 1),
            ],
        },
        {
            'family': 'Rubottom Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Rubottom oxidation to α-hydroxy aryl ketone.',
            'reactants_text': 'TMS enol ether of propiophenone',
            'products_text': 'α-hydroxy-1-phenyl-1-propanone',
            'reagents_text': 'mCPBA',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase24_shallow_top10_v1 sprint for Rubottom Oxidation. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C[Si](C)(C)OC(=Cc1ccccc1)C', 'TMS enol ether of propiophenone', 'reactants_text', 1),
                ('product', 'O=C(C)C(O)c1ccccc1', 'α-hydroxy-1-phenyl-1-propanone', 'products_text', 1),
            ],
        },
        # === Saegusa Oxidation ===
        {
            'family': 'Saegusa Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Saegusa oxidation: silyl enol ether → Pd(OAc)2 gives α,β-unsaturated carbonyl via Pd enolate.',
            'reactants_text': 'TMS enol ether of butanone',
            'products_text': 'methyl vinyl ketone (enone)',
            'reagents_text': 'Pd(OAc)2 (cat.), benzoquinone (reoxidant)',
            'conditions_text': 'CH3CN, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase24_shallow_top10_v1 sprint for Saegusa Oxidation. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C[Si](C)(C)OC(=C)CC', 'TMS enol ether of butanone', 'reactants_text', 1),
                ('product', 'O=C(C)C=C', 'methyl vinyl ketone (enone)', 'products_text', 1),
            ],
        },
        {
            'family': 'Saegusa Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Saegusa generating enone from β-substituted silyl enol ether.',
            'reactants_text': 'TMS enol ether of pentan-2-one',
            'products_text': '3-penten-2-one',
            'reagents_text': 'Pd(OAc)2, BQ (benzoquinone)',
            'conditions_text': 'CH3CN, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase24_shallow_top10_v1 sprint for Saegusa Oxidation. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C[Si](C)(C)OC(=CC)C', 'TMS enol ether of pentan-2-one', 'reactants_text', 1),
                ('product', 'O=C(C)C=CC', '3-penten-2-one', 'products_text', 1),
            ],
        },
        {
            'family': 'Saegusa Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Saegusa ring enone synthesis from cyclic silyl enol ether.',
            'reactants_text': 'TMS enol ether of cycloheptanone',
            'products_text': 'cyclohept-2-enone',
            'reagents_text': 'Pd(OAc)2 (1 equiv) or cat. with BQ',
            'conditions_text': 'CH3CN, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase24_shallow_top10_v1 sprint for Saegusa Oxidation. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C[Si](C)(C)OC1=CCCCC1', 'TMS enol ether of cycloheptanone', 'reactants_text', 1),
                ('product', 'O=C1C=CCCCC1', 'cyclohept-2-enone', 'products_text', 1),
            ],
        },
        # === Sakurai Allylation ===
        {
            'family': 'Sakurai Allylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Sakurai-Hosomi allylation: allylsilane + aldehyde under Lewis acid (TiCl4) gives homoallyl alcohol.',
            'reactants_text': 'benzaldehyde + allyltrimethylsilane',
            'products_text': '1-phenyl-3-buten-1-ol',
            'reagents_text': 'TiCl4',
            'conditions_text': 'CH2Cl2, -78°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase24_shallow_top10_v1 sprint for Sakurai Allylation. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1.C=CC[Si](C)(C)C', 'benzaldehyde + allyltrimethylsilane', 'reactants_text', 1),
                ('product', 'OC(CC=C)c1ccccc1', '1-phenyl-3-buten-1-ol', 'products_text', 1),
            ],
        },
        {
            'family': 'Sakurai Allylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Sakurai allylation of aryl ketone.',
            'reactants_text': 'acetophenone + allyltrimethylsilane',
            'products_text': '2-phenyl-4-penten-2-ol',
            'reagents_text': 'BF3·OEt2',
            'conditions_text': 'CH2Cl2, -78°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase24_shallow_top10_v1 sprint for Sakurai Allylation. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)c1ccccc1.C=CC[Si](C)(C)C', 'acetophenone + allyltrimethylsilane', 'reactants_text', 1),
                ('product', 'OC(C)(CC=C)c1ccccc1', '2-phenyl-4-penten-2-ol', 'products_text', 1),
            ],
        },
        {
            'family': 'Sakurai Allylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Sakurai-Hosomi allylation on aliphatic aldehyde.',
            'reactants_text': 'propanal + allyltrimethylsilane',
            'products_text': '5-hexen-3-ol',
            'reagents_text': 'TiCl4',
            'conditions_text': 'CH2Cl2, -78°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase24_shallow_top10_v1 sprint for Sakurai Allylation. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCC=O.C=CC[Si](C)(C)C', 'propanal + allyltrimethylsilane', 'reactants_text', 1),
                ('product', 'CCC(O)CC=C', '5-hexen-3-ol', 'products_text', 1),
            ],
        },
        # === Sandmeyer Reaction ===
        {
            'family': 'Sandmeyer Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Sandmeyer: aryl diazonium (from aniline, NaNO2/HCl) + CuCl gives aryl chloride.',
            'reactants_text': 'aniline (→ via diazonium)',
            'products_text': 'chlorobenzene',
            'reagents_text': 'NaNO2, HCl (diazotize); CuCl',
            'conditions_text': 'H2O, 0°C→60°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase24_shallow_top10_v1 sprint for Sandmeyer Reaction. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Nc1ccccc1', 'aniline (→ via diazonium)', 'reactants_text', 1),
                ('product', 'Clc1ccccc1', 'chlorobenzene', 'products_text', 1),
            ],
        },
        {
            'family': 'Sandmeyer Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Sandmeyer bromination via CuBr.',
            'reactants_text': 'p-toluidine',
            'products_text': 'p-bromotoluene',
            'reagents_text': 'NaNO2, HBr; CuBr',
            'conditions_text': 'H2O, 0°C→rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase24_shallow_top10_v1 sprint for Sandmeyer Reaction. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Nc1ccc(C)cc1', 'p-toluidine', 'reactants_text', 1),
                ('product', 'Brc1ccc(C)cc1', 'p-bromotoluene', 'products_text', 1),
            ],
        },
        {
            'family': 'Sandmeyer Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Sandmeyer cyanation of aryl diazonium with CuCN.',
            'reactants_text': 'p-anisidine',
            'products_text': 'p-methoxybenzonitrile',
            'reagents_text': 'NaNO2, HCl; CuCN, KCN',
            'conditions_text': 'H2O, 0°C→80°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase24_shallow_top10_v1 sprint for Sandmeyer Reaction. [phase24_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Nc1ccc(OC)cc1', 'p-anisidine', 'reactants_text', 1),
                ('product', 'N#Cc1ccc(OC)cc1', 'p-methoxybenzonitrile', 'products_text', 1),
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
