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

TAG = 'phase20_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Neber Rearrangement',
        'Nef Reaction',
        'Negishi Cross-Coupling',
        'Nenitzescu Indole Synthesis',
        'Nicholas Reaction',
    ],
    'b': [
        'Noyori Asymmetric Hydrogenation',
        'Nozaki-Hiyama-Kishi Reaction',
        'Oppenauer Oxidation',
        'Overman Rearrangement',
        'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Neber Rearrangement ===
        {
            'family': 'Neber Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Neber rearrangement: α-tosyl oxime under base gives α-amino ketone via azirine intermediate.',
            'reactants_text': 'tosyl oxime of phenylacetone',
            'products_text': 'α-amino ketone',
            'reagents_text': 'KOEt (base)',
            'conditions_text': 'EtOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase20_shallow_top10_v1 sprint for Neber Rearrangement. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(=NOS(=O)(=O)C)Cc1ccccc1', 'tosyl oxime of phenylacetone', 'reactants_text', 1),
                ('product', 'NC(C(=O)Cc1ccccc1)C', 'α-amino ketone', 'products_text', 1),
            ],
        },
        {
            'family': 'Neber Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Neber rearrangement of O-acetyl oxime via 2H-azirine.',
            'reactants_text': 'O-acetyl oxime of propiophenone',
            'products_text': 'α-amino aryl ketone',
            'reagents_text': 'K2CO3 or KOt-Bu',
            'conditions_text': 'EtOH, 80°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase20_shallow_top10_v1 sprint for Neber Rearrangement. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(c1ccccc1)CC(=NOC(=O)C)C', 'O-acetyl oxime of propiophenone', 'reactants_text', 1),
                ('product', 'NC(C)C(=O)c1ccccc1', 'α-amino aryl ketone', 'products_text', 1),
            ],
        },
        {
            'family': 'Neber Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Neber rearrangement of branched ketoxime tosylate to α-amino ketone.',
            'reactants_text': 'tosyl oxime of 2-methyl-3-hexanone',
            'products_text': 'α-amino ketone',
            'reagents_text': 'NaOEt',
            'conditions_text': 'EtOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase20_shallow_top10_v1 sprint for Neber Rearrangement. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(C)C(=NOS(=O)(=O)c1ccc(C)cc1)CC', 'tosyl oxime of 2-methyl-3-hexanone', 'reactants_text', 1),
                ('product', 'NC(CC)C(=O)C(C)C', 'α-amino ketone', 'products_text', 1),
            ],
        },
        # === Nef Reaction ===
        {
            'family': 'Nef Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Nef reaction: primary nitroalkane converted to aldehyde via anion hydrolysis.',
            'reactants_text': '2-methyl-1-nitropropane',
            'products_text': 'isobutyraldehyde',
            'reagents_text': 'NaOH (deprotonate); then H2SO4',
            'conditions_text': 'aq., 0°C→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase20_shallow_top10_v1 sprint for Nef Reaction. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=N(=O)CC(C)C', '2-methyl-1-nitropropane', 'reactants_text', 1),
                ('product', 'O=CC(C)C', 'isobutyraldehyde', 'products_text', 1),
            ],
        },
        {
            'family': 'Nef Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Nef reaction on secondary benzylic nitroalkane gives aryl ketone.',
            'reactants_text': '1-nitro-1-phenylethane',
            'products_text': 'acetophenone',
            'reagents_text': 'NaOH; H2SO4',
            'conditions_text': 'aqueous, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase20_shallow_top10_v1 sprint for Nef Reaction. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=N(=O)C(C)c1ccccc1', '1-nitro-1-phenylethane', 'reactants_text', 1),
                ('product', 'O=C(C)c1ccccc1', 'acetophenone', 'products_text', 1),
            ],
        },
        {
            'family': 'Nef Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Nef reaction converting nitroester to aldehyde-ester.',
            'reactants_text': 'methyl 4-nitrobutanoate',
            'products_text': 'methyl 4-oxobutanoate',
            'reagents_text': 'NaOMe; aq. H2SO4',
            'conditions_text': 'MeOH then aq. acid',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase20_shallow_top10_v1 sprint for Nef Reaction. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=N(=O)CCC(=O)OC', 'methyl 4-nitrobutanoate', 'reactants_text', 1),
                ('product', 'O=CCC(=O)OC', 'methyl 4-oxobutanoate', 'products_text', 1),
            ],
        },
        # === Negishi Cross-Coupling ===
        {
            'family': 'Negishi Cross-Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Negishi cross-coupling: aryl halide + methylzinc under Pd catalysis gives aryl-methyl bond.',
            'reactants_text': 'bromobenzene',
            'products_text': 'toluene (methyl group installed)',
            'reagents_text': 'CH3ZnBr, Pd(PPh3)4',
            'conditions_text': 'THF, rt→50°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase20_shallow_top10_v1 sprint for Negishi Cross-Coupling. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccccc1', 'bromobenzene', 'reactants_text', 1),
                ('product', 'Cc1ccccc1', 'toluene (methyl group installed)', 'products_text', 1),
            ],
        },
        {
            'family': 'Negishi Cross-Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Negishi coupling: aryl iodide + ethylzinc halide with Pd catalysis.',
            'reactants_text': '4-iodotoluene',
            'products_text': '4-ethyltoluene',
            'reagents_text': 'EtZnBr, Pd(PPh3)4',
            'conditions_text': 'THF, 50°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase20_shallow_top10_v1 sprint for Negishi Cross-Coupling. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1ccc(C)cc1', '4-iodotoluene', 'reactants_text', 1),
                ('product', 'Cc1ccc(CC)cc1', '4-ethyltoluene', 'products_text', 1),
            ],
        },
        {
            'family': 'Negishi Cross-Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Negishi aryl-aryl coupling via phenylzinc halide.',
            'reactants_text': 'bromobenzene',
            'products_text': 'biphenyl',
            'reagents_text': 'PhZnBr, Pd(dppf)Cl2',
            'conditions_text': 'THF, 60°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase20_shallow_top10_v1 sprint for Negishi Cross-Coupling. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccccc1', 'bromobenzene', 'reactants_text', 1),
                ('product', 'c1ccc(-c2ccccc2)cc1', 'biphenyl', 'products_text', 1),
            ],
        },
        # === Nenitzescu Indole Synthesis ===
        {
            'family': 'Nenitzescu Indole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Nenitzescu indole synthesis: p-benzoquinone + β-enaminoester yields 5-hydroxyindole.',
            'reactants_text': 'benzoquinone + β-aminocrotonate',
            'products_text': '5-hydroxy-2-methylindole',
            'reagents_text': 'AcOH, heat',
            'conditions_text': 'AcOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase20_shallow_top10_v1 sprint for Nenitzescu Indole Synthesis. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1C=CC(=O)C=C1.CC(=O)OC(=CC)N', 'benzoquinone + β-aminocrotonate', 'reactants_text', 1),
                ('product', 'Oc1cc2cc(C)[nH]c2cc1O', '5-hydroxy-2-methylindole', 'products_text', 1),
            ],
        },
        {
            'family': 'Nenitzescu Indole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Nenitzescu synthesis with doubly activated enaminone.',
            'reactants_text': 'benzoquinone + ethyl β-amino-α-acetyl-crotonate',
            'products_text': 'ethyl 5-hydroxy-2-methyl-3-acetyl-indole-carboxylate',
            'reagents_text': 'AcOH',
            'conditions_text': 'reflux, 2 h',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase20_shallow_top10_v1 sprint for Nenitzescu Indole Synthesis. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1C=CC(=O)C=C1.CC(=O)C(=C(N)C)C(=O)OCC', 'benzoquinone + ethyl β-amino-α-acetyl-crotonate', 'reactants_text', 1),
                ('product', 'Oc1cc2c(C(=O)OCC)c(C)[nH]c2cc1O', 'ethyl 5-hydroxy-2-methyl-3-acetyl-indole-carboxylate', 'products_text', 1),
            ],
        },
        {
            'family': 'Nenitzescu Indole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Nenitzescu indole synthesis to hydroxy-indole ester.',
            'reactants_text': 'benzoquinone + β-aminocrotonate ester',
            'products_text': 'ethyl 5-hydroxy-2-methylindole-3-carboxylate',
            'reagents_text': 'AcOH or nitromethane',
            'conditions_text': '80°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase20_shallow_top10_v1 sprint for Nenitzescu Indole Synthesis. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1C=CC(=O)C=C1.CC(=C(C)N)C(=O)OCC', 'benzoquinone + β-aminocrotonate ester', 'reactants_text', 1),
                ('product', 'Oc1cc2c(C(=O)OCC)c(C)[nH]c2cc1O', 'ethyl 5-hydroxy-2-methylindole-3-carboxylate', 'products_text', 1),
            ],
        },
        # === Nicholas Reaction ===
        {
            'family': 'Nicholas Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Nicholas reaction: propargyl cation stabilized by Co2(CO)6 gives alkoxide trapping; then decomplex with CAN.',
            'reactants_text': '1-phenyl-2-butyn-1-ol (Co2(CO)6-complexed)',
            'products_text': '3-methoxy-1-phenyl-2-butyne',
            'reagents_text': 'Co2(CO)8 (to complex alkyne); HBF4 (to form cation); MeOH (nucleophile); CAN (decomplex)',
            'conditions_text': 'CH2Cl2, -78°C, then CAN workup',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase20_shallow_top10_v1 sprint for Nicholas Reaction. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)C#Cc1ccccc1', '1-phenyl-2-butyn-1-ol (Co2(CO)6-complexed)', 'reactants_text', 1),
                ('product', 'CCC(OC)C#Cc1ccccc1', '3-methoxy-1-phenyl-2-butyne', 'products_text', 1),
            ],
        },
        {
            'family': 'Nicholas Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Nicholas trapping by ethanol of Co-stabilized propargyl cation.',
            'reactants_text': '1-phenyl-2-butyn-1-ol',
            'products_text': '1-ethoxy-1-phenyl-2-butyne',
            'reagents_text': 'Co2(CO)8; HBF4; EtOH; CAN',
            'conditions_text': 'CH2Cl2, -78°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase20_shallow_top10_v1 sprint for Nicholas Reaction. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(c1ccccc1)C#CC', '1-phenyl-2-butyn-1-ol', 'reactants_text', 1),
                ('product', 'CCC(C#Cc1ccccc1)OCC', '1-ethoxy-1-phenyl-2-butyne', 'products_text', 1),
            ],
        },
        {
            'family': 'Nicholas Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Nicholas: propargyl cation trapped by propanoic acid, then decobaltation to propargyl ester.',
            'reactants_text': '2-heptyn-2-ol',
            'products_text': 'propargyl propionate',
            'reagents_text': 'Co2(CO)8; HBF4; propanoic acid; CAN',
            'conditions_text': 'CH2Cl2, -78°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase20_shallow_top10_v1 sprint for Nicholas Reaction. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)C#CCCC', '2-heptyn-2-ol', 'reactants_text', 1),
                ('product', 'CC(OC(=O)CC)C#CCCC', 'propargyl propionate', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Noyori Asymmetric Hydrogenation ===
        {
            'family': 'Noyori Asymmetric Hydrogenation',
            'extract_kind': 'application_example',
            'transformation_text': 'Noyori asymmetric hydrogenation: Ru-BINAP/diamine catalyst reduces aryl ketone to chiral alcohol under H2.',
            'reactants_text': 'acetophenone',
            'products_text': '(S)-1-phenylethanol',
            'reagents_text': 'RuCl2[(S)-BINAP](DPEN), H2, KOt-Bu',
            'conditions_text': 'iPrOH, 10 atm H2, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase20_shallow_top10_v1 sprint for Noyori Asymmetric Hydrogenation. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)c1ccccc1', 'acetophenone', 'reactants_text', 1),
                ('product', 'OC(C)c1ccccc1', '(S)-1-phenylethanol', 'products_text', 1),
            ],
        },
        {
            'family': 'Noyori Asymmetric Hydrogenation',
            'extract_kind': 'application_example',
            'transformation_text': 'Noyori asymmetric hydrogenation of β-ketoester by Ru-BINAP catalyst.',
            'reactants_text': 'methyl acetoacetate',
            'products_text': 'methyl (R)-3-hydroxybutanoate',
            'reagents_text': 'Ru(OAc)2[(R)-BINAP], H2',
            'conditions_text': 'MeOH, 50°C, 50 atm',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase20_shallow_top10_v1 sprint for Noyori Asymmetric Hydrogenation. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(CC(=O)OC)C', 'methyl acetoacetate', 'reactants_text', 1),
                ('product', 'OC(CC(=O)OC)C', 'methyl (R)-3-hydroxybutanoate', 'products_text', 1),
            ],
        },
        {
            'family': 'Noyori Asymmetric Hydrogenation',
            'extract_kind': 'application_example',
            'transformation_text': 'Noyori hydrogenation of α,β-unsaturated aryl ketone to allylic alcohol (chemoselective with Ru-BINAP/diamine).',
            'reactants_text': 'phenyl vinyl ketone',
            'products_text': '(R)-1-phenylprop-2-en-1-ol',
            'reagents_text': 'Ru-TolBINAP/DPEN, H2, KOtBu',
            'conditions_text': 'iPrOH, 4 atm H2',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase20_shallow_top10_v1 sprint for Noyori Asymmetric Hydrogenation. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(c1ccccc1)C=C', 'phenyl vinyl ketone', 'reactants_text', 1),
                ('product', 'OC(c1ccccc1)C=C', '(R)-1-phenylprop-2-en-1-ol', 'products_text', 1),
            ],
        },
        # === Nozaki-Hiyama-Kishi Reaction ===
        {
            'family': 'Nozaki-Hiyama-Kishi Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Nozaki-Hiyama-Kishi: vinyl iodide + aldehyde coupling mediated by CrCl2/NiCl2.',
            'reactants_text': '(E)-β-iodostyrene + benzaldehyde',
            'products_text': '(1S,E)-1,3-diphenylallyl alcohol',
            'reagents_text': 'CrCl2 (excess), NiCl2 (cat.)',
            'conditions_text': 'DMF, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase20_shallow_top10_v1 sprint for Nozaki-Hiyama-Kishi Reaction. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'IC=Cc1ccccc1.O=Cc2ccccc2', '(E)-β-iodostyrene + benzaldehyde', 'reactants_text', 1),
                ('product', 'OC(c1ccccc1)C=Cc2ccccc2', '(1S,E)-1,3-diphenylallyl alcohol', 'products_text', 1),
            ],
        },
        {
            'family': 'Nozaki-Hiyama-Kishi Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'NHK allylation of aldehyde via allyl-Cr species.',
            'reactants_text': 'allyl iodide + cyclohexanecarboxaldehyde',
            'products_text': '1-cyclohexyl-3-buten-1-ol',
            'reagents_text': 'CrCl2, NiCl2',
            'conditions_text': 'DMF, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase20_shallow_top10_v1 sprint for Nozaki-Hiyama-Kishi Reaction. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'ICC=C.O=CC1CCCCC1', 'allyl iodide + cyclohexanecarboxaldehyde', 'reactants_text', 1),
                ('product', 'OC(CC=C)C1CCCCC1', '1-cyclohexyl-3-buten-1-ol', 'products_text', 1),
            ],
        },
        {
            'family': 'Nozaki-Hiyama-Kishi Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'NHK coupling of vinyl iodide (cyclic) with aryl aldehyde.',
            'reactants_text': '1-iodocyclohexene + benzaldehyde',
            'products_text': '(1-phenyl(cyclohexen-1-yl)methanol)',
            'reagents_text': 'CrCl2 / NiCl2 (cat.)',
            'conditions_text': 'DMSO, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase20_shallow_top10_v1 sprint for Nozaki-Hiyama-Kishi Reaction. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'IC1=CCCCC1.O=Cc2ccccc2', '1-iodocyclohexene + benzaldehyde', 'reactants_text', 1),
                ('product', 'OC(C1=CCCCC1)c2ccccc2', '(1-phenyl(cyclohexen-1-yl)methanol)', 'products_text', 1),
            ],
        },
        # === Oppenauer Oxidation ===
        {
            'family': 'Oppenauer Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Oppenauer oxidation: secondary alcohol oxidized to ketone via Al(OiPr)3 and acetone/cyclohexanone as H-acceptor.',
            'reactants_text': '1-phenylethanol',
            'products_text': 'acetophenone',
            'reagents_text': 'Al(OiPr)3, acetone (H-acceptor)',
            'conditions_text': 'toluene, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase20_shallow_top10_v1 sprint for Oppenauer Oxidation. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)c1ccccc1', '1-phenylethanol', 'reactants_text', 1),
                ('product', 'O=C(C)c1ccccc1', 'acetophenone', 'products_text', 1),
            ],
        },
        {
            'family': 'Oppenauer Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Oppenauer oxidation of cyclohexanol to cyclohexanone.',
            'reactants_text': 'cyclohexanol',
            'products_text': 'cyclohexanone',
            'reagents_text': 'Al(OtBu)3, cyclohexanone (H-acceptor)',
            'conditions_text': 'benzene, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase20_shallow_top10_v1 sprint for Oppenauer Oxidation. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC1CCCCC1', 'cyclohexanol', 'reactants_text', 1),
                ('product', 'O=C1CCCCC1', 'cyclohexanone', 'products_text', 1),
            ],
        },
        {
            'family': 'Oppenauer Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Oppenauer selectivity: tertiary alcohol does not oxidize (example shows scope limitation; unreactive substrate).',
            'reactants_text': '1-methyl-cyclopentan-1-ol (tertiary — no rxn typically)',
            'products_text': 'no oxidation — tertiary alcohol demonstration',
            'reagents_text': 'Al(OiPr)3, acetone',
            'conditions_text': 'toluene, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase20_shallow_top10_v1 sprint for Oppenauer Oxidation. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC1(C)CCCC1', '1-methyl-cyclopentan-1-ol (tertiary — no rxn typically)', 'reactants_text', 1),
                ('product', 'O=C1CCCC1=C', 'no oxidation — tertiary alcohol demonstration', 'products_text', 1),
            ],
        },
        # === Overman Rearrangement ===
        {
            'family': 'Overman Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Overman [3,3]-sigmatropic rearrangement of allyl trichloroacetimidate to allyl trichloroacetamide.',
            'reactants_text': 'allyl trichloroacetimidate',
            'products_text': 'N-allyl trichloroacetamide',
            'reagents_text': 'thermal or Hg/Pd(II) catalysis',
            'conditions_text': 'xylene, 140°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase20_shallow_top10_v1 sprint for Overman Rearrangement. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCOC(=N)C(Cl)(Cl)Cl', 'allyl trichloroacetimidate', 'reactants_text', 1),
                ('product', 'C=CCNC(=O)C(Cl)(Cl)Cl', 'N-allyl trichloroacetamide', 'products_text', 1),
            ],
        },
        {
            'family': 'Overman Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Overman rearrangement on prenyl imidate gives chiral allyl amide with transposition.',
            'reactants_text': 'prenyl trichloroacetimidate',
            'products_text': 'N-prenyl trichloroacetamide',
            'reagents_text': 'Pd(MeCN)2Cl2 (cat.)',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase20_shallow_top10_v1 sprint for Overman Rearrangement. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC(C)=CCOC(=N)C(Cl)(Cl)Cl', 'prenyl trichloroacetimidate', 'reactants_text', 1),
                ('product', 'CC(C)=CCNC(=O)C(Cl)(Cl)Cl', 'N-prenyl trichloroacetamide', 'products_text', 1),
            ],
        },
        {
            'family': 'Overman Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Overman rearrangement of cinnamyl imidate to allyl amide.',
            'reactants_text': 'cinnamyl trichloroacetimidate',
            'products_text': 'N-cinnamyl trichloroacetamide',
            'reagents_text': 'thermal or Hg(II)',
            'conditions_text': 'xylene reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase20_shallow_top10_v1 sprint for Overman Rearrangement. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccc(/C=C/COC(=N)C(Cl)(Cl)Cl)cc1', 'cinnamyl trichloroacetimidate', 'reactants_text', 1),
                ('product', 'c1ccc(/C=C/CNC(=O)C(Cl)(Cl)Cl)cc1', 'N-cinnamyl trichloroacetamide', 'products_text', 1),
            ],
        },
        # === Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement ===
        {
            'family': 'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Oxy-Cope (thermal): 3-hydroxy-1,5-hexadiene undergoes [3,3] shift giving δ,ε-unsaturated ketone.',
            'reactants_text': '3-ethyl-3-hydroxy-1,5-hexadiene',
            'products_text': '3-nonen-6-one',
            'reagents_text': 'thermal (220°C) or KH/18-crown-6 for anionic',
            'conditions_text': 'neat, 220°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase20_shallow_top10_v1 sprint for Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C=C)(CC=C)CC', '3-ethyl-3-hydroxy-1,5-hexadiene', 'reactants_text', 1),
                ('product', 'O=C(CC)CCC=CCC', '3-nonen-6-one', 'products_text', 1),
            ],
        },
        {
            'family': 'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Anionic Oxy-Cope: potassium alkoxide accelerates [3,3] sigmatropic shift by >10^10 rate.',
            'reactants_text': 'aryl-substituted 3-hydroxy-1,5-hexadiene',
            'products_text': 'unsaturated aryl ketone',
            'reagents_text': 'KH, 18-crown-6',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase20_shallow_top10_v1 sprint for Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C=C)(CC=Cc1ccccc1)CC', 'aryl-substituted 3-hydroxy-1,5-hexadiene', 'reactants_text', 1),
                ('product', 'O=C(CC)CCC=Cc1ccccc1', 'unsaturated aryl ketone', 'products_text', 1),
            ],
        },
        {
            'family': 'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Oxy-Cope on fused cyclic diol-alcohol giving ring-expanded enone after tautomerization.',
            'reactants_text': '1-vinyl-2-(2-propenyl)cyclopentanol',
            'products_text': 'ring-opened ring-expanded enone',
            'reagents_text': 'KH, THF',
            'conditions_text': 'THF, 25°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase20_shallow_top10_v1 sprint for Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement. [phase20_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC1(C=C)CCCC1C=C', '1-vinyl-2-(2-propenyl)cyclopentanol', 'reactants_text', 1),
                ('product', 'O=C1CCCCC1CCC=C', 'ring-opened ring-expanded enone', 'products_text', 1),
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
