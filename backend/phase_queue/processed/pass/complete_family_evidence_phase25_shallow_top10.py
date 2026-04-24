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

TAG = 'phase25_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Schmidt Reaction',
        'Schotten-Baumann Reaction',
        'Schwartz Hydrozirconation',
        'Seyferth-Gilbert Homologation',
        'Sharpless Asymmetric Aminohydroxylation',
    ],
    'b': [
        'Sharpless Asymmetric Dihydroxylation',
        'Sharpless Asymmetric Epoxidation',
        'Shi Asymmetric Epoxidation',
        'Simmons-Smith Cyclopropanation',
        'Simmons-Smith Reaction',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Schmidt Reaction ===
        {
            'family': 'Schmidt Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Schmidt reaction: ketone + HN3 under strong acid gives amide with aryl-migration (insertion of NH).',
            'reactants_text': 'benzophenone',
            'products_text': 'N-phenylbenzamide',
            'reagents_text': 'HN3 (from NaN3/H2SO4)',
            'conditions_text': 'CHCl3, 0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase25_shallow_top10_v1 sprint for Schmidt Reaction. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(c1ccccc1)c2ccccc2', 'benzophenone', 'reactants_text', 1),
                ('product', 'O=C(Nc1ccccc1)c2ccccc2', 'N-phenylbenzamide', 'products_text', 1),
            ],
        },
        {
            'family': 'Schmidt Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Schmidt reaction of aryl methyl ketone.',
            'reactants_text': 'acetophenone',
            'products_text': 'N-methylbenzamide',
            'reagents_text': 'NaN3, H2SO4',
            'conditions_text': 'CHCl3, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase25_shallow_top10_v1 sprint for Schmidt Reaction. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)c1ccccc1', 'acetophenone', 'reactants_text', 1),
                ('product', 'O=C(NC)c1ccccc1', 'N-methylbenzamide', 'products_text', 1),
            ],
        },
        {
            'family': 'Schmidt Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Schmidt reaction of carboxylic acid → amine (with loss of CO2/N2).',
            'reactants_text': 'benzoic acid',
            'products_text': 'aniline',
            'reagents_text': 'NaN3, H2SO4',
            'conditions_text': 'CHCl3, heat',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase25_shallow_top10_v1 sprint for Schmidt Reaction. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(=O)c1ccccc1', 'benzoic acid', 'reactants_text', 1),
                ('product', 'Nc1ccccc1', 'aniline', 'products_text', 1),
            ],
        },
        # === Schotten-Baumann Reaction ===
        {
            'family': 'Schotten-Baumann Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Schotten-Baumann: amine + acyl chloride under biphasic NaOH(aq)/organic gives amide.',
            'reactants_text': 'aniline + benzoyl chloride',
            'products_text': 'benzanilide',
            'reagents_text': 'NaOH (aq), CH2Cl2 (biphasic)',
            'conditions_text': '0°C, vigorous stirring',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase25_shallow_top10_v1 sprint for Schotten-Baumann Reaction. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Nc1ccccc1.ClC(=O)c2ccccc2', 'aniline + benzoyl chloride', 'reactants_text', 1),
                ('product', 'O=C(Nc1ccccc1)c2ccccc2', 'benzanilide', 'products_text', 1),
            ],
        },
        {
            'family': 'Schotten-Baumann Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Schotten-Baumann acylation of benzylamine with acetyl chloride.',
            'reactants_text': 'benzylamine + acetyl chloride',
            'products_text': 'N-benzylacetamide',
            'reagents_text': 'NaOH (aq), Et2O',
            'conditions_text': '0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase25_shallow_top10_v1 sprint for Schotten-Baumann Reaction. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'NCc1ccccc1.ClC(=O)C', 'benzylamine + acetyl chloride', 'reactants_text', 1),
                ('product', 'O=C(C)NCc1ccccc1', 'N-benzylacetamide', 'products_text', 1),
            ],
        },
        {
            'family': 'Schotten-Baumann Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Schotten-Baumann esterification of phenol.',
            'reactants_text': 'phenol + propanoyl chloride',
            'products_text': 'phenyl propanoate',
            'reagents_text': 'NaOH (aq), CH2Cl2',
            'conditions_text': '0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase25_shallow_top10_v1 sprint for Schotten-Baumann Reaction. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Oc1ccccc1.ClC(=O)CC', 'phenol + propanoyl chloride', 'reactants_text', 1),
                ('product', 'O=C(CC)Oc1ccccc1', 'phenyl propanoate', 'products_text', 1),
            ],
        },
        # === Schwartz Hydrozirconation ===
        {
            'family': 'Schwartz Hydrozirconation',
            'extract_kind': 'application_example',
            'transformation_text': 'Schwartz hydrozirconation: terminal alkyne + Cp2Zr(H)Cl gives (E)-vinyl zirconocene that can be protonated to alkene.',
            'reactants_text': '1-hexyne',
            'products_text': '(E)-1-hexene (via vinyl-Zr then hydrolysis)',
            'reagents_text': 'Cp2Zr(H)Cl (Schwartz reagent), then H2O',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase25_shallow_top10_v1 sprint for Schwartz Hydrozirconation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C#CCCCC', '1-hexyne', 'reactants_text', 1),
                ('product', 'C=CCCCC', '(E)-1-hexene (via vinyl-Zr then hydrolysis)', 'products_text', 1),
            ],
        },
        {
            'family': 'Schwartz Hydrozirconation',
            'extract_kind': 'application_example',
            'transformation_text': 'Schwartz hydrozirconation of phenylacetylene giving styrene after protonation.',
            'reactants_text': 'phenylacetylene',
            'products_text': 'styrene (via vinyl-Zr + H2O)',
            'reagents_text': 'Cp2Zr(H)Cl',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase25_shallow_top10_v1 sprint for Schwartz Hydrozirconation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C#Cc1ccccc1', 'phenylacetylene', 'reactants_text', 1),
                ('product', 'C=Cc1ccccc1', 'styrene (via vinyl-Zr + H2O)', 'products_text', 1),
            ],
        },
        {
            'family': 'Schwartz Hydrozirconation',
            'extract_kind': 'application_example',
            'transformation_text': 'Hydrozirconation followed by electrophilic iodination of vinyl-Zr gives (E)-vinyl iodide.',
            'reactants_text': '1-butyne',
            'products_text': '(E)-1-iodobutene',
            'reagents_text': 'Cp2Zr(H)Cl; then I2',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase25_shallow_top10_v1 sprint for Schwartz Hydrozirconation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C#CCC', '1-butyne', 'reactants_text', 1),
                ('product', 'CCC=CI', '(E)-1-iodobutene', 'products_text', 1),
            ],
        },
        # === Seyferth-Gilbert Homologation ===
        {
            'family': 'Seyferth-Gilbert Homologation',
            'extract_kind': 'application_example',
            'transformation_text': 'Seyferth-Gilbert: aldehyde + dimethyl (diazomethyl)phosphonate (Ohira-Bestmann reagent) + base gives terminal alkyne.',
            'reactants_text': 'benzaldehyde',
            'products_text': 'phenylacetylene',
            'reagents_text': '(MeO)2P(O)C(=N2)COCH3 (Ohira-Bestmann), K2CO3',
            'conditions_text': 'MeOH, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase25_shallow_top10_v1 sprint for Seyferth-Gilbert Homologation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1),
                ('product', 'C#Cc1ccccc1', 'phenylacetylene', 'products_text', 1),
            ],
        },
        {
            'family': 'Seyferth-Gilbert Homologation',
            'extract_kind': 'application_example',
            'transformation_text': 'Seyferth-Gilbert homologation of aliphatic aldehyde to terminal alkyne.',
            'reactants_text': 'propanal',
            'products_text': '1-butyne',
            'reagents_text': 'Ohira-Bestmann reagent, K2CO3',
            'conditions_text': 'MeOH, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase25_shallow_top10_v1 sprint for Seyferth-Gilbert Homologation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CCC', 'propanal', 'reactants_text', 1),
                ('product', 'C#CCC', '1-butyne', 'products_text', 1),
            ],
        },
        {
            'family': 'Seyferth-Gilbert Homologation',
            'extract_kind': 'application_example',
            'transformation_text': 'Seyferth-Gilbert homologation on branched aldehyde.',
            'reactants_text': 'cyclohexanecarboxaldehyde',
            'products_text': 'ethynylcyclohexane',
            'reagents_text': 'Ohira-Bestmann, K2CO3',
            'conditions_text': 'MeOH, 0°C→rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase25_shallow_top10_v1 sprint for Seyferth-Gilbert Homologation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CC1CCCCC1', 'cyclohexanecarboxaldehyde', 'reactants_text', 1),
                ('product', 'C#CC1CCCCC1', 'ethynylcyclohexane', 'products_text', 1),
            ],
        },
        # === Sharpless Asymmetric Aminohydroxylation ===
        {
            'family': 'Sharpless Asymmetric Aminohydroxylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Sharpless aminohydroxylation: alkene + Os cat. + chloramine-T + chiral ligand gives syn β-amino alcohol with ee.',
            'reactants_text': 'styrene',
            'products_text': '(1R,2S)-2-amino-1-phenyl-ethanol',
            'reagents_text': 'K2OsO2(OH)4, (DHQ)2PHAL, chloramine-T',
            'conditions_text': 'nPrOH/H2O, 0°C→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase25_shallow_top10_v1 sprint for Sharpless Asymmetric Aminohydroxylation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=Cc1ccccc1', 'styrene', 'reactants_text', 1),
                ('product', 'NC(c1ccccc1)C(O)', '(1R,2S)-2-amino-1-phenyl-ethanol', 'products_text', 1),
            ],
        },
        {
            'family': 'Sharpless Asymmetric Aminohydroxylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Sharpless AA on α,β-unsaturated ester to give chiral β-amino-α-hydroxy ester.',
            'reactants_text': 'methyl acrylate',
            'products_text': 'methyl 3-amino-2-hydroxy-propanoate',
            'reagents_text': 'Os cat., cinchona ligand, chloramine-M',
            'conditions_text': 'tBuOH/H2O',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase25_shallow_top10_v1 sprint for Sharpless Asymmetric Aminohydroxylation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CC(=O)OC', 'methyl acrylate', 'reactants_text', 1),
                ('product', 'NC(CO)C(=O)OC', 'methyl 3-amino-2-hydroxy-propanoate', 'products_text', 1),
            ],
        },
        {
            'family': 'Sharpless Asymmetric Aminohydroxylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Sharpless AA on electron-rich styrene.',
            'reactants_text': '4-methoxystyrene',
            'products_text': 'chiral amino alcohol',
            'reagents_text': 'Os cat., (DHQD)2PHAL',
            'conditions_text': 'tBuOH/H2O, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase25_shallow_top10_v1 sprint for Sharpless Asymmetric Aminohydroxylation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=Cc1ccc(OC)cc1', '4-methoxystyrene', 'reactants_text', 1),
                ('product', 'NC(c1ccc(OC)cc1)C(O)', 'chiral amino alcohol', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Sharpless Asymmetric Dihydroxylation ===
        {
            'family': 'Sharpless Asymmetric Dihydroxylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Sharpless AD: alkene + K2OsO4/K3Fe(CN)6 + chiral ligand gives chiral cis-diol.',
            'reactants_text': 'styrene',
            'products_text': '(1S,2S)-1-phenyl-1,2-ethanediol',
            'reagents_text': 'OsO4 (cat.), (DHQD)2PHAL, K3Fe(CN)6',
            'conditions_text': 'tBuOH/H2O, 0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase25_shallow_top10_v1 sprint for Sharpless Asymmetric Dihydroxylation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=Cc1ccccc1', 'styrene', 'reactants_text', 1),
                ('product', 'OC(c1ccccc1)C(O)', '(1S,2S)-1-phenyl-1,2-ethanediol', 'products_text', 1),
            ],
        },
        {
            'family': 'Sharpless Asymmetric Dihydroxylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Sharpless AD of β-methylstyrene giving syn-diol.',
            'reactants_text': 'β-methylstyrene',
            'products_text': 'chiral cis-diol',
            'reagents_text': 'OsO4, (DHQ)2PHAL, K3Fe(CN)6',
            'conditions_text': 'tBuOH/H2O, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase25_shallow_top10_v1 sprint for Sharpless Asymmetric Dihydroxylation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC=Cc1ccccc1', 'β-methylstyrene', 'reactants_text', 1),
                ('product', 'OC(C)C(O)c1ccccc1', 'chiral cis-diol', 'products_text', 1),
            ],
        },
        {
            'family': 'Sharpless Asymmetric Dihydroxylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Sharpless AD on α,β-unsaturated ester.',
            'reactants_text': 'ethyl crotonate',
            'products_text': 'chiral diol ester',
            'reagents_text': 'OsO4 (AD-mix-β)',
            'conditions_text': 'tBuOH/H2O, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase25_shallow_top10_v1 sprint for Sharpless Asymmetric Dihydroxylation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C(=CC)C(=O)OCC', 'ethyl crotonate', 'reactants_text', 1),
                ('product', 'OC(C)C(O)C(=O)OCC', 'chiral diol ester', 'products_text', 1),
            ],
        },
        # === Sharpless Asymmetric Epoxidation ===
        {
            'family': 'Sharpless Asymmetric Epoxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Sharpless AE: allylic alcohol + Ti(OiPr)4/(+)-DIPT/TBHP gives chiral epoxide with predictable stereochemistry.',
            'reactants_text': 'allyl alcohol',
            'products_text': '(R)-glycidol',
            'reagents_text': 'Ti(OiPr)4, (+)-DIPT, TBHP, 4Å MS',
            'conditions_text': 'CH2Cl2, -20°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase25_shallow_top10_v1 sprint for Sharpless Asymmetric Epoxidation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCC=C', 'allyl alcohol', 'reactants_text', 1),
                ('product', 'OCC1OC1', '(R)-glycidol', 'products_text', 1),
            ],
        },
        {
            'family': 'Sharpless Asymmetric Epoxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Sharpless AE of crotyl alcohol giving trans-epoxy alcohol.',
            'reactants_text': '(E)-2-buten-1-ol',
            'products_text': '(2R,3R)-2,3-epoxybutan-1-ol',
            'reagents_text': 'Ti(OiPr)4, (+)-DET, TBHP',
            'conditions_text': 'CH2Cl2, -20°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase25_shallow_top10_v1 sprint for Sharpless Asymmetric Epoxidation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCC=CC', '(E)-2-buten-1-ol', 'reactants_text', 1),
                ('product', 'OCC1OC1C', '(2R,3R)-2,3-epoxybutan-1-ol', 'products_text', 1),
            ],
        },
        {
            'family': 'Sharpless Asymmetric Epoxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Sharpless AE of cinnamyl alcohol.',
            'reactants_text': 'cinnamyl alcohol',
            'products_text': '(2S,3S)-2,3-epoxy-3-phenyl-1-propanol',
            'reagents_text': 'Ti(OiPr)4, (-)-DIPT, TBHP',
            'conditions_text': 'CH2Cl2, -20°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase25_shallow_top10_v1 sprint for Sharpless Asymmetric Epoxidation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCC=Cc1ccccc1', 'cinnamyl alcohol', 'reactants_text', 1),
                ('product', 'OCC1OC1c1ccccc1', '(2S,3S)-2,3-epoxy-3-phenyl-1-propanol', 'products_text', 1),
            ],
        },
        # === Shi Asymmetric Epoxidation ===
        {
            'family': 'Shi Asymmetric Epoxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Shi asymmetric epoxidation: alkene + Shi fructose-derived ketone catalyst + Oxone gives chiral epoxide for trans-alkenes.',
            'reactants_text': 'styrene',
            'products_text': '(R)-styrene oxide',
            'reagents_text': 'Shi fructose ketone, Oxone, NaHCO3',
            'conditions_text': 'CH3CN/H2O, 0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase25_shallow_top10_v1 sprint for Shi Asymmetric Epoxidation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=Cc1ccccc1', 'styrene', 'reactants_text', 1),
                ('product', 'C1OC1c1ccccc1', '(R)-styrene oxide', 'products_text', 1),
            ],
        },
        {
            'family': 'Shi Asymmetric Epoxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Shi epoxidation of trans-β-methylstyrene.',
            'reactants_text': 'trans-β-methylstyrene',
            'products_text': '(1R,2R)-1-methyl-2-phenyl-oxirane',
            'reagents_text': 'Shi ketone, Oxone, K2CO3',
            'conditions_text': 'CH3CN/DMM/H2O, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase25_shallow_top10_v1 sprint for Shi Asymmetric Epoxidation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC=Cc1ccccc1', 'trans-β-methylstyrene', 'reactants_text', 1),
                ('product', 'CC1OC1c1ccccc1', '(1R,2R)-1-methyl-2-phenyl-oxirane', 'products_text', 1),
            ],
        },
        {
            'family': 'Shi Asymmetric Epoxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Shi epoxidation of cis-alkene, typically lower ee but still active.',
            'reactants_text': 'cyclohexene',
            'products_text': 'cyclohexene oxide (racemic with Shi)',
            'reagents_text': 'Shi catalyst, Oxone',
            'conditions_text': 'CH3CN/H2O, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase25_shallow_top10_v1 sprint for Shi Asymmetric Epoxidation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C1=CCCCC1', 'cyclohexene', 'reactants_text', 1),
                ('product', 'C2CC3OC3CC2', 'cyclohexene oxide (racemic with Shi)', 'products_text', 1),
            ],
        },
        # === Simmons-Smith Cyclopropanation ===
        {
            'family': 'Simmons-Smith Cyclopropanation',
            'extract_kind': 'application_example',
            'transformation_text': 'Simmons-Smith: alkene + CH2I2/Zn-Cu gives cyclopropane stereospecifically.',
            'reactants_text': 'styrene',
            'products_text': 'phenylcyclopropane',
            'reagents_text': 'CH2I2, Zn-Cu couple',
            'conditions_text': 'Et2O, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase25_shallow_top10_v1 sprint for Simmons-Smith Cyclopropanation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=Cc1ccccc1', 'styrene', 'reactants_text', 1),
                ('product', 'C1CC1c1ccccc1', 'phenylcyclopropane', 'products_text', 1),
            ],
        },
        {
            'family': 'Simmons-Smith Cyclopropanation',
            'extract_kind': 'application_example',
            'transformation_text': 'Simmons-Smith bis-cyclopropanation of diene.',
            'reactants_text': '1,5-hexadiene',
            'products_text': "1,1'-bis-cyclopropyl (double cyclopropanation)",
            'reagents_text': 'CH2I2, Zn-Cu (excess)',
            'conditions_text': 'Et2O, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase25_shallow_top10_v1 sprint for Simmons-Smith Cyclopropanation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCCC=C', '1,5-hexadiene', 'reactants_text', 1),
                ('product', 'C1CC1CCC1CC1', "1,1'-bis-cyclopropyl (double cyclopropanation)", 'products_text', 1),
            ],
        },
        {
            'family': 'Simmons-Smith Cyclopropanation',
            'extract_kind': 'application_example',
            'transformation_text': 'Simmons-Smith directed by allylic alcohol (OH coordinates to Zn).',
            'reactants_text': '(E)-2-buten-1-ol',
            'products_text': '2-methylcyclopropylmethanol',
            'reagents_text': 'Et2Zn, CH2I2',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase25_shallow_top10_v1 sprint for Simmons-Smith Cyclopropanation. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCC=CC', '(E)-2-buten-1-ol', 'reactants_text', 1),
                ('product', 'OCC1CC1C', '2-methylcyclopropylmethanol', 'products_text', 1),
            ],
        },
        # === Simmons-Smith Reaction ===
        {
            'family': 'Simmons-Smith Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Simmons-Smith reaction: alkene + CH2I2/Zn gives cyclopropane; retains alkene stereochemistry.',
            'reactants_text': '2-butene',
            'products_text': '1,2-dimethylcyclopropane',
            'reagents_text': 'CH2I2, Zn-Cu',
            'conditions_text': 'Et2O, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase25_shallow_top10_v1 sprint for Simmons-Smith Reaction. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC=CC', '2-butene', 'reactants_text', 1),
                ('product', 'CC1CC1C', '1,2-dimethylcyclopropane', 'products_text', 1),
            ],
        },
        {
            'family': 'Simmons-Smith Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Simmons-Smith on cyclohexene giving norcarane.',
            'reactants_text': 'cyclohexene',
            'products_text': 'bicyclo[4.1.0]heptane (norcarane)',
            'reagents_text': 'CH2I2, Zn-Cu',
            'conditions_text': 'Et2O',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase25_shallow_top10_v1 sprint for Simmons-Smith Reaction. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C1=CCCCC1', 'cyclohexene', 'reactants_text', 1),
                ('product', 'C2CC3CC2CC3', 'bicyclo[4.1.0]heptane (norcarane)', 'products_text', 1),
            ],
        },
        {
            'family': 'Simmons-Smith Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Simmons-Smith on trisubstituted alkene.',
            'reactants_text': 'α-methylstyrene',
            'products_text': '1-methyl-1-phenyl-cyclopropane',
            'reagents_text': 'Et2Zn, CH2I2',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase25_shallow_top10_v1 sprint for Simmons-Smith Reaction. [phase25_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C(=C)c1ccccc1', 'α-methylstyrene', 'reactants_text', 1),
                ('product', 'C1CC1(c1ccccc1)C', '1-methyl-1-phenyl-cyclopropane', 'products_text', 1),
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
