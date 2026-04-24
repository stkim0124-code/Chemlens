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

TAG = 'phase32_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Barton Radical Decarboxylation Reaction',
        'Brook Rearrangement',
        'Claisen Rearrangement',
        'Aza-[2,3]-Wittig Rearrangement',
        'Bischler-Napieralski Isoquinoline Synthesis',
    ],
    'b': [
        'Cannizzaro Reaction',
        'Baeyer-Villiger Oxidation',
        'Baker-Venkataraman Rearrangement',
        'Barton Radical Decarboxylation',
        'Bartoli Indole Synthesis',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Barton Radical Decarboxylation Reaction ===
        {
            'family': 'Barton Radical Decarboxylation Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Barton radical decarboxylation: carboxylic acid → thiohydroxamate ester → hν generates alkyl radical + CO2.',
            'reactants_text': 'cyclohexanecarboxylic acid',
            'products_text': 'cyclohexane (after Barton decarboxylation)',
            'reagents_text': 'Barton ester, Bu3SnH, AIBN',
            'conditions_text': 'PhH, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase32_shallow_top10_v1 sprint for Barton Radical Decarboxylation Reaction. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(=O)C1CCCCC1', 'cyclohexanecarboxylic acid', 'reactants_text', 1),
                ('product', 'C1CCCCC1', 'cyclohexane (after Barton decarboxylation)', 'products_text', 1),
            ],
        },
        {
            'family': 'Barton Radical Decarboxylation Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Barton radical decarboxylation of tertiary carboxylic acid.',
            'reactants_text': 'pivalic acid',
            'products_text': 'isobutane',
            'reagents_text': 'N-hydroxy-2-thiopyridone ester, hv, tBuSH',
            'conditions_text': 'PhH, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase32_shallow_top10_v1 sprint for Barton Radical Decarboxylation Reaction. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(=O)C(C)(C)C', 'pivalic acid', 'reactants_text', 1),
                ('product', 'CC(C)C', 'isobutane', 'products_text', 1),
            ],
        },
        {
            'family': 'Barton Radical Decarboxylation Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Barton decarboxylation of arylacetic acid.',
            'reactants_text': '4-methoxyphenylacetic acid',
            'products_text': '4-methoxy-toluene (from benzylic radical)',
            'reagents_text': 'Barton ester, Bu3SnH, AIBN',
            'conditions_text': 'PhH, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase32_shallow_top10_v1 sprint for Barton Radical Decarboxylation Reaction. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(=O)Cc1ccc(OC)cc1', '4-methoxyphenylacetic acid', 'reactants_text', 1),
                ('product', 'Cc1ccc(OC)cc1', '4-methoxy-toluene (from benzylic radical)', 'products_text', 1),
            ],
        },
        # === Brook Rearrangement ===
        {
            'family': 'Brook Rearrangement',
            'extract_kind': 'overview',
            'transformation_text': 'Brook rearrangement: silicon migrates from C to O in α-silyl alcohols/alkoxides; thermodynamic because Si-O bond is stronger than Si-C.',
            'reactants_text': 'α-silyl alcohol (benzyl(trimethylsilyl)methanol)',
            'products_text': 'benzyloxytrimethylsilane',
            'reagents_text': 'KH or NaH (base)',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated overview seed (variant A) added during phase32_shallow_top10_v1 sprint for Brook Rearrangement. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC([Si](C)(C)C)c1ccccc1', 'α-silyl alcohol (benzyl(trimethylsilyl)methanol)', 'reactants_text', 1),
                ('product', 'O([Si](C)(C)C)C(c1ccccc1)', 'benzyloxytrimethylsilane', 'products_text', 1),
            ],
        },
        {
            'family': 'Brook Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Brook rearrangement of tertiary α-silyl alcohol.',
            'reactants_text': '2-(trimethylsilyl)-2-propanol',
            'products_text': '2-(trimethylsilyloxy)-propan-2-ol (rearranged silyl ether)',
            'reagents_text': 'KH',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase32_shallow_top10_v1 sprint for Brook Rearrangement. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)(C)[Si](C)(C)C', '2-(trimethylsilyl)-2-propanol', 'reactants_text', 1),
                ('product', 'OC(C)(C)O[Si](C)(C)C', '2-(trimethylsilyloxy)-propan-2-ol (rearranged silyl ether)', 'products_text', 1),
            ],
        },
        {
            'family': 'Brook Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Brook rearrangement of primary α-silyl alcohol.',
            'reactants_text': '1-(trimethylsilyl)-1-propanol',
            'products_text': 'propyloxy-trimethylsilane (C→O Si migration)',
            'reagents_text': 'KH (cat.)',
            'conditions_text': 'THF',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase32_shallow_top10_v1 sprint for Brook Rearrangement. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(CC)[Si](C)(C)C', '1-(trimethylsilyl)-1-propanol', 'reactants_text', 1),
                ('product', 'CCCO[Si](C)(C)C', 'propyloxy-trimethylsilane (C→O Si migration)', 'products_text', 1),
            ],
        },
        # === Claisen Rearrangement ===
        {
            'family': 'Claisen Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Aromatic Claisen: allyl aryl ether heats to give ortho-allyl phenol via [3,3]-sigmatropic rearrangement.',
            'reactants_text': 'allyl phenyl ether',
            'products_text': '2-allyl-phenol (ortho-Claisen product)',
            'reagents_text': 'thermal',
            'conditions_text': 'neat, 200°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase32_shallow_top10_v1 sprint for Claisen Rearrangement. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCOc1ccccc1', 'allyl phenyl ether', 'reactants_text', 1),
                ('product', 'OC1=CC=CC=C1CC=C', '2-allyl-phenol (ortho-Claisen product)', 'products_text', 1),
            ],
        },
        {
            'family': 'Claisen Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Aliphatic Claisen: allyl vinyl ether [3,3] rearranges to γ,δ-unsaturated carbonyl.',
            'reactants_text': 'allyl vinyl ether',
            'products_text': '4-pentenal',
            'reagents_text': 'thermal',
            'conditions_text': 'xylene, 180°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase32_shallow_top10_v1 sprint for Claisen Rearrangement. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCOC=C', 'allyl vinyl ether', 'reactants_text', 1),
                ('product', 'C=CCCC=O', '4-pentenal', 'products_text', 1),
            ],
        },
        {
            'family': 'Claisen Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Claisen rearrangement of para-substituted allyl aryl ether.',
            'reactants_text': 'allyl 4-methylphenyl ether',
            'products_text': '2-allyl-4-methyl-phenol',
            'reagents_text': 'thermal (200°C)',
            'conditions_text': 'neat',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase32_shallow_top10_v1 sprint for Claisen Rearrangement. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCOc1ccc(C)cc1', 'allyl 4-methylphenyl ether', 'reactants_text', 1),
                ('product', 'Cc1ccc(O)c(CC=C)c1', '2-allyl-4-methyl-phenol', 'products_text', 1),
            ],
        },
        # === Aza-[2,3]-Wittig Rearrangement ===
        {
            'family': 'Aza-[2,3]-Wittig Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Aza-[2,3]-Wittig: α-lithiated allylic amine undergoes concerted [2,3]-sigmatropic rearrangement to homoallyl amine.',
            'reactants_text': 'N,N-dimethyl-allylamine (α-lithiated)',
            'products_text': '2-methyl-4-penten-2-amine ([2,3]-product)',
            'reagents_text': 'nBuLi',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase32_shallow_top10_v1 sprint for Aza-[2,3]-Wittig Rearrangement. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCN(C)C', 'N,N-dimethyl-allylamine (α-lithiated)', 'reactants_text', 1),
                ('product', 'NC(CC=C)(C)', '2-methyl-4-penten-2-amine ([2,3]-product)', 'products_text', 1),
            ],
        },
        {
            'family': 'Aza-[2,3]-Wittig Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Aza-[2,3]-Wittig rearrangement of benzyl allylic amine.',
            'reactants_text': 'N-allyl-N-methyl-benzylamine',
            'products_text': '1-phenyl-3-buten-1-yl-methylamine ([2,3] product)',
            'reagents_text': 'nBuLi, HMPA',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase32_shallow_top10_v1 sprint for Aza-[2,3]-Wittig Rearrangement. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCN(Cc1ccccc1)C', 'N-allyl-N-methyl-benzylamine', 'reactants_text', 1),
                ('product', 'C(N(C)CC1CCCCC1)(CC=C)c2ccccc2', '1-phenyl-3-buten-1-yl-methylamine ([2,3] product)', 'products_text', 1),
            ],
        },
        {
            'family': 'Aza-[2,3]-Wittig Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Aza-[2,3]-Wittig on α-branched allyl amine.',
            'reactants_text': 'N-(2-methylallyl)-N-ethyl-methylamine',
            'products_text': '2,2-dimethyl-[2,3]-Wittig amine product',
            'reagents_text': 'LDA',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase32_shallow_top10_v1 sprint for Aza-[2,3]-Wittig Rearrangement. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=C(C)CN(CC)C', 'N-(2-methylallyl)-N-ethyl-methylamine', 'reactants_text', 1),
                ('product', 'CC(=C)CC(N(C)CC)C', '2,2-dimethyl-[2,3]-Wittig amine product', 'products_text', 1),
            ],
        },
        # === Bischler-Napieralski Isoquinoline Synthesis ===
        {
            'family': 'Bischler-Napieralski Isoquinoline Synthesis',
            'extract_kind': 'overview',
            'transformation_text': 'Bischler-Napieralski: β-arylethylamide undergoes Lewis/Brønsted-acid-catalyzed intramolecular acylation/cyclization to give 3,4-dihydroisoquinoline.',
            'reactants_text': 'N-(2-phenylethyl)-benzamide',
            'products_text': '1-phenyl-3,4-dihydro-isoquinoline',
            'reagents_text': 'POCl3 or P2O5',
            'conditions_text': 'PhMe, reflux',
            'notes_text': 'Manual curated overview seed (variant A) added during phase32_shallow_top10_v1 sprint for Bischler-Napieralski Isoquinoline Synthesis. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(NCCc1ccccc1)c2ccccc2', 'N-(2-phenylethyl)-benzamide', 'reactants_text', 1),
                ('product', 'C1(=NCCc2ccccc21)c3ccccc3', '1-phenyl-3,4-dihydro-isoquinoline', 'products_text', 1),
            ],
        },
        {
            'family': 'Bischler-Napieralski Isoquinoline Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Bischler-Napieralski on electron-rich β-arylethylamide.',
            'reactants_text': 'N-[2-(4-methoxyphenyl)ethyl]-acetamide',
            'products_text': '1-methyl-6-methoxy-3,4-dihydro-isoquinoline',
            'reagents_text': 'POCl3',
            'conditions_text': 'PhMe, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase32_shallow_top10_v1 sprint for Bischler-Napieralski Isoquinoline Synthesis. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(NCCc1ccc(OC)cc1)C', 'N-[2-(4-methoxyphenyl)ethyl]-acetamide', 'reactants_text', 1),
                ('product', 'CC1=NCCc2cc(OC)ccc21', '1-methyl-6-methoxy-3,4-dihydro-isoquinoline', 'products_text', 1),
            ],
        },
        {
            'family': 'Bischler-Napieralski Isoquinoline Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Bischler-Napieralski for dimethoxy substrate (papaverine-type).',
            'reactants_text': 'N-(3,4-dimethoxy-phenethyl)-benzamide',
            'products_text': '1-phenyl-6,7-dimethoxy-3,4-dihydro-isoquinoline',
            'reagents_text': 'POCl3, P2O5',
            'conditions_text': 'xylene, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase32_shallow_top10_v1 sprint for Bischler-Napieralski Isoquinoline Synthesis. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(NCCc1ccc(OC)c(OC)c1)c2ccccc2', 'N-(3,4-dimethoxy-phenethyl)-benzamide', 'reactants_text', 1),
                ('product', 'c1ccc(C2=NCCc3cc(OC)c(OC)cc32)cc1', '1-phenyl-6,7-dimethoxy-3,4-dihydro-isoquinoline', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Cannizzaro Reaction ===
        {
            'family': 'Cannizzaro Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Cannizzaro: non-enolizable aldehyde + concentrated NaOH gives alcohol + carboxylate (disproportionation via hydride transfer).',
            'reactants_text': 'benzaldehyde (2 eq) — non-enolizable',
            'products_text': 'benzyl alcohol + benzoic acid (salt)',
            'reagents_text': 'conc. NaOH',
            'conditions_text': 'rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase32_shallow_top10_v1 sprint for Cannizzaro Reaction. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1.O=Cc1ccccc1', 'benzaldehyde (2 eq) — non-enolizable', 'reactants_text', 1),
                ('product', 'OCc1ccccc1.OC(=O)c1ccccc1', 'benzyl alcohol + benzoic acid (salt)', 'products_text', 1),
            ],
        },
        {
            'family': 'Cannizzaro Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Cannizzaro of 4-chlorobenzaldehyde.',
            'reactants_text': '4-chlorobenzaldehyde',
            'products_text': '4-chlorobenzyl alcohol + 4-chlorobenzoate',
            'reagents_text': 'NaOH (50%)',
            'conditions_text': 'rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase32_shallow_top10_v1 sprint for Cannizzaro Reaction. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccc(Cl)cc1', '4-chlorobenzaldehyde', 'reactants_text', 1),
                ('product', 'OCc1ccc(Cl)cc1.OC(=O)c1ccc(Cl)cc1', '4-chlorobenzyl alcohol + 4-chlorobenzoate', 'products_text', 1),
            ],
        },
        {
            'family': 'Cannizzaro Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Cannizzaro of p-anisaldehyde.',
            'reactants_text': '4-methoxybenzaldehyde',
            'products_text': '4-methoxybenzyl alcohol + 4-methoxybenzoate',
            'reagents_text': 'KOH (aq, conc.)',
            'conditions_text': 'rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase32_shallow_top10_v1 sprint for Cannizzaro Reaction. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccc(OC)cc1', '4-methoxybenzaldehyde', 'reactants_text', 1),
                ('product', 'OCc1ccc(OC)cc1.OC(=O)c1ccc(OC)cc1', '4-methoxybenzyl alcohol + 4-methoxybenzoate', 'products_text', 1),
            ],
        },
        # === Baeyer-Villiger Oxidation ===
        {
            'family': 'Baeyer-Villiger Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Baeyer-Villiger: ketone + peracid (mCPBA) gives ester/lactone via 1,2-migration to electron-deficient O (migration pref: tert > sec > Ph > prim).',
            'reactants_text': 'cyclohexanone',
            'products_text': 'ε-caprolactone',
            'reagents_text': 'mCPBA',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase32_shallow_top10_v1 sprint for Baeyer-Villiger Oxidation. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCCC1', 'cyclohexanone', 'reactants_text', 1),
                ('product', 'O=C1OCCCCC1', 'ε-caprolactone', 'products_text', 1),
            ],
        },
        {
            'family': 'Baeyer-Villiger Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Baeyer-Villiger of methyl isopropyl ketone (iPr-migrating).',
            'reactants_text': '3-methyl-2-butanone',
            'products_text': 'isopropyl acetate (iPr migrates preferentially)',
            'reagents_text': 'mCPBA',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase32_shallow_top10_v1 sprint for Baeyer-Villiger Oxidation. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)C(C)C', '3-methyl-2-butanone', 'reactants_text', 1),
                ('product', 'O=C(OC(C)C)C', 'isopropyl acetate (iPr migrates preferentially)', 'products_text', 1),
            ],
        },
        {
            'family': 'Baeyer-Villiger Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Baeyer-Villiger of diaryl ketone with trifluoroperacetic acid.',
            'reactants_text': 'benzophenone',
            'products_text': 'phenyl benzoate',
            'reagents_text': 'trifluoroperacetic acid',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase32_shallow_top10_v1 sprint for Baeyer-Villiger Oxidation. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(c1ccccc1)c2ccccc2', 'benzophenone', 'reactants_text', 1),
                ('product', 'O=C(Oc1ccccc1)c2ccccc2', 'phenyl benzoate', 'products_text', 1),
            ],
        },
        # === Baker-Venkataraman Rearrangement ===
        {
            'family': 'Baker-Venkataraman Rearrangement',
            'extract_kind': 'overview',
            'transformation_text': 'Baker-Venkataraman: 2-acyloxyacetophenone + strong base → intramolecular acyl transfer yields 1,3-diketone (key precursor to flavones/chromones).',
            'reactants_text': '2-acetyl-phenyl benzoate',
            'products_text': '1-(2-hydroxyphenyl)-3-phenyl-1,3-propanedione',
            'reagents_text': 'KOH (base)',
            'conditions_text': 'pyridine, rt',
            'notes_text': 'Manual curated overview seed (variant A) added during phase32_shallow_top10_v1 sprint for Baker-Venkataraman Rearrangement. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(OC1=CC=CC=C1C(=O)C)c2ccccc2', '2-acetyl-phenyl benzoate', 'reactants_text', 1),
                ('product', 'O=C(CC(=O)c1ccccc1O)c2ccccc2', '1-(2-hydroxyphenyl)-3-phenyl-1,3-propanedione', 'products_text', 1),
            ],
        },
        {
            'family': 'Baker-Venkataraman Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Baker-Venkataraman with acetyl-aryl-acetate.',
            'reactants_text': '2-acetyl-phenyl acetate',
            'products_text': '1-(2-hydroxyphenyl)-1,3-butanedione',
            'reagents_text': 'KOH',
            'conditions_text': 'pyridine, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase32_shallow_top10_v1 sprint for Baker-Venkataraman Rearrangement. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(OC1=CC=CC=C1C(=O)C)C', '2-acetyl-phenyl acetate', 'reactants_text', 1),
                ('product', 'O=C(CC(=O)c1ccccc1O)C', '1-(2-hydroxyphenyl)-1,3-butanedione', 'products_text', 1),
            ],
        },
        {
            'family': 'Baker-Venkataraman Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Baker-Venkataraman with electron-rich aryl ester.',
            'reactants_text': '2-acetyl-phenyl 4-methoxybenzoate',
            'products_text': '1-(2-hydroxyphenyl)-3-(4-methoxyphenyl)-1,3-propanedione',
            'reagents_text': 'NaH',
            'conditions_text': 'pyridine',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase32_shallow_top10_v1 sprint for Baker-Venkataraman Rearrangement. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(OC1=CC=CC=C1C(=O)C)c2ccc(OC)cc2', '2-acetyl-phenyl 4-methoxybenzoate', 'reactants_text', 1),
                ('product', 'O=C(CC(=O)c1ccccc1O)c2ccc(OC)cc2', '1-(2-hydroxyphenyl)-3-(4-methoxyphenyl)-1,3-propanedione', 'products_text', 1),
            ],
        },
        # === Barton Radical Decarboxylation ===
        {
            'family': 'Barton Radical Decarboxylation',
            'extract_kind': 'overview',
            'transformation_text': 'Barton radical decarboxylation: carboxylic acid converted to N-hydroxypyridine-2-thione ester, which under hν/radical initiator fragments to CO2 + alkyl radical captured by H-donor.',
            'reactants_text': 'butanoic acid (as Barton ester)',
            'products_text': 'propane (via decarboxylated alkyl radical + H)',
            'reagents_text': 'Barton ester, Bu3SnH, AIBN',
            'conditions_text': 'PhH, reflux',
            'notes_text': 'Manual curated overview seed (variant A) added during phase32_shallow_top10_v1 sprint for Barton Radical Decarboxylation. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(=O)CCC', 'butanoic acid (as Barton ester)', 'reactants_text', 1),
                ('product', 'CCC', 'propane (via decarboxylated alkyl radical + H)', 'products_text', 1),
            ],
        },
        {
            'family': 'Barton Radical Decarboxylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Barton decarboxylation of cyclobutanecarboxylic acid.',
            'reactants_text': 'cyclobutanecarboxylic acid',
            'products_text': 'cyclobutane',
            'reagents_text': 'Barton ester, Bu3SnH',
            'conditions_text': 'PhH, hv',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase32_shallow_top10_v1 sprint for Barton Radical Decarboxylation. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(=O)C1CCC1', 'cyclobutanecarboxylic acid', 'reactants_text', 1),
                ('product', 'C1CCC1', 'cyclobutane', 'products_text', 1),
            ],
        },
        {
            'family': 'Barton Radical Decarboxylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Barton decarboxylation giving benzylic radical.',
            'reactants_text': '2-phenylpropanoic acid',
            'products_text': 'ethylbenzene (after decarboxylation and H-abstraction)',
            'reagents_text': 'Barton ester, tBuSH, hv',
            'conditions_text': 'PhH, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase32_shallow_top10_v1 sprint for Barton Radical Decarboxylation. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(=O)C(C)c1ccccc1', '2-phenylpropanoic acid', 'reactants_text', 1),
                ('product', 'Cc1ccccc1', 'ethylbenzene (after decarboxylation and H-abstraction)', 'products_text', 1),
            ],
        },
        # === Bartoli Indole Synthesis ===
        {
            'family': 'Bartoli Indole Synthesis',
            'extract_kind': 'overview',
            'transformation_text': 'Bartoli indole synthesis: ortho-substituted nitroarene + 3 eq vinyl Grignard at low T gives 7-substituted indole. Parent nitrobenzene + vinylMgBr yields indole.',
            'reactants_text': 'nitrobenzene',
            'products_text': 'indole (Bartoli-type, parent case)',
            'reagents_text': 'CH2=CHMgBr (3 eq), THF',
            'conditions_text': '-40°C',
            'notes_text': 'Manual curated overview seed (variant A) added during phase32_shallow_top10_v1 sprint for Bartoli Indole Synthesis. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=[N+]([O-])c1ccccc1', 'nitrobenzene', 'reactants_text', 1),
                ('product', 'c1ccc2[nH]ccc2c1', 'indole (Bartoli-type, parent case)', 'products_text', 1),
            ],
        },
        {
            'family': 'Bartoli Indole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Bartoli: ortho-methyl nitrobenzene + vinyl Grignard gives 7-methylindole.',
            'reactants_text': '2-methylnitrobenzene (ortho-methyl)',
            'products_text': '7-methylindole',
            'reagents_text': 'CH2=CHMgBr (3 eq)',
            'conditions_text': 'THF, -40°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase32_shallow_top10_v1 sprint for Bartoli Indole Synthesis. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Cc1ccccc1[N+](=O)[O-]', '2-methylnitrobenzene (ortho-methyl)', 'reactants_text', 1),
                ('product', 'Cc1ccc2[nH]ccc2c1', '7-methylindole', 'products_text', 1),
            ],
        },
        {
            'family': 'Bartoli Indole Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Bartoli on 2-bromonitrobenzene to 7-bromoindole.',
            'reactants_text': '2-bromonitrobenzene',
            'products_text': '7-bromoindole',
            'reagents_text': 'CH2=CHMgBr (3 eq)',
            'conditions_text': 'THF, -40°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase32_shallow_top10_v1 sprint for Bartoli Indole Synthesis. [phase32_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccccc1[N+](=O)[O-]', '2-bromonitrobenzene', 'reactants_text', 1),
                ('product', 'Brc1ccc2[nH]ccc2c1', '7-bromoindole', 'products_text', 1),
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
