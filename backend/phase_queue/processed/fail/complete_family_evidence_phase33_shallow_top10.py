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

TAG = 'phase33_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Barton Nitrite Ester Reaction',
        'Claisen-Ireland Rearrangement',
        'Arndt-Eistert Homologation / Synthesis',
        'Alkyne Metathesis',
        'Beckmann Rearrangement',
    ],
    'b': [
        'Aza-Cope Rearrangement',
        'Brook Rearrangement',
        'Aza-Wittig Reaction',
        'Clemmensen Reduction',
        'Bischler-Napieralski Isoquinoline Synthesis',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Barton Nitrite Ester Reaction ===
        {
            'family': 'Barton Nitrite Ester Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Barton nitrite ester photolysis: alcohol → nitrite ester → hν generates alkoxyl radical that performs δ-HAT; C-radical recombines with NO, giving δ-oxime.',
            'reactants_text': '1,1-dimethylcyclohexan-2-ol',
            'products_text': 'δ-oxime (after Barton nitrite photolysis and tautomerization)',
            'reagents_text': '(1) NOCl or BuONO; (2) hv',
            'conditions_text': 'PhH/dioxane, rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase33_shallow_top10_v1 sprint for Barton Nitrite Ester Reaction. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC1CCCCC1(C)C', '1,1-dimethylcyclohexan-2-ol', 'reactants_text', 1),
                ('product', 'ON=CC1CCCCC1(C)C', 'δ-oxime (after Barton nitrite photolysis and tautomerization)', 'products_text', 1),
            ],
        },
        {
            'family': 'Barton Nitrite Ester Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Barton nitrite ester on primary alcohol.',
            'reactants_text': 'cyclohexylmethanol',
            'products_text': 'cyclohexanecarboxaldehyde oxime (δ-HAT then tautomer)',
            'reagents_text': 'NOCl; hv',
            'conditions_text': 'PhH, rt',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase33_shallow_top10_v1 sprint for Barton Nitrite Ester Reaction. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCC1CCCCC1', 'cyclohexylmethanol', 'reactants_text', 1),
                ('product', 'ON=CC1CCCCC1', 'cyclohexanecarboxaldehyde oxime (δ-HAT then tautomer)', 'products_text', 1),
            ],
        },
        {
            'family': 'Barton Nitrite Ester Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Barton nitrite photolysis on tertiary alcohol.',
            'reactants_text': '2-methyl-2-octanol',
            'products_text': 'γ-oxime (δ-HAT product)',
            'reagents_text': 'BuONO; hv',
            'conditions_text': 'dioxane',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase33_shallow_top10_v1 sprint for Barton Nitrite Ester Reaction. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)(C)CCCCC', '2-methyl-2-octanol', 'reactants_text', 1),
                ('product', 'CC(C)(O)CCC(C=NO)C', 'γ-oxime (δ-HAT product)', 'products_text', 1),
            ],
        },
        # === Claisen-Ireland Rearrangement ===
        {
            'family': 'Claisen-Ireland Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Ireland-Claisen: allyl ester → silyl ketene acetal → [3,3] sigmatropic rearrangement → γ,δ-unsaturated carboxylic acid.',
            'reactants_text': 'allyl acetate',
            'products_text': '4-pentenoic acid (Ireland product)',
            'reagents_text': 'LDA, TMSCl; then Δ; H3O+',
            'conditions_text': 'THF, -78°C→rt',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase33_shallow_top10_v1 sprint for Claisen-Ireland Rearrangement. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCOC(=O)C', 'allyl acetate', 'reactants_text', 1),
                ('product', 'OC(=O)CC(C)C=C', '4-pentenoic acid (Ireland product)', 'products_text', 1),
            ],
        },
        {
            'family': 'Claisen-Ireland Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Ireland-Claisen of allyl propanoate.',
            'reactants_text': 'allyl propanoate',
            'products_text': '2-methyl-4-pentenoic acid',
            'reagents_text': 'LDA, TBSCl; then Δ; hydrolysis',
            'conditions_text': 'THF',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase33_shallow_top10_v1 sprint for Claisen-Ireland Rearrangement. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCOC(=O)CC', 'allyl propanoate', 'reactants_text', 1),
                ('product', 'OC(=O)C(CC)CC=C', '2-methyl-4-pentenoic acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Claisen-Ireland Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Ireland-Claisen on aryl ester.',
            'reactants_text': 'allyl benzoate',
            'products_text': '2-phenyl-4-pentenoic acid',
            'reagents_text': 'LDA, TMSCl; Δ',
            'conditions_text': 'THF, -78°C→rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase33_shallow_top10_v1 sprint for Claisen-Ireland Rearrangement. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCOC(=O)c1ccccc1', 'allyl benzoate', 'reactants_text', 1),
                ('product', 'OC(=O)C(c1ccccc1)CC=C', '2-phenyl-4-pentenoic acid', 'products_text', 1),
            ],
        },
        # === Arndt-Eistert Homologation / Synthesis ===
        {
            'family': 'Arndt-Eistert Homologation / Synthesis',
            'extract_kind': 'canonical_overview',
            'transformation_text': 'Arndt-Eistert: acid → acid chloride → diazoketone → Wolff rearrangement (Ag or hν) → ketene → trap with H2O gives homologated acid.',
            'reactants_text': '4-methoxybenzoic acid',
            'products_text': '4-methoxyphenylacetic acid (homologated by 1 C)',
            'reagents_text': '(1) SOCl2; (2) CH2N2; (3) Ag2O, H2O',
            'conditions_text': 'dioxane, Δ',
            'notes_text': 'Manual curated canonical-overview seed (variant A) added during phase33_shallow_top10_v1 sprint for Arndt-Eistert Homologation / Synthesis. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(=O)c1ccc(OC)cc1', '4-methoxybenzoic acid', 'reactants_text', 1),
                ('product', 'OC(=O)Cc1ccc(OC)cc1', '4-methoxyphenylacetic acid (homologated by 1 C)', 'products_text', 1),
            ],
        },
        {
            'family': 'Arndt-Eistert Homologation / Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Arndt-Eistert on cyclopentanecarboxylic acid.',
            'reactants_text': 'cyclopentanecarboxylic acid',
            'products_text': 'cyclopentylacetic acid',
            'reagents_text': 'SOCl2; CH2N2; Ag2O/H2O',
            'conditions_text': 'dioxane, 60°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase33_shallow_top10_v1 sprint for Arndt-Eistert Homologation / Synthesis. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(=O)C1CCCC1', 'cyclopentanecarboxylic acid', 'reactants_text', 1),
                ('product', 'OC(=O)CC1CCCC1', 'cyclopentylacetic acid', 'products_text', 1),
            ],
        },
        {
            'family': 'Arndt-Eistert Homologation / Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Arndt-Eistert homologation of butanoic acid.',
            'reactants_text': 'butanoic acid',
            'products_text': 'pentanoic acid (1 carbon longer)',
            'reagents_text': 'SOCl2; CH2N2; Ag2O, H2O',
            'conditions_text': 'dioxane, 80°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase33_shallow_top10_v1 sprint for Arndt-Eistert Homologation / Synthesis. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(=O)CCC', 'butanoic acid', 'reactants_text', 1),
                ('product', 'OC(=O)CCCC', 'pentanoic acid (1 carbon longer)', 'products_text', 1),
            ],
        },
        # === Alkyne Metathesis ===
        {
            'family': 'Alkyne Metathesis',
            'extract_kind': 'canonical_overview',
            'transformation_text': 'Alkyne metathesis: two internal alkynes exchange carbyne halves via Mo/W alkylidyne-mediated [2+2]/[2+2]-retro. Used for macrocycles and polymers.',
            'reactants_text': '2-butyne',
            'products_text': '2-pentyne (cross-metathesis example product)',
            'reagents_text': "Mo(≡CAr)[N(tBu)(Ar')]3 catalyst",
            'conditions_text': 'PhMe, 80°C',
            'notes_text': 'Manual curated canonical-overview seed (variant A) added during phase33_shallow_top10_v1 sprint for Alkyne Metathesis. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC#CC', '2-butyne', 'reactants_text', 1),
                ('product', 'CC#CCC', '2-pentyne (cross-metathesis example product)', 'products_text', 1),
            ],
        },
        {
            'family': 'Alkyne Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Alkyne metathesis homodimer forming symmetric 4-octyne.',
            'reactants_text': '3-heptyne',
            'products_text': '4-octyne (metathesis dimer)',
            'reagents_text': 'Schrock alkylidyne Mo catalyst',
            'conditions_text': 'PhMe, 80°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase33_shallow_top10_v1 sprint for Alkyne Metathesis. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCC#CCCC', '3-heptyne', 'reactants_text', 1),
                ('product', 'CCCC#CCCC', '4-octyne (metathesis dimer)', 'products_text', 1),
            ],
        },
        {
            'family': 'Alkyne Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Alkyne cross-metathesis of 2-hexyne giving 5-decyne + 2-butyne.',
            'reactants_text': '2-hexyne',
            'products_text': '5-decyne (cross-metathesis product)',
            'reagents_text': 'Mo alkylidyne',
            'conditions_text': 'PhMe, 80°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase33_shallow_top10_v1 sprint for Alkyne Metathesis. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC#CCCC', '2-hexyne', 'reactants_text', 1),
                ('product', 'CCCCC#CCCCC', '5-decyne (cross-metathesis product)', 'products_text', 1),
            ],
        },
        # === Beckmann Rearrangement ===
        {
            'family': 'Beckmann Rearrangement',
            'extract_kind': 'canonical_overview',
            'transformation_text': 'Beckmann rearrangement: ketoxime → acid-catalyzed ionization of N-OH → anti-migration of R to N → tautomerize to amide.',
            'reactants_text': '4-chloroacetophenone oxime',
            'products_text': 'N-methyl-4-chlorobenzamide',
            'reagents_text': 'P2O5',
            'conditions_text': 'xylene, reflux',
            'notes_text': 'Manual curated canonical-overview seed (variant A) added during phase33_shallow_top10_v1 sprint for Beckmann Rearrangement. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=NC(=C(C)c1ccc(Cl)cc1)', '4-chloroacetophenone oxime', 'reactants_text', 1),
                ('product', 'O=C(NC)c1ccc(Cl)cc1', 'N-methyl-4-chlorobenzamide', 'products_text', 1),
            ],
        },
        {
            'family': 'Beckmann Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Beckmann of aliphatic ketoxime.',
            'reactants_text': 'butanone oxime (anti-ethyl)',
            'products_text': 'N-ethyl-acetamide',
            'reagents_text': 'H2SO4',
            'conditions_text': 'AcOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase33_shallow_top10_v1 sprint for Beckmann Rearrangement. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=NC(=C(C)CC)', 'butanone oxime (anti-ethyl)', 'reactants_text', 1),
                ('product', 'O=C(NCC)C', 'N-ethyl-acetamide', 'products_text', 1),
            ],
        },
        {
            'family': 'Beckmann Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Beckmann of substituted cyclohexanone oxime.',
            'reactants_text': '3-methylcyclohexanone oxime',
            'products_text': 'N-H-ε-caprolactam derivative (7-methyl)',
            'reagents_text': 'PCl5',
            'conditions_text': 'Et2O, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase33_shallow_top10_v1 sprint for Beckmann Rearrangement. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=NC(=C1CC(CCC1)C)', '3-methylcyclohexanone oxime', 'reactants_text', 1),
                ('product', 'O=C1NCCCC1C', 'N-H-ε-caprolactam derivative (7-methyl)', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Aza-Cope Rearrangement ===
        {
            'family': 'Aza-Cope Rearrangement',
            'extract_kind': 'canonical_overview',
            'transformation_text': 'Cationic aza-Cope: [3,3]-sigmatropic rearrangement of N-allyl iminium (bis-allyl-cation-like) gives homoallyl enamine/imine; often paired with Mannich in Overman chemistry.',
            'reactants_text': 'N-allyl-propylideneiminium',
            'products_text': 'hex-4-enyl-amine (aza-Cope [3,3])',
            'reagents_text': 'H+ (cat.)',
            'conditions_text': 'CH3CN, 60°C',
            'notes_text': 'Manual curated canonical-overview seed (variant A) added during phase33_shallow_top10_v1 sprint for Aza-Cope Rearrangement. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CC[NH+]=CCC', 'N-allyl-propylideneiminium', 'reactants_text', 1),
                ('product', 'CCCC=CCN', 'hex-4-enyl-amine (aza-Cope [3,3])', 'products_text', 1),
            ],
        },
        {
            'family': 'Aza-Cope Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Neutral [3,3] aza-Cope of bis-allyl amine.',
            'reactants_text': 'N-allyl-N-methyl-N-allylamine (neutral bis-allyl amine)',
            'products_text': '(aza-Cope-shifted amine)',
            'reagents_text': 'heat',
            'conditions_text': 'PhMe, 140°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase33_shallow_top10_v1 sprint for Aza-Cope Rearrangement. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCN(C)CC=C', 'N-allyl-N-methyl-N-allylamine (neutral bis-allyl amine)', 'reactants_text', 1),
                ('product', 'CC(CC=C)CN(C)CC=C', '(aza-Cope-shifted amine)', 'products_text', 1),
            ],
        },
        {
            'family': 'Aza-Cope Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Cationic aza-Cope with aryl iminium.',
            'reactants_text': 'N-allyl-benzylideneiminium',
            'products_text': '4-amino-4-phenyl-1-butene (aza-Cope product)',
            'reagents_text': 'TfOH',
            'conditions_text': 'CH2Cl2, 40°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase33_shallow_top10_v1 sprint for Aza-Cope Rearrangement. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CC[NH+]=Cc1ccccc1', 'N-allyl-benzylideneiminium', 'reactants_text', 1),
                ('product', 'Nc1ccccc1(CCC=C)', '4-amino-4-phenyl-1-butene (aza-Cope product)', 'products_text', 1),
            ],
        },
        # === Brook Rearrangement ===
        {
            'family': 'Brook Rearrangement',
            'extract_kind': 'canonical_overview',
            'transformation_text': 'Brook [1,n] rearrangement: α-silyl carbanion/alkoxide migrates Si from C to O (thermodynamic driven by stronger Si-O bond). Widely used in anion relay.',
            'reactants_text': '1-(trimethylsilyl)-2-methyl-1-propanol (α-silyl alcohol)',
            'products_text': '2-methyl-1-propyloxy-trimethylsilane (C→O Si-shift)',
            'reagents_text': 'KH (base, catalytic)',
            'conditions_text': 'THF, rt',
            'notes_text': 'Manual curated canonical-overview seed (variant A) added during phase33_shallow_top10_v1 sprint for Brook Rearrangement. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC([Si](C)(C)C)C(C)C', '1-(trimethylsilyl)-2-methyl-1-propanol (α-silyl alcohol)', 'reactants_text', 1),
                ('product', 'O([Si](C)(C)C)C(C(C)C)', '2-methyl-1-propyloxy-trimethylsilane (C→O Si-shift)', 'products_text', 1),
            ],
        },
        {
            'family': 'Brook Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Brook rearrangement of allylic α-silyl alcohol.',
            'reactants_text': '1-(trimethylsilyl)-3-buten-1-ol',
            'products_text': '3-buten-1-yloxy-trimethylsilane',
            'reagents_text': 'NaH',
            'conditions_text': 'THF, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase33_shallow_top10_v1 sprint for Brook Rearrangement. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(CC=C)[Si](C)(C)C', '1-(trimethylsilyl)-3-buten-1-ol', 'reactants_text', 1),
                ('product', 'C=CCCO[Si](C)(C)C', '3-buten-1-yloxy-trimethylsilane', 'products_text', 1),
            ],
        },
        {
            'family': 'Brook Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Brook rearrangement of benzylic α-silyl alcohol.',
            'reactants_text': '1-(trimethylsilyl)-2-phenyl-1-ethanol',
            'products_text': '2-phenyl-ethyl-trimethylsilyl ether',
            'reagents_text': 'KH',
            'conditions_text': 'THF',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase33_shallow_top10_v1 sprint for Brook Rearrangement. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(Cc1ccccc1)[Si](C)(C)C', '1-(trimethylsilyl)-2-phenyl-1-ethanol', 'reactants_text', 1),
                ('product', 'c1ccccc1CCO[Si](C)(C)C', '2-phenyl-ethyl-trimethylsilyl ether', 'products_text', 1),
            ],
        },
        # === Aza-Wittig Reaction ===
        {
            'family': 'Aza-Wittig Reaction',
            'extract_kind': 'application_example',
            'transformation_text': "Aza-Wittig: iminophosphorane (R'N=PR3) + carbonyl gives imine + R3P=O (analog of Wittig).",
            'reactants_text': 'iminophosphorane + butanal',
            'products_text': 'N-methyl-1-butanimine (aza-Wittig)',
            'reagents_text': 'iminophosphorane, aldehyde',
            'conditions_text': 'PhMe, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase33_shallow_top10_v1 sprint for Aza-Wittig Reaction. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'N=[P+](c1ccccc1)(c2ccccc2)c3ccccc3.O=CCCC', 'iminophosphorane + butanal', 'reactants_text', 1),
                ('product', 'CCCC=NC', 'N-methyl-1-butanimine (aza-Wittig)', 'products_text', 1),
            ],
        },
        {
            'family': 'Aza-Wittig Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Aza-Wittig of anisaldehyde with iminophosphorane.',
            'reactants_text': '4-methoxybenzaldehyde + N-phenyl iminophosphorane',
            'products_text': 'N-(4-methoxy-benzylidene)-aniline',
            'reagents_text': 'PhN=PPh3',
            'conditions_text': 'PhMe, 80°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase33_shallow_top10_v1 sprint for Aza-Wittig Reaction. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccc(OC)cc1', '4-methoxybenzaldehyde + N-phenyl iminophosphorane', 'reactants_text', 1),
                ('product', 'C(=Nc1ccccc1)c2ccc(OC)cc2', 'N-(4-methoxy-benzylidene)-aniline', 'products_text', 1),
            ],
        },
        {
            'family': 'Aza-Wittig Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Aza-Wittig of aliphatic aldehyde with alkyl iminophosphorane.',
            'reactants_text': 'propanal + cyclohexylmethyl-iminophosphorane',
            'products_text': 'N-cyclohexylmethyl-propan-1-imine',
            'reagents_text': "R'N=PPh3",
            'conditions_text': 'PhMe',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase33_shallow_top10_v1 sprint for Aza-Wittig Reaction. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CCC', 'propanal + cyclohexylmethyl-iminophosphorane', 'reactants_text', 1),
                ('product', 'C(=NCC1CCCCC1)CC', 'N-cyclohexylmethyl-propan-1-imine', 'products_text', 1),
            ],
        },
        # === Clemmensen Reduction ===
        {
            'family': 'Clemmensen Reduction',
            'extract_kind': 'application_example',
            'transformation_text': 'Clemmensen: aryl ketone + Zn(Hg)/HCl (conc.) reduces C=O to CH2 (complementary to Wolff-Kishner for acid-tolerant systems).',
            'reactants_text': 'propiophenone',
            'products_text': 'propylbenzene',
            'reagents_text': 'Zn(Hg), conc. HCl',
            'conditions_text': 'PhMe/H2O, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase33_shallow_top10_v1 sprint for Clemmensen Reduction. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(c1ccccc1)CC', 'propiophenone', 'reactants_text', 1),
                ('product', 'CCCc1ccccc1', 'propylbenzene', 'products_text', 1),
            ],
        },
        {
            'family': 'Clemmensen Reduction',
            'extract_kind': 'application_example',
            'transformation_text': 'Clemmensen of diaryl ketone.',
            'reactants_text': 'benzophenone',
            'products_text': 'diphenylmethane',
            'reagents_text': 'Zn(Hg), HCl',
            'conditions_text': 'reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase33_shallow_top10_v1 sprint for Clemmensen Reduction. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(c1ccccc1)c2ccccc2', 'benzophenone', 'reactants_text', 1),
                ('product', 'C(c1ccccc1)c2ccccc2', 'diphenylmethane', 'products_text', 1),
            ],
        },
        {
            'family': 'Clemmensen Reduction',
            'extract_kind': 'application_example',
            'transformation_text': 'Clemmensen reduction of cyclic aryl ketone (1-tetralone → tetralin).',
            'reactants_text': '1-tetralone',
            'products_text': 'tetralin',
            'reagents_text': 'Zn(Hg), HCl',
            'conditions_text': 'reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase33_shallow_top10_v1 sprint for Clemmensen Reduction. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCc2ccccc12', '1-tetralone', 'reactants_text', 1),
                ('product', 'C1CCCc2ccccc12', 'tetralin', 'products_text', 1),
            ],
        },
        # === Bischler-Napieralski Isoquinoline Synthesis ===
        {
            'family': 'Bischler-Napieralski Isoquinoline Synthesis',
            'extract_kind': 'canonical_overview',
            'transformation_text': 'Bischler-Napieralski: N-β-arylethyl-amide + dehydrating Lewis acid cyclizes via intramolecular acylation and dehydration to 1-substituted 3,4-dihydroisoquinoline.',
            'reactants_text': 'N-(2-phenylethyl)-propanamide',
            'products_text': '1-ethyl-3,4-dihydro-isoquinoline',
            'reagents_text': 'POCl3',
            'conditions_text': 'PhMe, reflux',
            'notes_text': 'Manual curated canonical-overview seed (variant A) added during phase33_shallow_top10_v1 sprint for Bischler-Napieralski Isoquinoline Synthesis. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(NCCc1ccccc1)CC', 'N-(2-phenylethyl)-propanamide', 'reactants_text', 1),
                ('product', 'CCC1=NCCc2ccccc21', '1-ethyl-3,4-dihydro-isoquinoline', 'products_text', 1),
            ],
        },
        {
            'family': 'Bischler-Napieralski Isoquinoline Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Bischler-Napieralski on halogenated β-arylethylamide.',
            'reactants_text': 'N-[2-(4-chlorophenyl)ethyl]-acetamide',
            'products_text': '1-methyl-7-chloro-3,4-dihydro-isoquinoline',
            'reagents_text': 'POCl3, ZnCl2',
            'conditions_text': 'PhMe, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase33_shallow_top10_v1 sprint for Bischler-Napieralski Isoquinoline Synthesis. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(NCCc1ccc(Cl)cc1)C', 'N-[2-(4-chlorophenyl)ethyl]-acetamide', 'reactants_text', 1),
                ('product', 'CC1=NCCc2ccc(Cl)cc21', '1-methyl-7-chloro-3,4-dihydro-isoquinoline', 'products_text', 1),
            ],
        },
        {
            'family': 'Bischler-Napieralski Isoquinoline Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Bischler-Napieralski with ortho-methyl aroyl substrate.',
            'reactants_text': 'N-(3,4-dimethoxy-phenethyl)-2-methylbenzamide',
            'products_text': '1-(2-methylphenyl)-6,7-dimethoxy-3,4-dihydro-isoquinoline',
            'reagents_text': 'POCl3',
            'conditions_text': 'xylene, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase33_shallow_top10_v1 sprint for Bischler-Napieralski Isoquinoline Synthesis. [phase33_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(NCCc1ccc(OC)c(OC)c1)c2ccccc2C', 'N-(3,4-dimethoxy-phenethyl)-2-methylbenzamide', 'reactants_text', 1),
                ('product', 'Cc1ccccc1C2=NCCc3cc(OC)c(OC)cc32', '1-(2-methylphenyl)-6,7-dimethoxy-3,4-dihydro-isoquinoline', 'products_text', 1),
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
