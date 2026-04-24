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

TAG = 'phase31_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Corey-Kim Oxidation',
        'Darzens Glycidic Ester Condensation',
        'Friedel-Crafts Acylation',
        'Friedel-Crafts Alkylation',
        'Alkyne Metathesis',
    ],
    'b': [
        'Beckmann Rearrangement',
        'Bergman Cycloaromatization Reaction',
        'Biginelli Reaction',
        'Brown Hydroboration Reaction',
        'Aza-Cope Rearrangement',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Corey-Kim Oxidation ===
        {
            'family': 'Corey-Kim Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Corey-Kim: NCS + Me2S at low T generates chlorosulfonium that oxidizes primary alcohol to aldehyde (Et3N workup).',
            'reactants_text': '1-hexanol',
            'products_text': 'hexanal',
            'reagents_text': 'NCS, Me2S, Et3N',
            'conditions_text': 'CH2Cl2, -25°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase31_shallow_top10_v1 sprint for Corey-Kim Oxidation. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCCCCCC', '1-hexanol', 'reactants_text', 1),
                ('product', 'O=CCCCCC', 'hexanal', 'products_text', 1),
            ],
        },
        {
            'family': 'Corey-Kim Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Corey-Kim oxidation of secondary benzylic alcohol.',
            'reactants_text': '1-phenyl-ethanol',
            'products_text': 'acetophenone',
            'reagents_text': 'NCS, Me2S, Et3N',
            'conditions_text': 'CH2Cl2, -40°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase31_shallow_top10_v1 sprint for Corey-Kim Oxidation. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)c1ccccc1', '1-phenyl-ethanol', 'reactants_text', 1),
                ('product', 'O=C(C)c1ccccc1', 'acetophenone', 'products_text', 1),
            ],
        },
        {
            'family': 'Corey-Kim Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Corey-Kim oxidation of primary alicyclic alcohol to aldehyde.',
            'reactants_text': 'cyclohexylmethanol',
            'products_text': 'cyclohexanecarboxaldehyde',
            'reagents_text': 'NCS, Me2S, Et3N',
            'conditions_text': 'CH2Cl2, -25°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase31_shallow_top10_v1 sprint for Corey-Kim Oxidation. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCC1CCCCC1', 'cyclohexylmethanol', 'reactants_text', 1),
                ('product', 'O=CC1CCCCC1', 'cyclohexanecarboxaldehyde', 'products_text', 1),
            ],
        },
        # === Darzens Glycidic Ester Condensation ===
        {
            'family': 'Darzens Glycidic Ester Condensation',
            'extract_kind': 'application_example',
            'transformation_text': 'Darzens: aldehyde/ketone + α-halo ester + base (NaOEt) gives α,β-epoxy ester (glycidic ester) via aldol-then-SN2 cyclization.',
            'reactants_text': 'benzaldehyde + ethyl chloroacetate',
            'products_text': 'ethyl trans-3-phenyl-glycidate',
            'reagents_text': 'NaOEt',
            'conditions_text': 'EtOH, 0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase31_shallow_top10_v1 sprint for Darzens Glycidic Ester Condensation. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1.ClCC(=O)OCC', 'benzaldehyde + ethyl chloroacetate', 'reactants_text', 1),
                ('product', 'O=C(OCC)C1OC1c1ccccc1', 'ethyl trans-3-phenyl-glycidate', 'products_text', 1),
            ],
        },
        {
            'family': 'Darzens Glycidic Ester Condensation',
            'extract_kind': 'application_example',
            'transformation_text': 'Darzens on dialkyl ketone.',
            'reactants_text': 'acetone + ethyl chloroacetate',
            'products_text': 'ethyl 2,2-dimethyl-glycidate',
            'reagents_text': 'NaOEt',
            'conditions_text': 'EtOH, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase31_shallow_top10_v1 sprint for Darzens Glycidic Ester Condensation. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)C.ClCC(=O)OCC', 'acetone + ethyl chloroacetate', 'reactants_text', 1),
                ('product', 'O=C(OCC)C1OC1(C)C', 'ethyl 2,2-dimethyl-glycidate', 'products_text', 1),
            ],
        },
        {
            'family': 'Darzens Glycidic Ester Condensation',
            'extract_kind': 'application_example',
            'transformation_text': 'Darzens with electron-poor aromatic aldehyde.',
            'reactants_text': '4-chlorobenzaldehyde + ethyl chloroacetate',
            'products_text': 'ethyl trans-3-(4-chlorophenyl)-glycidate',
            'reagents_text': 'KOtBu',
            'conditions_text': 'THF, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase31_shallow_top10_v1 sprint for Darzens Glycidic Ester Condensation. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccc(Cl)cc1.ClCC(=O)OCC', '4-chlorobenzaldehyde + ethyl chloroacetate', 'reactants_text', 1),
                ('product', 'O=C(OCC)C1OC1c1ccc(Cl)cc1', 'ethyl trans-3-(4-chlorophenyl)-glycidate', 'products_text', 1),
            ],
        },
        # === Friedel-Crafts Acylation ===
        {
            'family': 'Friedel-Crafts Acylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Friedel-Crafts acylation: arene + acyl chloride + AlCl3 gives aryl ketone (no polyacylation since product deactivated).',
            'reactants_text': 'benzene + acetyl chloride',
            'products_text': 'acetophenone',
            'reagents_text': 'AlCl3',
            'conditions_text': 'CS2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase31_shallow_top10_v1 sprint for Friedel-Crafts Acylation. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccccc1.ClC(=O)C', 'benzene + acetyl chloride', 'reactants_text', 1),
                ('product', 'O=C(C)c1ccccc1', 'acetophenone', 'products_text', 1),
            ],
        },
        {
            'family': 'Friedel-Crafts Acylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Friedel-Crafts acylation of toluene (para-selective).',
            'reactants_text': 'toluene + propanoyl chloride',
            'products_text': '4-methyl-propiophenone',
            'reagents_text': 'AlCl3',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase31_shallow_top10_v1 sprint for Friedel-Crafts Acylation. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccc(C)cc1.ClC(=O)CC', 'toluene + propanoyl chloride', 'reactants_text', 1),
                ('product', 'O=C(CC)c1ccc(C)cc1', '4-methyl-propiophenone', 'products_text', 1),
            ],
        },
        {
            'family': 'Friedel-Crafts Acylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Friedel-Crafts acylation of anisole (para-selective).',
            'reactants_text': 'anisole + benzoyl chloride',
            'products_text': '4-methoxy-benzophenone',
            'reagents_text': 'AlCl3',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase31_shallow_top10_v1 sprint for Friedel-Crafts Acylation. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccc(OC)cc1.ClC(=O)c2ccccc2', 'anisole + benzoyl chloride', 'reactants_text', 1),
                ('product', 'O=C(c1ccc(OC)cc1)c2ccccc2', '4-methoxy-benzophenone', 'products_text', 1),
            ],
        },
        # === Friedel-Crafts Alkylation ===
        {
            'family': 'Friedel-Crafts Alkylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Friedel-Crafts alkylation: arene + alkyl halide + AlCl3 gives alkylated arene; polyalkylation and rearrangement are common limitations.',
            'reactants_text': 'benzene + tert-butyl chloride',
            'products_text': 'tert-butylbenzene',
            'reagents_text': 'AlCl3',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase31_shallow_top10_v1 sprint for Friedel-Crafts Alkylation. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccccc1.ClC(C)(C)C', 'benzene + tert-butyl chloride', 'reactants_text', 1),
                ('product', 'C(c1ccccc1)(C)(C)C', 'tert-butylbenzene', 'products_text', 1),
            ],
        },
        {
            'family': 'Friedel-Crafts Alkylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Friedel-Crafts alkylation (1° halide may rearrange).',
            'reactants_text': 'toluene + 1-chloropropane',
            'products_text': '4-methyl-propylbenzene (major) / cumene-like rearranged product',
            'reagents_text': 'AlCl3',
            'conditions_text': 'CH2Cl2, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase31_shallow_top10_v1 sprint for Friedel-Crafts Alkylation. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccc(C)cc1.ClCCC', 'toluene + 1-chloropropane', 'reactants_text', 1),
                ('product', 'CCCc1ccc(C)cc1', '4-methyl-propylbenzene (major) / cumene-like rearranged product', 'products_text', 1),
            ],
        },
        {
            'family': 'Friedel-Crafts Alkylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Friedel-Crafts benzylation of electron-rich arene.',
            'reactants_text': 'anisole + benzyl chloride',
            'products_text': '4-methoxy-diphenylmethane',
            'reagents_text': 'FeCl3',
            'conditions_text': 'CH2Cl2, rt',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase31_shallow_top10_v1 sprint for Friedel-Crafts Alkylation. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'c1ccc(OC)cc1.ClCc2ccccc2', 'anisole + benzyl chloride', 'reactants_text', 1),
                ('product', 'COc1ccc(Cc2ccccc2)cc1', '4-methoxy-diphenylmethane', 'products_text', 1),
            ],
        },
        # === Alkyne Metathesis ===
        {
            'family': 'Alkyne Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Alkyne metathesis: two internal alkynes + Mo/W alkylidyne catalyst exchange carbyne halves (here 2 × 2-pentyne → 3-hexyne + 2-butyne).',
            'reactants_text': '2-pentyne',
            'products_text': '3-hexyne (via symmetric alkyne metathesis)',
            'reagents_text': 'Mo(≡CtBu)(OtBu)3 (Schrock catalyst)',
            'conditions_text': 'PhMe, 80°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase31_shallow_top10_v1 sprint for Alkyne Metathesis. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC#CCC', '2-pentyne', 'reactants_text', 1),
                ('product', 'CCC#CCC', '3-hexyne (via symmetric alkyne metathesis)', 'products_text', 1),
            ],
        },
        {
            'family': 'Alkyne Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Alkyne metathesis homodimer of aryl alkyne.',
            'reactants_text': '1-phenyl-1-propyne',
            'products_text': 'diphenylacetylene (tolan)',
            'reagents_text': 'Mo alkylidyne/silanol support',
            'conditions_text': 'PhMe, 100°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase31_shallow_top10_v1 sprint for Alkyne Metathesis. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC#Cc1ccccc1', '1-phenyl-1-propyne', 'reactants_text', 1),
                ('product', 'c1ccc(C#Cc2ccccc2)cc1', 'diphenylacetylene (tolan)', 'products_text', 1),
            ],
        },
        {
            'family': 'Alkyne Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Alkyne cross-metathesis of unsymmetric internal alkyne.',
            'reactants_text': '3-heptyne',
            'products_text': '3-hexyne (alkyne exchange)',
            'reagents_text': 'W alkylidyne catalyst',
            'conditions_text': 'PhMe, 80°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase31_shallow_top10_v1 sprint for Alkyne Metathesis. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCC#CCCC', '3-heptyne', 'reactants_text', 1),
                ('product', 'CCC#CCC', '3-hexyne (alkyne exchange)', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Beckmann Rearrangement ===
        {
            'family': 'Beckmann Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Beckmann: ketoxime + acid rearranges to amide with anti-group migrating; industrially the nylon-6 monomer synthesis.',
            'reactants_text': 'cyclohexanone oxime (anti-oxime)',
            'products_text': 'ε-caprolactam',
            'reagents_text': 'conc. H2SO4',
            'conditions_text': '100°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase31_shallow_top10_v1 sprint for Beckmann Rearrangement. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=N/C(=C1CCCCC1)', 'cyclohexanone oxime (anti-oxime)', 'reactants_text', 1),
                ('product', 'O=C1NCCCCC1', 'ε-caprolactam', 'products_text', 1),
            ],
        },
        {
            'family': 'Beckmann Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Beckmann of aryl methyl ketoxime.',
            'reactants_text': 'acetophenone oxime',
            'products_text': 'N-methyl-benzamide (anti-phenyl migrates)',
            'reagents_text': 'PCl5',
            'conditions_text': 'Et2O, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase31_shallow_top10_v1 sprint for Beckmann Rearrangement. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=NC(=C(C)c1ccccc1)', 'acetophenone oxime', 'reactants_text', 1),
                ('product', 'O=C(NC)c1ccccc1', 'N-methyl-benzamide (anti-phenyl migrates)', 'products_text', 1),
            ],
        },
        {
            'family': 'Beckmann Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Beckmann rearrangement of cyclopentanone oxime to 6-membered lactam.',
            'reactants_text': 'cyclopentanone oxime',
            'products_text': 'δ-valerolactam',
            'reagents_text': 'H2SO4',
            'conditions_text': '100°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase31_shallow_top10_v1 sprint for Beckmann Rearrangement. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=NC(=C1CCCC1)', 'cyclopentanone oxime', 'reactants_text', 1),
                ('product', 'O=C1NCCCC1', 'δ-valerolactam', 'products_text', 1),
            ],
        },
        # === Bergman Cycloaromatization Reaction ===
        {
            'family': 'Bergman Cycloaromatization Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Bergman cycloaromatization: cis-enediyne at ~200°C cyclizes to 1,4-benzenediyl diradical that abstracts H to give benzene.',
            'reactants_text': '(Z)-hex-3-ene-1,5-diyne (enediyne)',
            'products_text': 'benzene (via p-benzyne diradical; 1,4-H abstraction gives benzene)',
            'reagents_text': 'thermal',
            'conditions_text': '200°C, neat',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase31_shallow_top10_v1 sprint for Bergman Cycloaromatization Reaction. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C(#CC=CC#C)', '(Z)-hex-3-ene-1,5-diyne (enediyne)', 'reactants_text', 1),
                ('product', 'c1ccccc1', 'benzene (via p-benzyne diradical; 1,4-H abstraction gives benzene)', 'products_text', 1),
            ],
        },
        {
            'family': 'Bergman Cycloaromatization Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Phenyl-substituted enediyne cyclization.',
            'reactants_text': '1-phenyl-(Z)-enediyne',
            'products_text': 'biphenyl (via p-benzyne + H abstraction)',
            'reagents_text': 'thermal',
            'conditions_text': '180°C, 1,4-CHD (H source)',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase31_shallow_top10_v1 sprint for Bergman Cycloaromatization Reaction. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C(#CC=CC#C)c1ccccc1', '1-phenyl-(Z)-enediyne', 'reactants_text', 1),
                ('product', 'c1ccc(-c2ccccc2)cc1', 'biphenyl (via p-benzyne + H abstraction)', 'products_text', 1),
            ],
        },
        {
            'family': 'Bergman Cycloaromatization Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Bergman of methyl-capped enediyne giving dimethylbenzene.',
            'reactants_text': 'cis-deca-3-ene-2,8-diyne',
            'products_text': 'ortho-xylene (after p-benzyne 1,4-HAA)',
            'reagents_text': 'thermal',
            'conditions_text': '160°C, 1,4-cyclohexadiene',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase31_shallow_top10_v1 sprint for Bergman Cycloaromatization Reaction. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC#CC=CC#CC', 'cis-deca-3-ene-2,8-diyne', 'reactants_text', 1),
                ('product', 'Cc1ccccc1C', 'ortho-xylene (after p-benzyne 1,4-HAA)', 'products_text', 1),
            ],
        },
        # === Biginelli Reaction ===
        {
            'family': 'Biginelli Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Biginelli: aldehyde + β-ketoester + urea/thiourea under acid gives dihydropyrimidinone (DHPM).',
            'reactants_text': 'benzaldehyde + ethyl acetoacetate + urea',
            'products_text': 'ethyl 4-phenyl-6-methyl-2-oxo-1,2,3,4-tetrahydropyrimidine-5-carboxylate',
            'reagents_text': 'HCl (cat.)',
            'conditions_text': 'EtOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase31_shallow_top10_v1 sprint for Biginelli Reaction. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1.CC(=O)CC(=O)OCC.NC(=O)N', 'benzaldehyde + ethyl acetoacetate + urea', 'reactants_text', 1),
                ('product', 'O=C1NC(C)=C(C(=O)OCC)C(c2ccccc2)N1', 'ethyl 4-phenyl-6-methyl-2-oxo-1,2,3,4-tetrahydropyrimidine-5-carboxylate', 'products_text', 1),
            ],
        },
        {
            'family': 'Biginelli Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Biginelli with aliphatic aldehyde.',
            'reactants_text': 'butanal + ethyl acetoacetate + urea',
            'products_text': 'ethyl 4-propyl-6-methyl-2-oxo-1,2,3,4-tetrahydropyrimidine-5-carboxylate',
            'reagents_text': 'Yb(OTf)3 (cat.)',
            'conditions_text': 'EtOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase31_shallow_top10_v1 sprint for Biginelli Reaction. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CCCC.CC(=O)CC(=O)OCC.NC(=O)N', 'butanal + ethyl acetoacetate + urea', 'reactants_text', 1),
                ('product', 'O=C1NC(C)=C(C(=O)OCC)C(CCC)N1', 'ethyl 4-propyl-6-methyl-2-oxo-1,2,3,4-tetrahydropyrimidine-5-carboxylate', 'products_text', 1),
            ],
        },
        {
            'family': 'Biginelli Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Biginelli with thiourea and electron-rich aldehyde.',
            'reactants_text': 'p-anisaldehyde + ethyl acetoacetate + thiourea',
            'products_text': 'ethyl 4-(4-methoxyphenyl)-6-methyl-2-thioxo-1,2,3,4-tetrahydropyrimidine-5-carboxylate',
            'reagents_text': 'HCl (cat.)',
            'conditions_text': 'EtOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase31_shallow_top10_v1 sprint for Biginelli Reaction. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccc(OC)cc1.CC(=O)CC(=O)OCC.NC(=S)N', 'p-anisaldehyde + ethyl acetoacetate + thiourea', 'reactants_text', 1),
                ('product', 'S=C1NC(C)=C(C(=O)OCC)C(c2ccc(OC)cc2)N1', 'ethyl 4-(4-methoxyphenyl)-6-methyl-2-thioxo-1,2,3,4-tetrahydropyrimidine-5-carboxylate', 'products_text', 1),
            ],
        },
        # === Brown Hydroboration Reaction ===
        {
            'family': 'Brown Hydroboration Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Brown hydroboration: terminal alkene + BH3 (or 9-BBN) + H2O2/NaOH gives anti-Markovnikov primary alcohol.',
            'reactants_text': '1-hexene',
            'products_text': '1-hexanol (anti-Markovnikov)',
            'reagents_text': 'BH3·THF; then H2O2, NaOH',
            'conditions_text': 'THF, 0°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase31_shallow_top10_v1 sprint for Brown Hydroboration Reaction. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCCCC', '1-hexene', 'reactants_text', 1),
                ('product', 'OCCCCCC', '1-hexanol (anti-Markovnikov)', 'products_text', 1),
            ],
        },
        {
            'family': 'Brown Hydroboration Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Brown hydroboration of styrene (anti-Markovnikov).',
            'reactants_text': 'styrene',
            'products_text': '2-phenyl-ethanol (anti-Markovnikov)',
            'reagents_text': '9-BBN; then H2O2, NaOH',
            'conditions_text': 'THF, 0°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase31_shallow_top10_v1 sprint for Brown Hydroboration Reaction. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=Cc1ccccc1', 'styrene', 'reactants_text', 1),
                ('product', 'OCCc1ccccc1', '2-phenyl-ethanol (anti-Markovnikov)', 'products_text', 1),
            ],
        },
        {
            'family': 'Brown Hydroboration Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Brown hydroboration-oxidation of internal alkene (both regioisomers similar).',
            'reactants_text': '2-pentene',
            'products_text': '2-pentanol',
            'reagents_text': 'BH3·SMe2; H2O2/NaOH',
            'conditions_text': 'THF, 0°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase31_shallow_top10_v1 sprint for Brown Hydroboration Reaction. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC=CCC', '2-pentene', 'reactants_text', 1),
                ('product', 'OC(C)CCC', '2-pentanol', 'products_text', 1),
            ],
        },
        # === Aza-Cope Rearrangement ===
        {
            'family': 'Aza-Cope Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': '[3,3]-aza-Cope: iminium + allyl system undergoes [3,3]-sigmatropic rearrangement (cationic aza-Cope; often paired with Mannich).',
            'reactants_text': 'N-allyl-N-methyl-N-allylammonium (bis-allyl ammonium)',
            'products_text': '6-(dimethylamino)-1,5-hexadien-3-ene rearranged product',
            'reagents_text': 'heat',
            'conditions_text': 'CH2Cl2, 60°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase31_shallow_top10_v1 sprint for Aza-Cope Rearrangement. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CC[N+](C)(C)CC=C', 'N-allyl-N-methyl-N-allylammonium (bis-allyl ammonium)', 'reactants_text', 1),
                ('product', 'C=CCC=CC(C)(C)N', '6-(dimethylamino)-1,5-hexadien-3-ene rearranged product', 'products_text', 1),
            ],
        },
        {
            'family': 'Aza-Cope Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Cationic aza-Cope [3,3] sigmatropic rearrangement.',
            'reactants_text': 'N-allyl-isopropylideneiminium',
            'products_text': '4-amino-4-methyl-1-pentene (aza-Cope product)',
            'reagents_text': 'protic acid',
            'conditions_text': 'CH3CN, 50°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase31_shallow_top10_v1 sprint for Aza-Cope Rearrangement. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CC[NH+]=C(C)C', 'N-allyl-isopropylideneiminium', 'reactants_text', 1),
                ('product', 'NC(C)(C)CC=C', '4-amino-4-methyl-1-pentene (aza-Cope product)', 'products_text', 1),
            ],
        },
        {
            'family': 'Aza-Cope Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Neutral 2-aza-Cope [3,3] rearrangement.',
            'reactants_text': 'N-allyl-N-methyl-1-propenyl-amine',
            'products_text': 'neutral aza-Cope product',
            'reagents_text': 'heat (thermal)',
            'conditions_text': 'PhMe, 150°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase31_shallow_top10_v1 sprint for Aza-Cope Rearrangement. [phase31_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C=CCN(C)C(C)=C', 'N-allyl-N-methyl-1-propenyl-amine', 'reactants_text', 1),
                ('product', 'NC(C)CC=CC', 'neutral aza-Cope product', 'products_text', 1),
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
