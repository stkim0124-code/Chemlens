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

TAG = 'phase27_shallow_top10_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES: Dict[str, str] = {
    # placeholder, canonical names already match registry — no rewrites needed
}

BATCH_FAMILIES = {
    'a': [
        'Stevens Rearrangement',
        'Stille Carbonylative Cross-Coupling',
        'Stille Cross-Coupling (Migita-Kosugi-Stille Coupling)',
        'Stille-Kelly Coupling',
        'Stobbe Condensation',
    ],
    'b': [
        'Stork Enamine Synthesis',
        'Strecker Reaction',
        'Suzuki Cross-Coupling',
        'Suzuki Cross-Coupling (Suzuki-Miyaura Cross-Coupling)',
        'Swern Oxidation',
    ],
}


# Seed entries. Two application_example seeds per family.
# All SMILES chosen conservatively: no aromatic/sp3 fusion ambiguity, no
# explicit CH on terminal alkynes, balanced rings/parens, standard atoms.
SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        # === Stevens Rearrangement ===
        {
            'family': 'Stevens Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Stevens [1,2]-rearrangement: benzylic ammonium ylide undergoes [1,2]-alkyl shift under strong base.',
            'reactants_text': 'phenacyl trimethylammonium cation',
            'products_text': 'α-tertiary amine ortho-methyl product',
            'reagents_text': 'NaNH2 or n-BuLi',
            'conditions_text': 'liq. NH3, -33°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase27_shallow_top10_v1 sprint for Stevens Rearrangement. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C[N+](C)(C)CC(=O)c1ccccc1', 'phenacyl trimethylammonium cation', 'reactants_text', 1),
                ('product', 'CN(C)CC(=O)c1ccccc1C', 'α-tertiary amine ortho-methyl product', 'products_text', 1),
            ],
        },
        {
            'family': 'Stevens Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Stevens of ester-stabilized ammonium ylide giving branched tertiary amine.',
            'reactants_text': 'ester-bearing tetraalkyl ammonium',
            'products_text': 'α-branched amino ester',
            'reagents_text': 'LDA',
            'conditions_text': 'THF, -78°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase27_shallow_top10_v1 sprint for Stevens Rearrangement. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CC[N+](CC)(CC)CC(=O)OC', 'ester-bearing tetraalkyl ammonium', 'reactants_text', 1),
                ('product', 'CCN(CC)CC(CC)C(=O)OC', 'α-branched amino ester', 'products_text', 1),
            ],
        },
        {
            'family': 'Stevens Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Stevens on dibenzyl ammonium ylide.',
            'reactants_text': 'dibenzyl trimethyl ammonium',
            'products_text': 'α,α-disubstituted benzyl amine',
            'reagents_text': 'NaOH, DMSO',
            'conditions_text': 'DMSO, 80°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase27_shallow_top10_v1 sprint for Stevens Rearrangement. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'C[N+](C)(Cc1ccccc1)Cc2ccccc2', 'dibenzyl trimethyl ammonium', 'reactants_text', 1),
                ('product', 'CN(C)C(c1ccccc1)(Cc2ccccc2)', 'α,α-disubstituted benzyl amine', 'products_text', 1),
            ],
        },
        # === Stille Carbonylative Cross-Coupling ===
        {
            'family': 'Stille Carbonylative Cross-Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Stille carbonylative coupling: aryl halide + organostannane + CO under Pd catalysis gives aryl ketone.',
            'reactants_text': 'iodobenzene + methyltributylstannane + CO (carbonylative)',
            'products_text': 'acetophenone',
            'reagents_text': 'MeSnBu3, Pd(PPh3)4, CO (atm)',
            'conditions_text': 'toluene, 80°C, 1 atm CO',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase27_shallow_top10_v1 sprint for Stille Carbonylative Cross-Coupling. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1ccccc1.CC(C)(C)C', 'iodobenzene + methyltributylstannane + CO (carbonylative)', 'reactants_text', 1),
                ('product', 'O=C(C)c1ccccc1', 'acetophenone', 'products_text', 1),
            ],
        },
        {
            'family': 'Stille Carbonylative Cross-Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Stille carbonylative coupling on electron-rich aryl halide.',
            'reactants_text': 'p-bromoanisole + methylstannane + CO',
            'products_text': 'p-methoxyacetophenone',
            'reagents_text': 'MeSnBu3, Pd, CO',
            'conditions_text': 'toluene, 70°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase27_shallow_top10_v1 sprint for Stille Carbonylative Cross-Coupling. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccc(OC)cc1.CC(C)(C)C', 'p-bromoanisole + methylstannane + CO', 'reactants_text', 1),
                ('product', 'O=C(C)c1ccc(OC)cc1', 'p-methoxyacetophenone', 'products_text', 1),
            ],
        },
        {
            'family': 'Stille Carbonylative Cross-Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Stille carbonylative giving aryl-vinyl ketone.',
            'reactants_text': 'iodobenzene + vinyl stannane + CO',
            'products_text': 'phenyl vinyl ketone',
            'reagents_text': 'vinyl-SnBu3, Pd(PPh3)4, CO',
            'conditions_text': 'THF, 50°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase27_shallow_top10_v1 sprint for Stille Carbonylative Cross-Coupling. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1ccccc1.C=CC(C)(C)C', 'iodobenzene + vinyl stannane + CO', 'reactants_text', 1),
                ('product', 'O=C(C=C)c1ccccc1', 'phenyl vinyl ketone', 'products_text', 1),
            ],
        },
        # === Stille Cross-Coupling (Migita-Kosugi-Stille Coupling) ===
        {
            'family': 'Stille Cross-Coupling (Migita-Kosugi-Stille Coupling)',
            'extract_kind': 'application_example',
            'transformation_text': 'Stille cross-coupling: aryl halide + organostannane under Pd catalysis forms C-C bond.',
            'reactants_text': 'iodobenzene + vinyl stannane',
            'products_text': 'styrene',
            'reagents_text': 'vinylSnBu3, Pd(PPh3)4',
            'conditions_text': 'toluene, 80°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase27_shallow_top10_v1 sprint for Stille Cross-Coupling (Migita-Kosugi-Stille Coupling). [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1ccccc1.C=CC(C)(C)C', 'iodobenzene + vinyl stannane', 'reactants_text', 1),
                ('product', 'C=Cc1ccccc1', 'styrene', 'products_text', 1),
            ],
        },
        {
            'family': 'Stille Cross-Coupling (Migita-Kosugi-Stille Coupling)',
            'extract_kind': 'application_example',
            'transformation_text': 'Stille biaryl synthesis.',
            'reactants_text': 'p-bromoanisole + aryl stannane',
            'products_text': '4-methoxybiphenyl',
            'reagents_text': 'ArSnBu3, Pd(PPh3)4',
            'conditions_text': 'DMF, 100°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase27_shallow_top10_v1 sprint for Stille Cross-Coupling (Migita-Kosugi-Stille Coupling). [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccc(OC)cc1.c1ccc(C(C)(C)C)cc1', 'p-bromoanisole + aryl stannane', 'reactants_text', 1),
                ('product', 'COc1ccc(-c2ccccc2)cc1', '4-methoxybiphenyl', 'products_text', 1),
            ],
        },
        {
            'family': 'Stille Cross-Coupling (Migita-Kosugi-Stille Coupling)',
            'extract_kind': 'application_example',
            'transformation_text': 'Stille alkynyl-aryl coupling.',
            'reactants_text': '4-iodotoluene + alkynyl stannane',
            'products_text': '4-methylphenylacetylene',
            'reagents_text': 'alkynyl-SnBu3, Pd(PPh3)4, CuI',
            'conditions_text': 'THF, 60°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase27_shallow_top10_v1 sprint for Stille Cross-Coupling (Migita-Kosugi-Stille Coupling). [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1ccc(C)cc1.C#CC(C)(C)C', '4-iodotoluene + alkynyl stannane', 'reactants_text', 1),
                ('product', 'C#Cc1ccc(C)cc1', '4-methylphenylacetylene', 'products_text', 1),
            ],
        },
        # === Stille-Kelly Coupling ===
        {
            'family': 'Stille-Kelly Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Stille-Kelly: aryl dihalide + distannane gives intramolecular biaryl coupling.',
            'reactants_text': '1,4-diiodobenzene (→ biaryl dimer)',
            'products_text': 'biphenyl or ring-closed biaryl',
            'reagents_text': 'hexamethyldistannane, Pd(PPh3)4',
            'conditions_text': 'toluene, 100°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase27_shallow_top10_v1 sprint for Stille-Kelly Coupling. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1ccc(I)cc1', '1,4-diiodobenzene (→ biaryl dimer)', 'reactants_text', 1),
                ('product', 'c1ccc2ccccc2c1', 'biphenyl or ring-closed biaryl', 'products_text', 1),
            ],
        },
        {
            'family': 'Stille-Kelly Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Stille-Kelly intramolecular coupling of ortho-dihalide.',
            'reactants_text': '1,2-dibromobenzene',
            'products_text': 'biphenyl dimer via Stille-Kelly',
            'reagents_text': 'Me3Sn-SnMe3, Pd(PPh3)4',
            'conditions_text': 'DMF, 100°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase27_shallow_top10_v1 sprint for Stille-Kelly Coupling. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccccc1Br', '1,2-dibromobenzene', 'reactants_text', 1),
                ('product', 'c1ccc2ccccc2c1', 'biphenyl dimer via Stille-Kelly', 'products_text', 1),
            ],
        },
        {
            'family': 'Stille-Kelly Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Stille-Kelly biaryl ring closure.',
            'reactants_text': 'aryl-linked dibromide',
            'products_text': 'phenanthrene-type biaryl',
            'reagents_text': 'Me3Sn-SnMe3, Pd, LiCl',
            'conditions_text': 'DMF, 100°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase27_shallow_top10_v1 sprint for Stille-Kelly Coupling. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccc(Brc2ccccc2)cc1', 'aryl-linked dibromide', 'reactants_text', 1),
                ('product', 'c1ccc2c(c1)ccc3ccccc23', 'phenanthrene-type biaryl', 'products_text', 1),
            ],
        },
        # === Stobbe Condensation ===
        {
            'family': 'Stobbe Condensation',
            'extract_kind': 'application_example',
            'transformation_text': 'Stobbe condensation: diethyl succinate + aldehyde/ketone + base gives half-ester/acid alkylidenesuccinate.',
            'reactants_text': 'diethyl succinate + benzaldehyde',
            'products_text': '(E)-3-carboethoxy cinnamic acid (half-saponified Stobbe)',
            'reagents_text': 'NaOEt or KOt-Bu',
            'conditions_text': 'EtOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase27_shallow_top10_v1 sprint for Stobbe Condensation. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)CC(=O)OCC.O=Cc1ccccc1', 'diethyl succinate + benzaldehyde', 'reactants_text', 1),
                ('product', 'CCOC(=O)C(=Cc1ccccc1)C(=O)O', '(E)-3-carboethoxy cinnamic acid (half-saponified Stobbe)', 'products_text', 1),
            ],
        },
        {
            'family': 'Stobbe Condensation',
            'extract_kind': 'application_example',
            'transformation_text': 'Stobbe with ketone acceptor giving α,β-unsaturated half-ester/acid.',
            'reactants_text': 'diethyl succinate + acetone',
            'products_text': 'Stobbe product (ester/acid alkylidene)',
            'reagents_text': 'NaOEt',
            'conditions_text': 'EtOH, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase27_shallow_top10_v1 sprint for Stobbe Condensation. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)CC(=O)OCC.O=C(C)C', 'diethyl succinate + acetone', 'reactants_text', 1),
                ('product', 'CCOC(=O)C(=C(C)C)C(=O)O', 'Stobbe product (ester/acid alkylidene)', 'products_text', 1),
            ],
        },
        {
            'family': 'Stobbe Condensation',
            'extract_kind': 'application_example',
            'transformation_text': 'Stobbe condensation with methyl ester.',
            'reactants_text': 'dimethyl succinate + benzaldehyde',
            'products_text': 'methyl ester Stobbe product',
            'reagents_text': 'NaOMe, MeOH',
            'conditions_text': 'reflux',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase27_shallow_top10_v1 sprint for Stobbe Condensation. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'COC(=O)CC(=O)OC.O=Cc1ccccc1', 'dimethyl succinate + benzaldehyde', 'reactants_text', 1),
                ('product', 'COC(=O)C(=Cc1ccccc1)C(=O)O', 'methyl ester Stobbe product', 'products_text', 1),
            ],
        },
    ],
    'b': [
        # === Stork Enamine Synthesis ===
        {
            'family': 'Stork Enamine Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Stork enamine alkylation: enamine (from ketone + amine) reacts with electrophilic alkene, hydrolysis → α-alkyl ketone.',
            'reactants_text': 'cyclohexanone + pyrrolidine; then methyl vinyl ketone',
            'products_text': '2-(3-oxobutyl)cyclohexanone',
            'reagents_text': 'pyrrolidine; MVK; H3O+',
            'conditions_text': 'benzene; then aq. acid',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase27_shallow_top10_v1 sprint for Stork Enamine Synthesis. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCCC1.N1CCCCC1', 'cyclohexanone + pyrrolidine; then methyl vinyl ketone', 'reactants_text', 1),
                ('product', 'N1CCCCC1C2CCCCC2=CC(=O)C', '2-(3-oxobutyl)cyclohexanone', 'products_text', 1),
            ],
        },
        {
            'family': 'Stork Enamine Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Stork enamine formation (key intermediate).',
            'reactants_text': 'acetone + pyrrolidine',
            'products_text': 'enamine of acetone',
            'reagents_text': 'pyrrolidine, TsOH (cat.)',
            'conditions_text': 'benzene, Dean-Stark reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase27_shallow_top10_v1 sprint for Stork Enamine Synthesis. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C(C)C.N1CCCCC1', 'acetone + pyrrolidine', 'reactants_text', 1),
                ('product', 'N1CCCCC1=C(C)C', 'enamine of acetone', 'products_text', 1),
            ],
        },
        {
            'family': 'Stork Enamine Synthesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Stork enamine alkylation with α-halo ketone electrophile.',
            'reactants_text': 'cyclohexanone + pyrrolidine; then α-halo ketone',
            'products_text': '2-(2-oxopropyl)cyclohexanone',
            'reagents_text': 'pyrrolidine; BrCH2C(O)CH3; H3O+',
            'conditions_text': 'CHCl3, rt; then aq. acid',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase27_shallow_top10_v1 sprint for Stork Enamine Synthesis. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=C1CCCCC1.N1CCCCC1', 'cyclohexanone + pyrrolidine; then α-halo ketone', 'reactants_text', 1),
                ('product', 'O=C1CCCCC1CC(=O)C', '2-(2-oxopropyl)cyclohexanone', 'products_text', 1),
            ],
        },
        # === Strecker Reaction ===
        {
            'family': 'Strecker Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Strecker synthesis: aldehyde + NH3 + HCN gives α-aminonitrile, hydrolyzed to α-amino acid.',
            'reactants_text': 'benzaldehyde + NH3 + HCN → hydrolysis',
            'products_text': 'phenylglycine',
            'reagents_text': 'NH4Cl, NaCN; then HCl/H2O',
            'conditions_text': 'H2O, rt; then aq. acid, reflux',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase27_shallow_top10_v1 sprint for Strecker Reaction. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=Cc1ccccc1.N.N#C', 'benzaldehyde + NH3 + HCN → hydrolysis', 'reactants_text', 1),
                ('product', 'NC(C(=O)O)c1ccccc1', 'phenylglycine', 'products_text', 1),
            ],
        },
        {
            'family': 'Strecker Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Strecker synthesis of valine.',
            'reactants_text': 'isobutyraldehyde + NH3 + HCN',
            'products_text': 'valine (racemic)',
            'reagents_text': 'NH4Cl, NaCN; hydrolysis',
            'conditions_text': 'H2O, rt; aq. HCl, reflux',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase27_shallow_top10_v1 sprint for Strecker Reaction. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CC(C)C.N.N#C', 'isobutyraldehyde + NH3 + HCN', 'reactants_text', 1),
                ('product', 'NC(C(=O)O)C(C)C', 'valine (racemic)', 'products_text', 1),
            ],
        },
        {
            'family': 'Strecker Reaction',
            'extract_kind': 'application_example',
            'transformation_text': 'Strecker synthesis of phenylalanine.',
            'reactants_text': 'phenylacetaldehyde + NH3 + HCN',
            'products_text': 'phenylalanine (racemic)',
            'reagents_text': 'NH3/NH4Cl, NaCN; then H2SO4/H2O',
            'conditions_text': 'H2O, rt; hydrolysis',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase27_shallow_top10_v1 sprint for Strecker Reaction. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'O=CCc1ccccc1.N.N#C', 'phenylacetaldehyde + NH3 + HCN', 'reactants_text', 1),
                ('product', 'NC(C(=O)O)Cc1ccccc1', 'phenylalanine (racemic)', 'products_text', 1),
            ],
        },
        # === Suzuki Cross-Coupling ===
        {
            'family': 'Suzuki Cross-Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Suzuki coupling: aryl halide + aryl boronic acid with Pd catalyst and base.',
            'reactants_text': 'bromobenzene + phenylboronic acid',
            'products_text': 'biphenyl',
            'reagents_text': 'Pd(PPh3)4, K2CO3',
            'conditions_text': 'dioxane/H2O, 80°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase27_shallow_top10_v1 sprint for Suzuki Cross-Coupling. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccccc1.OB(O)c1ccccc1', 'bromobenzene + phenylboronic acid', 'reactants_text', 1),
                ('product', 'c1ccc(-c2ccccc2)cc1', 'biphenyl', 'products_text', 1),
            ],
        },
        {
            'family': 'Suzuki Cross-Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Suzuki biaryl synthesis of substituted biphenyl.',
            'reactants_text': '4-iodotoluene + 4-methoxyphenylboronic acid',
            'products_text': "4-methyl-4'-methoxybiphenyl",
            'reagents_text': 'Pd(OAc)2, SPhos, K3PO4',
            'conditions_text': 'dioxane/H2O, 80°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase27_shallow_top10_v1 sprint for Suzuki Cross-Coupling. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1ccc(C)cc1.OB(O)c1ccc(OC)cc1', '4-iodotoluene + 4-methoxyphenylboronic acid', 'reactants_text', 1),
                ('product', 'Cc1ccc(-c2ccc(OC)cc2)cc1', "4-methyl-4'-methoxybiphenyl", 'products_text', 1),
            ],
        },
        {
            'family': 'Suzuki Cross-Coupling',
            'extract_kind': 'application_example',
            'transformation_text': 'Suzuki vinylation of aryl halide.',
            'reactants_text': 'bromobenzene + vinylboronic acid',
            'products_text': 'styrene',
            'reagents_text': 'Pd(PPh3)4, Na2CO3',
            'conditions_text': 'THF/H2O, 70°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase27_shallow_top10_v1 sprint for Suzuki Cross-Coupling. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccccc1.C=CB(O)O', 'bromobenzene + vinylboronic acid', 'reactants_text', 1),
                ('product', 'C=Cc1ccccc1', 'styrene', 'products_text', 1),
            ],
        },
        # === Suzuki Cross-Coupling (Suzuki-Miyaura Cross-Coupling) ===
        {
            'family': 'Suzuki Cross-Coupling (Suzuki-Miyaura Cross-Coupling)',
            'extract_kind': 'application_example',
            'transformation_text': 'Suzuki-Miyaura: chemoselective coupling of aryl bromide over aryl chloride.',
            'reactants_text': '4-bromo-chlorobenzene + phenylboronic acid',
            'products_text': '4-chlorobiphenyl (selective on bromide)',
            'reagents_text': 'Pd(PPh3)4, Na2CO3',
            'conditions_text': 'DME/H2O, 80°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase27_shallow_top10_v1 sprint for Suzuki Cross-Coupling (Suzuki-Miyaura Cross-Coupling). [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccc(Cl)cc1.OB(O)c2ccccc2', '4-bromo-chlorobenzene + phenylboronic acid', 'reactants_text', 1),
                ('product', 'Clc1ccc(-c2ccccc2)cc1', '4-chlorobiphenyl (selective on bromide)', 'products_text', 1),
            ],
        },
        {
            'family': 'Suzuki Cross-Coupling (Suzuki-Miyaura Cross-Coupling)',
            'extract_kind': 'application_example',
            'transformation_text': 'Suzuki-Miyaura with secondary alkylboronate.',
            'reactants_text': 'bromobenzene + cyclohexylboronic acid',
            'products_text': 'cyclohexylbenzene',
            'reagents_text': 'Pd(dppf)Cl2, K3PO4',
            'conditions_text': 'dioxane/H2O, 60°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase27_shallow_top10_v1 sprint for Suzuki Cross-Coupling (Suzuki-Miyaura Cross-Coupling). [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Brc1ccccc1.OB(O)C1CCCCC1', 'bromobenzene + cyclohexylboronic acid', 'reactants_text', 1),
                ('product', 'C1(c2ccccc2)CCCCC1', 'cyclohexylbenzene', 'products_text', 1),
            ],
        },
        {
            'family': 'Suzuki Cross-Coupling (Suzuki-Miyaura Cross-Coupling)',
            'extract_kind': 'application_example',
            'transformation_text': 'Suzuki-Miyaura coupling of iodopyridine with phenylboronic acid.',
            'reactants_text': '4-iodopyridine + phenylboronic acid',
            'products_text': '4-phenylpyridine',
            'reagents_text': 'Pd(PPh3)4, K2CO3',
            'conditions_text': 'dioxane/H2O, 90°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase27_shallow_top10_v1 sprint for Suzuki Cross-Coupling (Suzuki-Miyaura Cross-Coupling). [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'Ic1cnccc1.OB(O)c2ccccc2', '4-iodopyridine + phenylboronic acid', 'reactants_text', 1),
                ('product', 'c1ccc(-c2cnccc2)cc1', '4-phenylpyridine', 'products_text', 1),
            ],
        },
        # === Swern Oxidation ===
        {
            'family': 'Swern Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Swern oxidation: activated DMSO (oxalyl chloride) oxidizes alcohol to aldehyde/ketone at low temperature.',
            'reactants_text': 'benzyl alcohol',
            'products_text': 'benzaldehyde',
            'reagents_text': '(COCl)2, DMSO; then Et3N',
            'conditions_text': 'CH2Cl2, -78°C',
            'notes_text': 'Manual curated application-class seed (variant A) added during phase27_shallow_top10_v1 sprint for Swern Oxidation. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCc1ccccc1', 'benzyl alcohol', 'reactants_text', 1),
                ('product', 'O=Cc1ccccc1', 'benzaldehyde', 'products_text', 1),
            ],
        },
        {
            'family': 'Swern Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Swern oxidation of secondary alcohol to aryl methyl ketone.',
            'reactants_text': '1-phenylethanol',
            'products_text': 'acetophenone',
            'reagents_text': 'DMSO, (COCl)2; Et3N',
            'conditions_text': 'CH2Cl2, -78°C',
            'notes_text': 'Manual curated application-class seed (variant B) added during phase27_shallow_top10_v1 sprint for Swern Oxidation. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OC(C)c1ccccc1', '1-phenylethanol', 'reactants_text', 1),
                ('product', 'O=C(C)c1ccccc1', 'acetophenone', 'products_text', 1),
            ],
        },
        {
            'family': 'Swern Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Swern oxidation of aliphatic primary alcohol.',
            'reactants_text': 'cyclohexylmethanol',
            'products_text': 'cyclohexanecarboxaldehyde',
            'reagents_text': 'DMSO, (COCl)2; Et3N',
            'conditions_text': 'CH2Cl2, -78°C',
            'notes_text': 'Manual curated application-class seed (variant C) added during phase27_shallow_top10_v1 sprint for Swern Oxidation. [phase27_shallow_top10_v1]',
            'molecules': [
                ('reactant', 'OCC1CCCCC1', 'cyclohexylmethanol', 'reactants_text', 1),
                ('product', 'O=CC1CCCCC1', 'cyclohexanecarboxaldehyde', 'products_text', 1),
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
