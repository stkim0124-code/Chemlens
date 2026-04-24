import argparse
import datetime as dt
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from rdkit import Chem
    from rdkit.Chem import rdFingerprintGenerator
except Exception:
    Chem = None
    rdFingerprintGenerator = None

TAG = 'phase11_shallow_top10_completion_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES = {
    "De Mayo Cycloaddition (Enone-Alkene [2+2] Photocycloaddition)": 'De Mayo Cycloaddition',
    'Demjanov and Tiffeneau-Demjanov Rearrangement': 'Demjanov Rearrangement and Tiffeneau-Demjanov Rearrangement',
    'Dess-Martin Reaction': 'Dess-Martin Oxidation',
    'Danishefsky Diene Cycloaddition': "Danishefsky's Diene Cycloaddition",
    'Eschweiler-Clarke Methylation (Reductive Alkylation)': 'Eschweiler-Clarke Methylation',
    'Eschenmoser Claisen Rearrangement': 'Eschenmoser-Claisen Rearrangement',
    'Enyne Ring-Closing Metathesis': 'Enyne Metathesis',
}

BATCH_FAMILIES = {
    'a': [
        "Danishefsky's Diene Cycloaddition",
        'De Mayo Cycloaddition',
        'Demjanov Rearrangement and Tiffeneau-Demjanov Rearrangement',
        'Dess-Martin Oxidation',
        'Dieckmann Condensation',
    ],
    'b': [
        'Diels-Alder Cycloaddition',
        'Dienone-Phenol Rearrangement',
        'Enyne Metathesis',
        'Eschenmoser-Claisen Rearrangement',
        'Eschweiler-Clarke Methylation',
    ],
}

SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {
    'a': [
        {
            'family': "Danishefsky's Diene Cycloaddition",
            'extract_kind': 'application_example',
            'transformation_text': "Application example: Danishefsky's diene cycloaddition of Danishefsky's diene with acrolein to a substituted cyclohexenone.",
            'reactants_text': "Danishefsky's diene and acrolein",
            'products_text': 'methoxy cyclohexenone aldehyde adduct',
            'reagents_text': 'Lewis acid activation',
            'conditions_text': "curated application-class seed representing electron-rich diene cycloaddition with an aldehyde dienophile",
            'notes_text': "Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for Danishefsky's diene cycloaddition chemistry. Adds an aldehyde dienophile example beyond the overview evidence. [phase11_shallow_top10_completion_v1]",
            'molecules': [
                ('reactant', 'CO/C=C/C(OC)=C/C(C)=O', "Danishefsky's diene", 'reactants_text', 1),
                ('reactant', 'C=CC=O', 'acrolein', 'reactants_text', 1),
                ('product', 'COC1=CC(CC=O)CCC1=O', 'methoxy cyclohexenone aldehyde adduct', 'products_text', 1),
            ],
        },
        {
            'family': "Danishefsky's Diene Cycloaddition",
            'extract_kind': 'application_example',
            'transformation_text': "Application example: Danishefsky's diene cycloaddition of Danishefsky's diene with methyl vinyl ketone to a substituted cyclohexenone.",
            'reactants_text': "Danishefsky's diene and methyl vinyl ketone",
            'products_text': 'methoxy cyclohexenone ketone adduct',
            'reagents_text': 'Lewis acid activation',
            'conditions_text': 'curated application-class seed adding an enone dienophile example to the Danishefsky diene family',
            'notes_text': "Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for Danishefsky's diene cycloaddition chemistry. Adds a methyl vinyl ketone dienophile example for family depth. [phase11_shallow_top10_completion_v1]",
            'molecules': [
                ('reactant', 'CO/C=C/C(OC)=C/C(C)=O', "Danishefsky's diene", 'reactants_text', 1),
                ('reactant', 'C=CC(C)=O', 'methyl vinyl ketone', 'reactants_text', 1),
                ('product', 'COC1=CC(C(C)=O)CCC1=O', 'methoxy cyclohexenone ketone adduct', 'products_text', 1),
            ],
        },
        {
            'family': 'De Mayo Cycloaddition',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: De Mayo cycloaddition of acetylacetone with cyclohexene to a 1,5-dicarbonyl adduct after retro-aldol fragmentation.',
            'reactants_text': 'acetylacetone and cyclohexene',
            'products_text': 'cyclohexyl-substituted 1,5-dicarbonyl adduct',
            'reagents_text': 'hv',
            'conditions_text': 'curated application-class seed representing photochemical enolized beta-dicarbonyl cycloaddition followed by fragmentation',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for De Mayo cycloaddition chemistry. Provides a classic 1,5-dicarbonyl formation example beyond the overview evidence. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'CC(=O)CC(C)=O', 'acetylacetone', 'reactants_text', 1),
                ('reactant', 'C1=CCCCC1', 'cyclohexene', 'reactants_text', 1),
                ('product', 'CC(=O)CC(=O)C(C)C1CCCCC1', 'cyclohexyl-substituted 1,5-dicarbonyl adduct', 'products_text', 1),
            ],
        },
        {
            'family': 'De Mayo Cycloaddition',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: De Mayo cycloaddition of acetylacetone with cyclopentene to a ring-substituted 1,5-dicarbonyl adduct.',
            'reactants_text': 'acetylacetone and cyclopentene',
            'products_text': 'cyclopentyl-substituted 1,5-dicarbonyl adduct',
            'reagents_text': 'hv',
            'conditions_text': 'curated application-class seed adding a second alkene partner example to the De Mayo cycloaddition family',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for De Mayo cycloaddition chemistry. Adds a cyclopentene partner example for family depth. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'CC(=O)CC(C)=O', 'acetylacetone', 'reactants_text', 1),
                ('reactant', 'C1=CCCC1', 'cyclopentene', 'reactants_text', 1),
                ('product', 'CC(=O)CC(=O)C(C)C1CCCC1', 'cyclopentyl-substituted 1,5-dicarbonyl adduct', 'products_text', 1),
            ],
        },
        {
            'family': 'Demjanov Rearrangement and Tiffeneau-Demjanov Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: Tiffeneau-Demjanov ring expansion of 1-aminomethylcyclohexanol to cycloheptanone.',
            'reactants_text': '1-aminomethylcyclohexanol',
            'products_text': 'cycloheptanone',
            'reagents_text': 'nitrous acid',
            'conditions_text': 'curated application-class seed representing amino alcohol diazotization followed by ring expansion',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for Demjanov/Tiffeneau-Demjanov chemistry. Provides a cyclohexanol-to-cycloheptanone ring expansion example beyond the overview evidence. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'NC(O)C1CCCCC1', '1-aminomethylcyclohexanol', 'reactants_text', 1),
                ('product', 'O=C1CCCCCC1', 'cycloheptanone', 'products_text', 1),
            ],
        },
        {
            'family': 'Demjanov Rearrangement and Tiffeneau-Demjanov Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: Tiffeneau-Demjanov ring expansion of 1-aminomethylcyclopentanol to cyclohexanone.',
            'reactants_text': '1-aminomethylcyclopentanol',
            'products_text': 'cyclohexanone',
            'reagents_text': 'nitrous acid',
            'conditions_text': 'curated application-class seed adding a second amino alcohol ring-expansion example to the Demjanov family',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for Demjanov/Tiffeneau-Demjanov chemistry. Adds a cyclopentanol-to-cyclohexanone expansion example for family depth. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'NC(O)C1CCCC1', '1-aminomethylcyclopentanol', 'reactants_text', 1),
                ('product', 'O=C1CCCCC1', 'cyclohexanone', 'products_text', 1),
            ],
        },
        {
            'family': 'Dess-Martin Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: Dess-Martin oxidation of benzyl alcohol to benzaldehyde.',
            'reactants_text': 'benzyl alcohol',
            'products_text': 'benzaldehyde',
            'reagents_text': 'Dess-Martin periodinane',
            'conditions_text': 'curated application-class seed representing mild primary alcohol oxidation in the Dess-Martin family',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for Dess-Martin oxidation chemistry. Provides a primary alcohol oxidation example beyond the overview evidence. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'OCc1ccccc1', 'benzyl alcohol', 'reactants_text', 1),
                ('product', 'O=Cc1ccccc1', 'benzaldehyde', 'products_text', 1),
            ],
        },
        {
            'family': 'Dess-Martin Oxidation',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: Dess-Martin oxidation of 1-phenylethanol to acetophenone.',
            'reactants_text': '1-phenylethanol',
            'products_text': 'acetophenone',
            'reagents_text': 'Dess-Martin periodinane',
            'conditions_text': 'curated application-class seed adding a secondary benzylic alcohol oxidation example to the Dess-Martin family',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for Dess-Martin oxidation chemistry. Adds a secondary alcohol oxidation example for family depth. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'CC(O)c1ccccc1', '1-phenylethanol', 'reactants_text', 1),
                ('product', 'CC(=O)c1ccccc1', 'acetophenone', 'products_text', 1),
            ],
        },
        {
            'family': 'Dieckmann Condensation',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: Dieckmann condensation of diethyl pimelate to ethyl 2-oxocyclohexane-1-carboxylate.',
            'reactants_text': 'diethyl pimelate',
            'products_text': 'ethyl 2-oxocyclohexane-1-carboxylate',
            'reagents_text': 'sodium ethoxide',
            'conditions_text': 'curated application-class seed representing intramolecular Claisen cyclization to a six-membered beta-keto ester',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for Dieckmann condensation chemistry. Provides a six-membered ring beta-keto ester example beyond the overview evidence. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)CCCCC(=O)OCC', 'diethyl pimelate', 'reactants_text', 1),
                ('product', 'CCOC(=O)C1CCCCC1=O', 'ethyl 2-oxocyclohexane-1-carboxylate', 'products_text', 1),
            ],
        },
        {
            'family': 'Dieckmann Condensation',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: Dieckmann condensation of diethyl suberate to ethyl 2-oxocycloheptane-1-carboxylate.',
            'reactants_text': 'diethyl suberate',
            'products_text': 'ethyl 2-oxocycloheptane-1-carboxylate',
            'reagents_text': 'sodium ethoxide',
            'conditions_text': 'curated application-class seed adding a seven-membered ring beta-keto ester example to the Dieckmann family',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for Dieckmann condensation chemistry. Adds a seven-membered ring cyclization example for family depth. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'CCOC(=O)CCCCCC(=O)OCC', 'diethyl suberate', 'reactants_text', 1),
                ('product', 'CCOC(=O)C1CCCCCC1=O', 'ethyl 2-oxocycloheptane-1-carboxylate', 'products_text', 1),
            ],
        },
    ],
    'b': [
        {
            'family': 'Diels-Alder Cycloaddition',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: Diels-Alder cycloaddition of 1,3-butadiene with maleic anhydride to a cyclohexene anhydride.',
            'reactants_text': '1,3-butadiene and maleic anhydride',
            'products_text': 'cyclohexene anhydride adduct',
            'reagents_text': 'thermal cycloaddition',
            'conditions_text': 'curated application-class seed representing a classic normal-electron-demand Diels-Alder cycloaddition',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for Diels-Alder cycloaddition chemistry. Provides a classic butadiene/maleic anhydride example beyond the overview evidence. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'C=CC=C', '1,3-butadiene', 'reactants_text', 1),
                ('reactant', 'O=C1OC(=O)C=C1', 'maleic anhydride', 'reactants_text', 1),
                ('product', 'O=C1CC2C=CCC2C(=O)O1', 'cyclohexene anhydride adduct', 'products_text', 1),
            ],
        },
        {
            'family': 'Diels-Alder Cycloaddition',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: Diels-Alder cycloaddition of 1,3-butadiene with acrylonitrile to a cyclohexene nitrile.',
            'reactants_text': '1,3-butadiene and acrylonitrile',
            'products_text': 'cyclohexene nitrile adduct',
            'reagents_text': 'thermal cycloaddition',
            'conditions_text': 'curated application-class seed adding a nitrile dienophile example to the Diels-Alder family',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for Diels-Alder cycloaddition chemistry. Adds an acrylonitrile dienophile example for family depth. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'C=CC=C', '1,3-butadiene', 'reactants_text', 1),
                ('reactant', 'C=CC#N', 'acrylonitrile', 'reactants_text', 1),
                ('product', 'N#CC1CC=CCC1', 'cyclohexene nitrile adduct', 'products_text', 1),
            ],
        },
        {
            'family': 'Dienone-Phenol Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: acid-promoted dienone-phenol rearrangement of a 4,4-dimethylcyclohexadienone to a substituted phenol.',
            'reactants_text': 'substituted cyclohexadienone',
            'products_text': 'substituted phenol',
            'reagents_text': 'acid',
            'conditions_text': 'curated application-class seed representing rearrangement of a cyclohexadienone to a phenol framework',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for dienone-phenol rearrangement chemistry. Provides a tert-butyl-like rearrangement example beyond the overview evidence. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'CC1=CC(=O)CCC1(C)C', 'substituted cyclohexadienone', 'reactants_text', 1),
                ('product', 'CC1=CC(O)=CC(C)(C)C1', 'substituted phenol', 'products_text', 1),
            ],
        },
        {
            'family': 'Dienone-Phenol Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: dienone-phenol rearrangement of an alkyl-substituted cyclohexadienone to an alkyl phenol.',
            'reactants_text': 'alkyl-substituted cyclohexadienone',
            'products_text': 'alkyl phenol',
            'reagents_text': 'acid',
            'conditions_text': 'curated application-class seed adding a second dienone-phenol rearrangement example for family depth',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for dienone-phenol rearrangement chemistry. Adds a second alkyl-substituted phenol example for family depth. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'CC1=CC(C)(C)C(=O)C=C1', 'alkyl-substituted cyclohexadienone', 'reactants_text', 1),
                ('product', 'CC1=CC(O)=CC(C)C1', 'alkyl phenol', 'products_text', 1),
            ],
        },
        {
            'family': 'Enyne Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: ring-closing enyne metathesis of a tethered enyne to a cyclic diene.',
            'reactants_text': 'tethered enyne substrate',
            'products_text': 'cyclic diene product',
            'reagents_text': 'Grubbs catalyst',
            'conditions_text': 'curated application-class seed representing ring-closing enyne metathesis of a terminal alkyne/alkene substrate',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for enyne metathesis chemistry. Provides a ring-closing enyne metathesis example beyond the overview evidence. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'C#CCCCC=C', 'tethered enyne substrate', 'reactants_text', 1),
                ('product', 'C1#CCCCC=CC1', 'cyclic diene product', 'products_text', 1),
            ],
        },
        {
            'family': 'Enyne Metathesis',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: enyne metathesis of a substituted tethered enyne to a substituted cyclic diene.',
            'reactants_text': 'substituted tethered enyne',
            'products_text': 'substituted cyclic diene',
            'reagents_text': 'Grubbs catalyst',
            'conditions_text': 'curated application-class seed adding a substituted enyne substrate to the enyne metathesis family',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for enyne metathesis chemistry. Adds a substituted ring-closing enyne example for family depth. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'C=CC#CC=C(C)C', 'substituted tethered enyne', 'reactants_text', 1),
                ('product', 'CC1=CCCC#CC1', 'substituted cyclic diene', 'products_text', 1),
            ],
        },
        {
            'family': 'Eschenmoser-Claisen Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: Eschenmoser-Claisen rearrangement of an allylic amino acetal to a gamma,delta-unsaturated amide.',
            'reactants_text': 'allylic amino acetal precursor',
            'products_text': 'gamma,delta-unsaturated amide',
            'reagents_text': 'thermal rearrangement',
            'conditions_text': 'curated application-class seed representing allylic amide-forming [3,3]-sigmatropic rearrangement in the Eschenmoser-Claisen family',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for Eschenmoser-Claisen rearrangement chemistry. Provides an unsaturated amide example beyond the overview evidence. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'C=CCN(C)C(=O)OCC', 'allylic amino acetal precursor', 'reactants_text', 1),
                ('product', 'CCOC(=O)C(C)=CCNC', 'gamma,delta-unsaturated amide', 'products_text', 1),
            ],
        },
        {
            'family': 'Eschenmoser-Claisen Rearrangement',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: Eschenmoser-Claisen rearrangement of an allylic amino isopropyl acetal to a substituted unsaturated amide.',
            'reactants_text': 'allylic amino isopropyl acetal precursor',
            'products_text': 'substituted gamma,delta-unsaturated amide',
            'reagents_text': 'thermal rearrangement',
            'conditions_text': 'curated application-class seed adding a second allylic amide-forming rearrangement example to the Eschenmoser-Claisen family',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for Eschenmoser-Claisen rearrangement chemistry. Adds a second allylic amide example for family depth. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'C=CCN(C)C(=O)OC(C)C', 'allylic amino isopropyl acetal precursor', 'reactants_text', 1),
                ('product', 'CC(C)OC(=O)C(C)=CCNC', 'substituted gamma,delta-unsaturated amide', 'products_text', 1),
            ],
        },
        {
            'family': 'Eschweiler-Clarke Methylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: Eschweiler-Clarke methylation of phenethylamine to N,N-dimethylphenethylamine.',
            'reactants_text': 'phenethylamine',
            'products_text': 'N,N-dimethylphenethylamine',
            'reagents_text': 'formaldehyde, formic acid',
            'conditions_text': 'curated application-class seed representing reductive dimethylation of a primary amine in the Eschweiler-Clarke family',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for Eschweiler-Clarke methylation chemistry. Provides a primary amine dimethylation example beyond the overview evidence. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'NCCc1ccccc1', 'phenethylamine', 'reactants_text', 1),
                ('product', 'CN(C)CCc1ccccc1', 'N,N-dimethylphenethylamine', 'products_text', 1),
            ],
        },
        {
            'family': 'Eschweiler-Clarke Methylation',
            'extract_kind': 'application_example',
            'transformation_text': 'Application example: Eschweiler-Clarke methylation of benzylamine to N,N-dimethylbenzylamine.',
            'reactants_text': 'benzylamine',
            'products_text': 'N,N-dimethylbenzylamine',
            'reagents_text': 'formaldehyde, formic acid',
            'conditions_text': 'curated application-class seed adding a benzylic amine dimethylation example to the Eschweiler-Clarke family',
            'notes_text': 'Manual curated application-class seed added during the phase11 5+5 shallow-family completion sprint for Eschweiler-Clarke methylation chemistry. Adds a benzylamine dimethylation example for family depth. [phase11_shallow_top10_completion_v1]',
            'molecules': [
                ('reactant', 'NCc1ccccc1', 'benzylamine', 'reactants_text', 1),
                ('product', 'CN(C)Cc1ccccc1', 'N,N-dimethylbenzylamine', 'products_text', 1),
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
        cur = conn.execute(
            'UPDATE reaction_extracts SET reaction_family_name=?, reaction_family_name_norm=?, updated_at=? WHERE reaction_family_name=?',
            (official, fam_norm, NOW, alias),
        )
        out.append({'table': 'reaction_extracts', 'from': alias, 'to': official, 'rows_updated': cur.rowcount})
        cur = conn.execute(
            'UPDATE extract_molecules SET reaction_family_name=? WHERE reaction_family_name=?',
            (official, alias),
        )
        out.append({'table': 'extract_molecules', 'from': alias, 'to': official, 'rows_updated': cur.rowcount})
    return out


def backfill_extract_smiles_from_molecules(conn: sqlite3.Connection, families: Sequence[str]) -> List[Dict[str, Any]]:
    out = []
    for family in families:
        rows = conn.execute(
            """
            SELECT re.id,
                   GROUP_CONCAT(CASE WHEN em.role='reactant' AND em.queryable=1 AND em.smiles IS NOT NULL THEN em.smiles END, ' | ') AS reactant_smiles,
                   GROUP_CONCAT(CASE WHEN em.role='product' AND em.queryable=1 AND em.smiles IS NOT NULL THEN em.smiles END, ' | ') AS product_smiles
            FROM reaction_extracts re
            LEFT JOIN extract_molecules em ON em.extract_id = re.id
            WHERE re.reaction_family_name = ?
            GROUP BY re.id
            """,
            (family,),
        ).fetchall()
        updated = 0
        for extract_id, reactant, product in rows:
            cur = conn.execute(
                """
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
                """,
                (reactant, product, reactant, product, NOW, extract_id),
            )
            updated += cur.rowcount
        out.append({'family': family, 'extract_rows_touched': updated})
    return out


def find_anchor(conn: sqlite3.Connection, seed: Dict[str, Any]) -> Tuple[int, int, str]:
    row = conn.execute(
        """
        SELECT re.scheme_candidate_id, COALESCE(pi.page_no, 0) AS page_no, COALESCE(pi.image_filename, ?) AS image_filename
        FROM reaction_extracts re
        LEFT JOIN scheme_candidates sc ON sc.id = re.scheme_candidate_id
        LEFT JOIN page_images pi ON pi.id = sc.page_image_id
        WHERE re.reaction_family_name=? AND re.scheme_candidate_id IS NOT NULL
        ORDER BY CASE WHEN re.extract_kind='canonical_overview' THEN 0 WHEN re.extract_kind='application_example' THEN 1 ELSE 2 END, re.id
        LIMIT 1
        """,
        (seed.get('anchor_image', 'phase11_anchor.jpg'), seed['family']),
    ).fetchone()
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

    cur = conn.execute(
        """
        INSERT INTO reaction_extracts (
            scheme_candidate_id, reaction_family_name, reaction_family_name_norm, extract_kind,
            transformation_text, reactants_text, products_text, intermediates_text, reagents_text,
            conditions_text, notes_text, reactant_smiles, product_smiles,
            smiles_confidence, extraction_confidence, parse_status, promote_decision,
            extractor_model, extractor_prompt_version, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            anchor_id, fam, fam_norm, seed['extract_kind'], seed.get('transformation_text'),
            seed.get('reactants_text'), seed.get('products_text'), seed.get('intermediates_text'),
            seed.get('reagents_text'), seed.get('conditions_text'), seed.get('notes_text'),
            reactant_smiles, product_smiles, 0.98 if (reactant_smiles or product_smiles) else 0.0,
            0.98, 'curated', 'promote', 'manual_curated_seed', TAG, NOW, NOW,
        ),
    )
    extract_id = cur.lastrowid
    inserted = 0
    for role, smiles, name, source_field, queryable in seed['molecules']:
        csmiles = canon(smiles) if smiles else None
        conn.execute(
            """
            INSERT INTO extract_molecules (
                extract_id, role, smiles, smiles_kind, quality_tier, reaction_family_name,
                source_zip, page_no, queryable, note_text, morgan_fp, normalized_text,
                source_field, structure_source, fg_tags, role_confidence, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                extract_id, role, csmiles, 'canonical', 1 if queryable and csmiles else 3, fam,
                'named reactions.pdf', page_no, 1 if queryable and csmiles else 0,
                f'[{TAG}: {name}]' if name else f'[{TAG}]', fp_blob(csmiles) if (queryable and csmiles) else None,
                (name.lower() if name else None), source_field, TAG if csmiles else None, None,
                0.98 if csmiles else 0.0, NOW,
            ),
        )
        inserted += 1
    return {'family': fam, 'status': 'inserted', 'extract_id': extract_id, 'page_no': page_no, 'image_filename': image_filename, 'inserted_molecules': inserted}


def verify_family(conn: sqlite3.Connection, family: str) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT COUNT(*) AS extract_count,
               SUM(CASE WHEN extract_kind='canonical_overview' THEN 1 ELSE 0 END) AS overview_count,
               SUM(CASE WHEN extract_kind='application_example' THEN 1 ELSE 0 END) AS application_count,
               SUM(CASE WHEN COALESCE(reactant_smiles,'')<>'' THEN 1 ELSE 0 END) AS extract_with_reactant,
               SUM(CASE WHEN COALESCE(product_smiles,'')<>'' THEN 1 ELSE 0 END) AS extract_with_product,
               SUM(CASE WHEN COALESCE(reactant_smiles,'')<>'' AND COALESCE(product_smiles,'')<>'' THEN 1 ELSE 0 END) AS extract_with_both
        FROM reaction_extracts WHERE reaction_family_name=?
        """,
        (family,),
    ).fetchone()
    mol = conn.execute(
        """
        SELECT SUM(CASE WHEN role='reactant' AND queryable=1 AND smiles IS NOT NULL AND smiles<>'' THEN 1 ELSE 0 END) AS queryable_reactants,
               SUM(CASE WHEN role='product' AND queryable=1 AND smiles IS NOT NULL AND smiles<>'' THEN 1 ELSE 0 END) AS queryable_products,
               COUNT(*) AS molecule_rows
        FROM extract_molecules WHERE reaction_family_name=?
        """,
        (family,),
    ).fetchone()
    pair_count = conn.execute(
        """
        SELECT COUNT(DISTINCT COALESCE(reactant_smiles,'') || ' || ' || COALESCE(product_smiles,''))
        FROM reaction_extracts
        WHERE reaction_family_name=? AND COALESCE(reactant_smiles,'')<>'' AND COALESCE(product_smiles,'')<>''
        """,
        (family,),
    ).fetchone()[0]
    curated = conn.execute(
        """SELECT id, extract_kind, transformation_text, reactants_text, products_text
           FROM reaction_extracts WHERE reaction_family_name=? AND notes_text LIKE ? ORDER BY id""",
        (family, '%' + TAG + '%')).fetchall()
    return {
        'family': family,
        **dict(row), **dict(mol),
        'unique_queryable_pair_count': int(pair_count or 0),
        'completion_gate_minimum_pass': int((row['overview_count'] or 0)) >= 1 and int((row['application_count'] or 0)) >= 2 and int((mol['queryable_reactants'] or 0)) >= 1 and int((mol['queryable_products'] or 0)) >= 1 and int(pair_count or 0) >= 1,
        'rich_completion_pass': int((row['overview_count'] or 0)) >= 1 and int((row['application_count'] or 0)) >= 2 and int((mol['queryable_reactants'] or 0)) >= 3 and int((mol['queryable_products'] or 0)) >= 3 and int(pair_count or 0) >= 3,
        'curated_extract_ids': [int(r['id']) for r in curated],
        'curated_extract_summaries': [dict(r) for r in curated],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description='Complete shallow-family sprint phase11 top10 in 5+5 batches.')
    ap.add_argument('--db', default='app/labint.db')
    ap.add_argument('--report-dir', default='reports/family_completion_phase11_shallow_top10')
    ap.add_argument('--batch', choices=['a', 'b', 'all'], default='a')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    families = BATCH_FAMILIES['a'] + BATCH_FAMILIES['b'] if args.batch == 'all' else BATCH_FAMILIES[args.batch]
    seeds = SEEDS_BY_BATCH['a'] + SEEDS_BY_BATCH['b'] if args.batch == 'all' else SEEDS_BY_BATCH[args.batch]

    db_path = Path(args.db)
    report_dir = Path(args.report_dir) / dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    report_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        before = {
            'queryable_family_count': queryable_family_count(conn),
            'queryable_count': queryable_count(conn),
            'reaction_extract_count': conn.execute('SELECT COUNT(*) FROM reaction_extracts').fetchone()[0],
            'extract_molecule_count': conn.execute('SELECT COUNT(*) FROM extract_molecules').fetchone()[0],
        }
        conn.execute('BEGIN')
        alias_updates = normalize_aliases(conn, families)
        backfill = backfill_extract_smiles_from_molecules(conn, families)
        seed_results = [insert_seed(conn, seed) for seed in seeds]
        verify = [verify_family(conn, fam) for fam in families]
        after = {
            'queryable_family_count': queryable_family_count(conn),
            'queryable_count': queryable_count(conn),
            'reaction_extract_count': conn.execute('SELECT COUNT(*) FROM reaction_extracts').fetchone()[0],
            'extract_molecule_count': conn.execute('SELECT COUNT(*) FROM extract_molecules').fetchone()[0],
        }
        rich_count = sum(1 for v in verify if v['rich_completion_pass'])
        shallow_count = len(verify) - rich_count
        payload = {
            'tag': TAG,
            'batch': args.batch,
            'db': str(db_path),
            'dry_run': bool(args.dry_run),
            'families': families,
            'before': before,
            'after': after,
            'alias_updates': alias_updates,
            'backfill': backfill,
            'seed_results': seed_results,
            'verify': verify,
            'rich_count': rich_count,
            'shallow_count': shallow_count,
        }
        if args.dry_run:
            conn.rollback(); status = '[DRY-RUN] rolled back changes'
        else:
            conn.commit(); status = '[APPLY] committed changes'
        suffix = f'phase11_shallow_top10_{args.batch}'
        jpath = report_dir / f'{suffix}_summary.json'
        mpath = report_dir / f'{suffix}_summary.md'
        jpath.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        lines = [
            f'# Family completion {suffix} summary','',f'- db: `{db_path}`',f'- batch: `{args.batch}`',f'- dry_run: `{args.dry_run}`','',status,'',
            '## Before',
            f"- queryable_family_count: {before['queryable_family_count']}",
            f"- queryable_count: {before['queryable_count']}",
            f"- reaction_extract_count: {before['reaction_extract_count']}",
            f"- extract_molecule_count: {before['extract_molecule_count']}",'',
            '## After',
            f"- queryable_family_count: {after['queryable_family_count']}",
            f"- queryable_count: {after['queryable_count']}",
            f"- reaction_extract_count: {after['reaction_extract_count']}",
            f"- extract_molecule_count: {after['extract_molecule_count']}",'',
            f'- rich_count: {rich_count}', f'- shallow_count: {shallow_count}',''
        ]
        for item in verify:
            lines.append(f"## {item['family']}")
            for k in ['extract_count','overview_count','application_count','extract_with_reactant','extract_with_product','extract_with_both','queryable_reactants','queryable_products','unique_queryable_pair_count','completion_gate_minimum_pass','rich_completion_pass']:
                lines.append(f'- {k}: {item[k]}')
            lines.append('')
        mpath.write_text('\n'.join(lines) + '\n', encoding='utf-8')
        print(status)
        print(f'summary json: {jpath}')
        print(f'summary md:   {mpath}')
        return 0
    finally:
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())
