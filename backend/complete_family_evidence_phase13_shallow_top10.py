
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

TAG = 'phase13_shallow_top10_completion_v2'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES = {'Glaser Reaction': 'Glaser Coupling', 'Hell Volhard Zelinsky Reaction': 'Hell-Volhard-Zelinsky Reaction', 'HVZ Reaction': 'Hell-Volhard-Zelinsky Reaction', 'Hantzsch Synthesis': 'Hantzsch Dihydropyridine Synthesis', 'Hetero Diels-Alder Cycloaddition': 'Hetero Diels-Alder Cycloaddition (HDA)', 'Hetero Diels Alder Cycloaddition': 'Hetero Diels-Alder Cycloaddition (HDA)', 'Hofmann Elimination Reaction': 'Hofmann Elimination', 'Hofmann Rearrangement Reaction': 'Hofmann Rearrangement', 'Horner-Wadsworth-Emmons': 'Horner-Wadsworth-Emmons Olefination', 'Horner-Wadsworth-Emmons Reaction': 'Horner-Wadsworth-Emmons Olefination'}
BATCH_FAMILIES = {'a': ['Glaser Coupling', 'Grignard Reaction', 'Heck Reaction', 'Hell-Volhard-Zelinsky Reaction', 'Henry Reaction'], 'b': ['Hantzsch Dihydropyridine Synthesis', 'Hetero Diels-Alder Cycloaddition (HDA)', 'Hofmann Elimination', 'Hofmann Rearrangement', 'Horner-Wadsworth-Emmons Olefination']}
SEEDS_BY_BATCH = {'a': [{'family': 'Glaser Coupling', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Glaser coupling of phenylacetylene to diphenylbutadiyne.', 'reactants_text': 'phenylacetylene', 'products_text': '1,4-diphenyl-1,3-butadiyne', 'reagents_text': 'Cu(I), oxygen', 'conditions_text': 'curated application-class seed representing oxidative homocoupling of terminal alkynes', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Glaser coupling chemistry. Adds a diaryl diyne example beyond the overview evidence. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'C#Cc1ccccc1', 'phenylacetylene', 'reactants_text', 1), ('product', 'c1ccccc1C#CC#Cc1ccccc1', '1,4-diphenyl-1,3-butadiyne', 'products_text', 1)]}, {'family': 'Glaser Coupling', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Glaser coupling of 4-ethynyltoluene to bis(4-methylphenyl)butadiyne.', 'reactants_text': '4-ethynyltoluene', 'products_text': '1,4-bis(4-methylphenyl)-1,3-butadiyne', 'reagents_text': 'Cu(I), oxygen', 'conditions_text': 'curated application-class seed adding a para-methyl aryl acetylene homocoupling example to the Glaser family', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Glaser coupling chemistry. Adds a para-tolyl diyne example for family depth. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'Cc1ccc(C#C)cc1', '4-ethynyltoluene', 'reactants_text', 1), ('product', 'Cc1ccc(C#CC#Cc2ccc(C)cc2)cc1', '1,4-bis(4-methylphenyl)-1,3-butadiyne', 'products_text', 1)]}, {'family': 'Glaser Coupling', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Glaser coupling of 1-hexyne to 5-decyne.', 'reactants_text': '1-hexyne', 'products_text': '5-decyne', 'reagents_text': 'Cu(I), oxygen', 'conditions_text': 'curated application-class seed adding an aliphatic terminal alkyne homocoupling example to the Glaser family', 'notes_text': 'Manual curated application-class seed added during the phase13 micro-hotfix v2 for Glaser coupling chemistry. Adds an aliphatic diyne example so unique queryable pairs reach rich completion. [phase13_shallow_top10_completion_v2]', 'molecules': [('reactant', 'CCCC#C', '1-hexyne', 'reactants_text', 1), ('product', 'CCCC#CC#CCCC', '5-decyne', 'products_text', 1)]}, {'family': 'Grignard Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Grignard addition of phenylmagnesium bromide to benzaldehyde giving benzhydrol.', 'reactants_text': 'benzaldehyde and phenylmagnesium bromide', 'products_text': 'benzhydrol', 'reagents_text': 'phenylmagnesium bromide', 'conditions_text': 'curated application-class seed representing nucleophilic addition of a Grignard reagent to an aldehyde', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Grignard chemistry. Provides a diaryl carbinol example beyond the overview evidence. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1), ('product', 'OC(c1ccccc1)c1ccccc1', 'benzhydrol', 'products_text', 1)]}, {'family': 'Grignard Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Grignard addition of methylmagnesium bromide to cyclohexanone giving 1-methylcyclohexanol.', 'reactants_text': 'cyclohexanone and methylmagnesium bromide', 'products_text': '1-methylcyclohexanol', 'reagents_text': 'methylmagnesium bromide', 'conditions_text': 'curated application-class seed adding an aliphatic ketone Grignard addition example', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Grignard chemistry. Adds a ketone-to-tertiary-alcohol example for family depth. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=C1CCCCC1', 'cyclohexanone', 'reactants_text', 1), ('product', 'CC1(O)CCCCC1', '1-methylcyclohexanol', 'products_text', 1)]}, {'family': 'Heck Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Heck coupling of iodobenzene with methyl acrylate to methyl cinnamate.', 'reactants_text': 'iodobenzene and methyl acrylate', 'products_text': 'methyl cinnamate', 'reagents_text': 'Pd catalyst, base', 'conditions_text': 'curated application-class seed representing aryl halide / alkene cross-coupling', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Heck chemistry. Provides a classic aryl halide / acrylate coupling example. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'Ic1ccccc1', 'iodobenzene', 'reactants_text', 1), ('reactant', 'C=CC(=O)OC', 'methyl acrylate', 'reactants_text', 1), ('product', 'COC(=O)C=Cc1ccccc1', 'methyl cinnamate', 'products_text', 1)]}, {'family': 'Heck Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Heck coupling of bromobenzene with styrene to stilbene.', 'reactants_text': 'bromobenzene and styrene', 'products_text': 'stilbene', 'reagents_text': 'Pd catalyst, base', 'conditions_text': 'curated application-class seed adding an aryl halide / styrene coupling example to the Heck family', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Heck chemistry. Adds a diaryl alkene example for family depth. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'Brc1ccccc1', 'bromobenzene', 'reactants_text', 1), ('reactant', 'C=Cc1ccccc1', 'styrene', 'reactants_text', 1), ('product', 'c1ccc(/C=C/c2ccccc2)cc1', 'stilbene', 'products_text', 1)]}, {'family': 'Hell-Volhard-Zelinsky Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Hell-Volhard-Zelinsky bromination of propionic acid to 2-bromopropionic acid.', 'reactants_text': 'propionic acid', 'products_text': '2-bromopropionic acid', 'reagents_text': 'Br2, PBr3', 'conditions_text': 'curated application-class seed representing alpha-halogenation of a carboxylic acid', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Hell-Volhard-Zelinsky chemistry. Provides a simple alpha-bromination example beyond the overview evidence. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CCC(=O)O', 'propionic acid', 'reactants_text', 1), ('product', 'CC(Br)C(=O)O', '2-bromopropionic acid', 'products_text', 1)]}, {'family': 'Hell-Volhard-Zelinsky Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Hell-Volhard-Zelinsky bromination of butyric acid to 2-bromobutyric acid.', 'reactants_text': 'butyric acid', 'products_text': '2-bromobutyric acid', 'reagents_text': 'Br2, PBr3', 'conditions_text': 'curated application-class seed adding a second alpha-bromocarboxylic acid example to the HVZ family', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Hell-Volhard-Zelinsky chemistry. Adds a higher homolog example for family depth. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CCCC(=O)O', 'butyric acid', 'reactants_text', 1), ('product', 'CCC(Br)C(=O)O', '2-bromobutyric acid', 'products_text', 1)]}, {'family': 'Hell-Volhard-Zelinsky Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Hell-Volhard-Zelinsky bromination of pentanoic acid to 2-bromopentanoic acid.', 'reactants_text': 'pentanoic acid', 'products_text': '2-bromopentanoic acid', 'reagents_text': 'Br2, PBr3', 'conditions_text': 'curated application-class seed adding a third alpha-bromocarboxylic acid example to the HVZ family', 'notes_text': 'Manual curated application-class seed added during the phase13 micro-hotfix v2 for Hell-Volhard-Zelinsky chemistry. Adds a pentanoic acid homolog so unique queryable pairs reach rich completion. [phase13_shallow_top10_completion_v2]', 'molecules': [('reactant', 'CCCC(=O)O', 'pentanoic acid', 'reactants_text', 1), ('product', 'CCCC(Br)C(=O)O', '2-bromopentanoic acid', 'products_text', 1)]}, {'family': 'Henry Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Henry reaction of benzaldehyde with nitromethane to 2-nitro-1-phenylethanol.', 'reactants_text': 'benzaldehyde and nitromethane', 'products_text': '2-nitro-1-phenylethanol', 'reagents_text': 'base', 'conditions_text': 'curated application-class seed representing nitroaldol addition to an aromatic aldehyde', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Henry reaction chemistry. Provides a benzylic nitro alcohol example beyond the overview evidence. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1), ('reactant', 'C[N+](=O)[O-]', 'nitromethane', 'reactants_text', 1), ('product', 'OCC([N+](=O)[O-])c1ccccc1', '2-nitro-1-phenylethanol', 'products_text', 1)]}, {'family': 'Henry Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Henry reaction of anisaldehyde with nitroethane to a beta-nitro alcohol.', 'reactants_text': '4-anisaldehyde and nitroethane', 'products_text': '1-(4-methoxyphenyl)-2-nitropropan-1-ol', 'reagents_text': 'base', 'conditions_text': 'curated application-class seed adding an alkyl nitroalkane example to the Henry family', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Henry reaction chemistry. Adds a para-methoxy aryl example for family depth. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'COc1ccc(C=O)cc1', '4-anisaldehyde', 'reactants_text', 1), ('reactant', 'CC[N+](=O)[O-]', 'nitroethane', 'reactants_text', 1), ('product', 'CC([N+](=O)[O-])C(O)c1ccc(OC)cc1', '1-(4-methoxyphenyl)-2-nitropropan-1-ol', 'products_text', 1)]}], 'b': [{'family': 'Hantzsch Dihydropyridine Synthesis', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Hantzsch synthesis from ethyl acetoacetate, formaldehyde, and ammonia to a dihydropyridine diester.', 'reactants_text': 'ethyl acetoacetate, formaldehyde, and ammonia', 'products_text': 'diethyl 2,6-dimethyl-1,4-dihydropyridine-3,5-dicarboxylate', 'reagents_text': 'ammonia', 'conditions_text': 'curated application-class seed representing the multicomponent Hantzsch dihydropyridine synthesis', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Hantzsch dihydropyridine synthesis chemistry. Provides a classic Hantzsch ester example beyond the overview evidence. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CCOC(=O)CC(C)=O', 'ethyl acetoacetate', 'reactants_text', 1), ('reactant', 'C=O', 'formaldehyde', 'reactants_text', 1), ('reactant', 'N', 'ammonia', 'reactants_text', 1), ('product', 'CCOC(=O)C1=C(C)NC(C)=C(C(=O)OCC)C1', 'Hantzsch ester diethyl dihydropyridine', 'products_text', 1)]}, {'family': 'Hantzsch Dihydropyridine Synthesis', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Hantzsch synthesis from methyl acetoacetate, formaldehyde, and ammonia to the dimethyl Hantzsch ester.', 'reactants_text': 'methyl acetoacetate, formaldehyde, and ammonia', 'products_text': 'dimethyl 2,6-dimethyl-1,4-dihydropyridine-3,5-dicarboxylate', 'reagents_text': 'ammonia', 'conditions_text': 'curated application-class seed adding a methyl ester Hantzsch example for family depth', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Hantzsch dihydropyridine synthesis chemistry. Adds a dimethyl ester variant for family depth. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'COC(=O)CC(C)=O', 'methyl acetoacetate', 'reactants_text', 1), ('reactant', 'C=O', 'formaldehyde', 'reactants_text', 1), ('reactant', 'N', 'ammonia', 'reactants_text', 1), ('product', 'COC(=O)C1=C(C)NC(C)=C(C(=O)OC)C1', 'dimethyl Hantzsch ester', 'products_text', 1)]}, {'family': 'Hetero Diels-Alder Cycloaddition (HDA)', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Hetero Diels-Alder cycloaddition of butadiene with formaldehyde to dihydropyran.', 'reactants_text': '1,3-butadiene and formaldehyde', 'products_text': '3,4-dihydro-2H-pyran', 'reagents_text': 'thermal cycloaddition', 'conditions_text': 'curated application-class seed representing oxadiene/aldehyde hetero Diels-Alder cycloaddition', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for hetero Diels-Alder chemistry. Provides a simple oxygen-containing cycloadduct example beyond the overview evidence. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'C=CC=C', '1,3-butadiene', 'reactants_text', 1), ('reactant', 'C=O', 'formaldehyde', 'reactants_text', 1), ('product', 'C1=CCCOC1', '3,4-dihydro-2H-pyran', 'products_text', 1)]}, {'family': 'Hetero Diels-Alder Cycloaddition (HDA)', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Hetero Diels-Alder cycloaddition of isoprene with formaldehyde to methyl-substituted dihydropyran.', 'reactants_text': 'isoprene and formaldehyde', 'products_text': '2-methyl-3,4-dihydro-2H-pyran', 'reagents_text': 'thermal cycloaddition', 'conditions_text': 'curated application-class seed adding a substituted hetero Diels-Alder example', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for hetero Diels-Alder chemistry. Adds a methyl-substituted oxygen heterocycle example for family depth. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CC(=C)C=C', 'isoprene', 'reactants_text', 1), ('reactant', 'C=O', 'formaldehyde', 'reactants_text', 1), ('product', 'CC1=CCCOC1', '2-methyl-3,4-dihydro-2H-pyran', 'products_text', 1)]}, {'family': 'Hofmann Elimination', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Hofmann elimination of butyltrimethylammonium hydroxide to 1-butene.', 'reactants_text': 'butyltrimethylammonium substrate', 'products_text': '1-butene', 'reagents_text': 'heat, hydroxide', 'conditions_text': 'curated application-class seed representing elimination from a quaternary ammonium substrate', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Hofmann elimination chemistry. Provides a terminal alkene-forming example beyond the overview evidence. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CCCC[N+](C)(C)C', 'butyltrimethylammonium substrate', 'reactants_text', 1), ('product', 'CCC=C', '1-butene', 'products_text', 1)]}, {'family': 'Hofmann Elimination', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Hofmann elimination of cyclohexyltrimethylammonium substrate to cyclohexene.', 'reactants_text': 'cyclohexyltrimethylammonium substrate', 'products_text': 'cyclohexene', 'reagents_text': 'heat, hydroxide', 'conditions_text': 'curated application-class seed adding a cyclic ammonium elimination example to the Hofmann elimination family', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Hofmann elimination chemistry. Adds a cyclic alkene example for family depth. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'C[N+](C)(C)C1CCCCC1', 'cyclohexyltrimethylammonium substrate', 'reactants_text', 1), ('product', 'C1=CCCCC1', 'cyclohexene', 'products_text', 1)]}, {'family': 'Hofmann Rearrangement', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Hofmann rearrangement of benzamide to aniline.', 'reactants_text': 'benzamide', 'products_text': 'aniline', 'reagents_text': 'Br2, base', 'conditions_text': 'curated application-class seed representing one-carbon-shortening amide-to-amine conversion', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Hofmann rearrangement chemistry. Provides a classic benzamide example beyond the overview evidence. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'NC(=O)c1ccccc1', 'benzamide', 'reactants_text', 1), ('product', 'Nc1ccccc1', 'aniline', 'products_text', 1)]}, {'family': 'Hofmann Rearrangement', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Hofmann rearrangement of cyclohexanecarboxamide to cyclohexylamine.', 'reactants_text': 'cyclohexanecarboxamide', 'products_text': 'cyclohexylamine', 'reagents_text': 'Br2, base', 'conditions_text': 'curated application-class seed adding an aliphatic amide example to the Hofmann rearrangement family', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Hofmann rearrangement chemistry. Adds a cycloalkyl amine example for family depth. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'NC(=O)C1CCCCC1', 'cyclohexanecarboxamide', 'reactants_text', 1), ('product', 'NC1CCCCC1', 'cyclohexylamine', 'products_text', 1)]}, {'family': 'Hofmann Rearrangement', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Hofmann rearrangement of propionamide to ethylamine.', 'reactants_text': 'propionamide', 'products_text': 'ethylamine', 'reagents_text': 'Br2, base', 'conditions_text': 'curated application-class seed adding a simple aliphatic amide example to ensure pair diversity in the Hofmann rearrangement family', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Hofmann rearrangement chemistry. Adds a small aliphatic amine example for pair diversity. [phase13_shallow_top10_completion_v1_hotfix]', 'molecules': [('reactant', 'CCC(=O)N', 'propionamide', 'reactants_text', 1), ('product', 'CCN', 'ethylamine', 'products_text', 1)]}, {'family': 'Horner-Wadsworth-Emmons Olefination', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Horner-Wadsworth-Emmons olefination of benzaldehyde with triethyl phosphonoacetate to ethyl cinnamate.', 'reactants_text': 'benzaldehyde and triethyl phosphonoacetate', 'products_text': 'ethyl cinnamate', 'reagents_text': 'base', 'conditions_text': 'curated application-class seed representing phosphonate olefination of an aldehyde', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Horner-Wadsworth-Emmons chemistry. Provides a classic alpha,beta-unsaturated ester example beyond the overview evidence. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1), ('reactant', 'CCOP(=O)(CC(=O)OCC)OCC', 'triethyl phosphonoacetate', 'reactants_text', 1), ('product', 'CCOC(=O)C=Cc1ccccc1', 'ethyl cinnamate', 'products_text', 1)]}, {'family': 'Horner-Wadsworth-Emmons Olefination', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Horner-Wadsworth-Emmons olefination of anisaldehyde with triethyl phosphonoacetate to ethyl 4-methoxycinnamate.', 'reactants_text': '4-anisaldehyde and triethyl phosphonoacetate', 'products_text': 'ethyl 4-methoxycinnamate', 'reagents_text': 'base', 'conditions_text': 'curated application-class seed adding an anisaldehyde phosphonate olefination example to the HWE family', 'notes_text': 'Manual curated application-class seed added during the phase13 5+5 shallow-family completion sprint for Horner-Wadsworth-Emmons chemistry. Adds a para-methoxy aryl example for family depth. [phase13_shallow_top10_completion_v1]', 'molecules': [('reactant', 'COc1ccc(C=O)cc1', '4-anisaldehyde', 'reactants_text', 1), ('reactant', 'CCOP(=O)(CC(=O)OCC)OCC', 'triethyl phosphonoacetate', 'reactants_text', 1), ('product', 'CCOC(=O)C=Cc1ccc(OC)cc1', 'ethyl 4-methoxycinnamate', 'products_text', 1)]}]}


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
    out=[]
    target_set=set(families)
    for alias, official in DATA_ALIASES.items():
        if official not in target_set:
            continue
        fam_norm=family_norm(official)
        cur=conn.execute('UPDATE reaction_extracts SET reaction_family_name=?, reaction_family_name_norm=?, updated_at=? WHERE reaction_family_name=?',(official,fam_norm,NOW,alias))
        out.append({'table':'reaction_extracts','from':alias,'to':official,'rows_updated':cur.rowcount})
        cur=conn.execute('UPDATE extract_molecules SET reaction_family_name=? WHERE reaction_family_name=?',(official,alias))
        out.append({'table':'extract_molecules','from':alias,'to':official,'rows_updated':cur.rowcount})
    return out


def backfill_extract_smiles_from_molecules(conn: sqlite3.Connection, families: Sequence[str]) -> List[Dict[str, Any]]:
    out=[]
    for family in families:
        rows=conn.execute("""
            SELECT re.id,
                   GROUP_CONCAT(CASE WHEN em.role='reactant' AND em.queryable=1 AND em.smiles IS NOT NULL THEN em.smiles END, ' | ') AS reactant_smiles,
                   GROUP_CONCAT(CASE WHEN em.role='product' AND em.queryable=1 AND em.smiles IS NOT NULL THEN em.smiles END, ' | ') AS product_smiles
            FROM reaction_extracts re
            LEFT JOIN extract_molecules em ON em.extract_id = re.id
            WHERE re.reaction_family_name = ?
            GROUP BY re.id
        """,(family,)).fetchall()
        updated=0
        for extract_id, reactant, product in rows:
            cur=conn.execute("""
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
            """,(reactant,product,reactant,product,NOW,extract_id))
            updated += cur.rowcount
        out.append({'family':family,'extract_rows_touched':updated})
    return out


def find_anchor(conn: sqlite3.Connection, seed: Dict[str, Any]) -> Tuple[int,int,str]:
    row=conn.execute("""
        SELECT re.scheme_candidate_id, COALESCE(pi.page_no, 0) AS page_no, COALESCE(pi.image_filename, ?) AS image_filename
        FROM reaction_extracts re
        LEFT JOIN scheme_candidates sc ON sc.id = re.scheme_candidate_id
        LEFT JOIN page_images pi ON pi.id = sc.page_image_id
        WHERE re.reaction_family_name=? AND re.scheme_candidate_id IS NOT NULL
        ORDER BY CASE WHEN re.extract_kind='canonical_overview' THEN 0 WHEN re.extract_kind='application_example' THEN 1 ELSE 2 END, re.id
        LIMIT 1
    """,(seed.get('anchor_image','phase13_anchor.jpg'), seed['family'])).fetchone()
    if row and row[0] is not None:
        return int(row[0]), int(row[1] or 0), row[2]
    raise RuntimeError(f"Could not find usable anchor for {seed['family']}")


def existing_seed_extract(conn: sqlite3.Connection, family: str, notes_text: str) -> Optional[int]:
    row=conn.execute('SELECT id FROM reaction_extracts WHERE reaction_family_name=? AND notes_text=? LIMIT 1',(family, notes_text)).fetchone()
    return row[0] if row else None


def insert_seed(conn: sqlite3.Connection, seed: Dict[str, Any]) -> Dict[str, Any]:
    fam=seed['family']
    fam_norm=family_norm(fam)
    existing=existing_seed_extract(conn,fam,seed['notes_text'])
    anchor_id,page_no,image_filename=find_anchor(conn,seed)
    if existing:
        return {'family':fam,'status':'skipped_existing','extract_id':existing,'page_no':page_no,'image_filename':image_filename,'inserted_molecules':0}
    reactants_for_extract=[]; products_for_extract=[]
    for role, smiles, _name, _source_field, queryable in seed['molecules']:
        if queryable != 1 or smiles is None:
            continue
        cs=canon(smiles)
        if role=='reactant': reactants_for_extract.append(cs)
        elif role=='product': products_for_extract.append(cs)
    reactant_smiles=' | '.join(sorted(set(reactants_for_extract))) if reactants_for_extract else None
    product_smiles=' | '.join(sorted(set(products_for_extract))) if products_for_extract else None
    cur=conn.execute("""
        INSERT INTO reaction_extracts (
            scheme_candidate_id, reaction_family_name, reaction_family_name_norm, extract_kind,
            transformation_text, reactants_text, products_text, intermediates_text, reagents_text,
            conditions_text, notes_text, reactant_smiles, product_smiles,
            smiles_confidence, extraction_confidence, parse_status, promote_decision,
            extractor_model, extractor_prompt_version, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,(
        anchor_id, fam, fam_norm, seed['extract_kind'], seed.get('transformation_text'),
        seed.get('reactants_text'), seed.get('products_text'), seed.get('intermediates_text'),
        seed.get('reagents_text'), seed.get('conditions_text'), seed.get('notes_text'),
        reactant_smiles, product_smiles, 0.98 if (reactant_smiles or product_smiles) else 0.0,
        0.98, 'curated', 'promote', 'manual_curated_seed', TAG, NOW, NOW,
    ))
    extract_id=cur.lastrowid
    inserted=0
    for role, smiles, name, source_field, queryable in seed['molecules']:
        csmiles=canon(smiles) if smiles else None
        conn.execute("""
            INSERT INTO extract_molecules (
                extract_id, role, smiles, smiles_kind, quality_tier, reaction_family_name,
                source_zip, page_no, queryable, note_text, morgan_fp, normalized_text,
                source_field, structure_source, fg_tags, role_confidence, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,(
            extract_id, role, csmiles, 'canonical', 1 if queryable and csmiles else 3, fam,
            'named reactions.pdf', page_no, 1 if queryable and csmiles else 0,
            f'[{TAG}: {name}]' if name else f'[{TAG}]', fp_blob(csmiles) if (queryable and csmiles) else None,
            (name.lower() if name else None), source_field, TAG if csmiles else None, None,
            0.98 if csmiles else 0.0, NOW,
        ))
        inserted += 1
    return {'family':fam,'status':'inserted','extract_id':extract_id,'page_no':page_no,'image_filename':image_filename,'inserted_molecules':inserted}


def verify_family(conn: sqlite3.Connection, family: str) -> Dict[str, Any]:
    row=conn.execute("""
        SELECT COUNT(*) AS extract_count,
               SUM(CASE WHEN extract_kind='canonical_overview' THEN 1 ELSE 0 END) AS overview_count,
               SUM(CASE WHEN extract_kind='application_example' THEN 1 ELSE 0 END) AS application_count,
               SUM(CASE WHEN COALESCE(reactant_smiles,'')<>'' THEN 1 ELSE 0 END) AS extract_with_reactant,
               SUM(CASE WHEN COALESCE(product_smiles,'')<>'' THEN 1 ELSE 0 END) AS extract_with_product,
               SUM(CASE WHEN COALESCE(reactant_smiles,'')<>'' AND COALESCE(product_smiles,'')<>'' THEN 1 ELSE 0 END) AS extract_with_both
        FROM reaction_extracts WHERE reaction_family_name=?
    """,(family,)).fetchone()
    mol=conn.execute("""
        SELECT SUM(CASE WHEN role='reactant' AND queryable=1 AND smiles IS NOT NULL AND smiles<>'' THEN 1 ELSE 0 END) AS queryable_reactants,
               SUM(CASE WHEN role='product' AND queryable=1 AND smiles IS NOT NULL AND smiles<>'' THEN 1 ELSE 0 END) AS queryable_products,
               COUNT(*) AS molecule_rows
        FROM extract_molecules WHERE reaction_family_name=?
    """,(family,)).fetchone()
    pair_count=conn.execute("""
        SELECT COUNT(DISTINCT COALESCE(reactant_smiles,'') || ' || ' || COALESCE(product_smiles,''))
        FROM reaction_extracts
        WHERE reaction_family_name=?
          AND COALESCE(reactant_smiles,'')<>''
          AND COALESCE(product_smiles,'')<>''
    """,(family,)).fetchone()[0]
    return {'family':family, **dict(row), **dict(mol), 'unique_queryable_pair_count': int(pair_count or 0), 'completion_minimum_pass': int((row['overview_count'] or 0))>=1 and int((row['application_count'] or 0))>=2 and int((mol['queryable_reactants'] or 0))>=1 and int((mol['queryable_products'] or 0))>=1 and int(pair_count or 0)>=1, 'rich_completion_pass': int((row['overview_count'] or 0))>=1 and int((row['application_count'] or 0))>=2 and int((mol['queryable_reactants'] or 0))>=3 and int((mol['queryable_products'] or 0))>=3 and int(pair_count or 0)>=3}


def main() -> int:
    ap=argparse.ArgumentParser(description='Complete phase13 shallow top10 in 5+5 batches.')
    ap.add_argument('--db', default='app/labint.db')
    ap.add_argument('--report-dir', default='reports/family_completion_phase13_shallow_top10')
    ap.add_argument('--batch', choices=['a','b','all'], default='all')
    ap.add_argument('--dry-run', action='store_true')
    args=ap.parse_args()
    db_path=Path(args.db)
    report_dir=Path(args.report_dir)/dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    report_dir.mkdir(parents=True, exist_ok=True)
    batches=['a','b'] if args.batch=='all' else [args.batch]
    conn=sqlite3.connect(str(db_path)); conn.row_factory=sqlite3.Row
    try:
        before_families=queryable_family_count(conn)
        before_queryable=queryable_count(conn)
        all_results=[]; all_alias=[]; all_backfill=[]; verify=[]; families=[]
        for batch in batches:
            fams=BATCH_FAMILIES[batch]; families.extend(fams)
            all_alias.extend(normalize_aliases(conn, fams))
            all_backfill.extend(backfill_extract_smiles_from_molecules(conn, fams))
            all_results.extend([insert_seed(conn, seed) for seed in SEEDS_BY_BATCH[batch]])
        for fam in families:
            verify.append(verify_family(conn, fam))
        after_families=queryable_family_count(conn)
        after_queryable=queryable_count(conn)
        payload={'tag':TAG,'db':str(db_path),'batch':args.batch,'dry_run':bool(args.dry_run),'before':{'queryable_family_count':before_families,'queryable_count':before_queryable},'after':{'queryable_family_count':after_families,'queryable_count':after_queryable},'alias_updates':all_alias,'backfill_updates':all_backfill,'seed_results':all_results,'verify':verify}
        suffix=f'phase13_shallow_top10_{args.batch}'
        jpath=report_dir/f'{suffix}_summary.json'
        mpath=report_dir/f'{suffix}_summary.md'
        jpath.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8')
        lines=[f'# Family completion {suffix} summary','',f'- db: `{db_path}`',f'- dry_run: `{args.dry_run}`',f'- queryable_family_count: {before_families} → {after_families}',f'- queryable_count: {before_queryable} → {after_queryable}','']
        for item in verify:
            lines.append(f"## {item['family']}")
            for k in ['extract_count','overview_count','application_count','extract_with_reactant','extract_with_product','extract_with_both','queryable_reactants','queryable_products','unique_queryable_pair_count','completion_minimum_pass','rich_completion_pass']:
                lines.append(f'- {k}: {item[k]}')
            lines.append('')
        mpath.write_text('\n'.join(lines)+'\n', encoding='utf-8')
        if args.dry_run:
            conn.rollback(); print('[DRY-RUN] rolled back changes')
        else:
            conn.commit(); print('[APPLY] committed changes')
        print(f'summary json: {jpath}')
        print(f'summary md:   {mpath}')
        return 0
    finally:
        conn.close()

if __name__=='__main__':
    raise SystemExit(main())
