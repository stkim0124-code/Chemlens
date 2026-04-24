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

TAG = 'phase15_shallow_top10_completion_hotfix_v4'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES = {'Krapcho Dealkoxycarbonylation': 'Krapcho Dealkoxycarbonylation (Krapcho Reaction)', 'Kumada Coupling': 'Kumada Cross-Coupling', 'Kumada Cross Coupling': 'Kumada Cross-Coupling', 'Haloform Reaction': 'Lieben Haloform Reaction', 'Luche Reaction': 'Luche Reduction', 'Ley Oxidation of Alcohols': 'Ley Oxidation'}
BATCH_FAMILIES = {'a': ['Knoevenagel Condensation', 'Knorr Pyrrole Synthesis', 'Kornblum Oxidation', 'Krapcho Dealkoxycarbonylation (Krapcho Reaction)', 'Kumada Cross-Coupling'], 'b': ['Ley Oxidation', 'Lieben Haloform Reaction', 'Lossen Rearrangement', 'Luche Reduction', 'Mannich Reaction']}
SEEDS_BY_BATCH = {'a': [{'family': 'Knoevenagel Condensation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Knoevenagel condensation of benzaldehyde with malononitrile to benzylidenemalononitrile.', 'reactants_text': 'benzaldehyde and malononitrile', 'products_text': 'benzylidenemalononitrile', 'reagents_text': 'base catalyst', 'conditions_text': 'curated application-class seed representing aldehyde / active methylene condensation', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Knoevenagel condensation chemistry. Adds a benzaldehyde/malononitrile example beyond the overview evidence. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1), ('reactant', 'N#CC#N', 'malononitrile', 'reactants_text', 1), ('product', 'N#CC(C#N)=Cc1ccccc1', 'benzylidenemalononitrile', 'products_text', 1)]}, {'family': 'Knoevenagel Condensation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Knoevenagel condensation of 4-anisaldehyde with malononitrile to 4-methoxybenzylidenemalononitrile.', 'reactants_text': '4-anisaldehyde and malononitrile', 'products_text': '4-methoxybenzylidenemalononitrile', 'reagents_text': 'base catalyst', 'conditions_text': 'curated application-class seed adding an electron-rich aryl aldehyde example', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Knoevenagel condensation chemistry. Adds an anisaldehyde example for family depth. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'COc1ccc(C=O)cc1', '4-anisaldehyde', 'reactants_text', 1), ('reactant', 'N#CC#N', 'malononitrile', 'reactants_text', 1), ('product', 'COc1ccc(C=C(C#N)C#N)cc1', '4-methoxybenzylidenemalononitrile', 'products_text', 1)]}, {'family': 'Knorr Pyrrole Synthesis', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Knorr pyrrole synthesis of 2,5-dimethylpyrrole from a 1,4-dicarbonyl equivalent and ammonia.', 'reactants_text': 'hexane-2,5-dione and ammonia equivalent', 'products_text': '2,5-dimethylpyrrole', 'reagents_text': 'ammonia or ammonium acetate', 'conditions_text': 'curated application-class seed representing Paal-Knorr/Knorr-type pyrrole formation from a diketone', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Knorr pyrrole synthesis chemistry. Adds a simple diketone-to-pyrrole example beyond the overview evidence. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CC(=O)CC(C)=O', 'hexane-2,5-dione', 'reactants_text', 1), ('product', 'Cc1c[nH]c(C)c1', '2,5-dimethylpyrrole', 'products_text', 1)]}, {'family': 'Knorr Pyrrole Synthesis', 'extract_kind': 'application_example', 'transformation_text': 'Application example: N-methyl Knorr pyrrole synthesis of 1,2,5-trimethylpyrrole from a 1,4-dicarbonyl equivalent and methylamine.', 'reactants_text': 'hexane-2,5-dione and methylamine equivalent', 'products_text': '1,2,5-trimethylpyrrole', 'reagents_text': 'methylamine', 'conditions_text': 'curated application-class seed adding an N-substituted pyrrole example', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Knorr pyrrole synthesis chemistry. Adds an N-methyl pyrrole example for family depth. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CC(=O)CC(C)=O', 'hexane-2,5-dione', 'reactants_text', 1), ('product', 'Cc1cc(C)n(C)c1', '1,2,5-trimethylpyrrole', 'products_text', 1)]}, {'family': 'Kornblum Oxidation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Kornblum oxidation of benzyl bromide to benzaldehyde.', 'reactants_text': 'benzyl bromide', 'products_text': 'benzaldehyde', 'reagents_text': 'DMSO, base', 'conditions_text': 'curated application-class seed representing benzylic halide oxidation to aldehyde', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Kornblum oxidation chemistry. Adds a benzyl bromide oxidation example beyond the overview evidence. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'BrCc1ccccc1', 'benzyl bromide', 'reactants_text', 1), ('product', 'O=Cc1ccccc1', 'benzaldehyde', 'products_text', 1)]}, {'family': 'Kornblum Oxidation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Kornblum oxidation of 4-methoxybenzyl bromide to 4-anisaldehyde.', 'reactants_text': '4-methoxybenzyl bromide', 'products_text': '4-anisaldehyde', 'reagents_text': 'DMSO, base', 'conditions_text': 'curated application-class seed adding an anisyl benzylic halide oxidation example', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Kornblum oxidation chemistry. Adds an anisyl benzylic bromide example for family depth. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'COc1ccc(CBr)cc1', '4-methoxybenzyl bromide', 'reactants_text', 1), ('product', 'COc1ccc(C=O)cc1', '4-anisaldehyde', 'products_text', 1)]}, {'family': 'Kornblum Oxidation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Kornblum oxidation of 4-nitrobenzyl bromide to 4-nitrobenzaldehyde.', 'reactants_text': '4-nitrobenzyl bromide', 'products_text': '4-nitrobenzaldehyde', 'reagents_text': 'DMSO, base', 'conditions_text': 'curated hotfix seed adding a third distinct benzylic halide oxidation example', 'notes_text': 'Manual curated hotfix seed added during the phase15 shallow-family completion hotfix for Kornblum oxidation chemistry. Adds a 4-nitrobenzyl bromide example so the family reaches rich pair count. [phase15_shallow_top10_completion_hotfix_v4]', 'molecules': [('reactant', 'O=[N+]([O-])c1ccc(CBr)cc1', '4-nitrobenzyl bromide', 'reactants_text', 1), ('product', 'O=[N+]([O-])c1ccc(C=O)cc1', '4-nitrobenzaldehyde', 'products_text', 1)]}, {'family': 'Krapcho Dealkoxycarbonylation (Krapcho Reaction)', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Krapcho dealkoxycarbonylation of diethyl 2-benzylmalonate to ethyl 3-phenylpropionate.', 'reactants_text': 'diethyl 2-benzylmalonate', 'products_text': 'ethyl 3-phenylpropionate', 'reagents_text': 'LiCl, DMSO/water, heat', 'conditions_text': 'curated application-class seed representing dealkoxycarbonylation of a malonate derivative', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Krapcho dealkoxycarbonylation chemistry. Adds a benzyl-substituted malonate example beyond the overview evidence. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CCOC(=O)C(Cc1ccccc1)C(=O)OCC', 'diethyl 2-benzylmalonate', 'reactants_text', 1), ('product', 'CCOC(=O)CCc1ccccc1', 'ethyl 3-phenylpropionate', 'products_text', 1)]}, {'family': 'Krapcho Dealkoxycarbonylation (Krapcho Reaction)', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Krapcho dealkoxycarbonylation of diethyl 2-methylmalonate to ethyl propionate.', 'reactants_text': 'diethyl 2-methylmalonate', 'products_text': 'ethyl propionate', 'reagents_text': 'LiCl, DMSO/water, heat', 'conditions_text': 'curated application-class seed adding a simple alkyl malonate example', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Krapcho dealkoxycarbonylation chemistry. Adds a simple alkyl malonate example for family depth. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CCOC(=O)C(C)C(=O)OCC', 'diethyl 2-methylmalonate', 'reactants_text', 1), ('product', 'CCOC(=O)CC', 'ethyl propionate', 'products_text', 1)]}, {'family': 'Kumada Cross-Coupling', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Kumada cross-coupling of bromobenzene with ethylmagnesium bromide to ethylbenzene.', 'reactants_text': 'bromobenzene and ethylmagnesium bromide equivalent', 'products_text': 'ethylbenzene', 'reagents_text': 'Ni or Pd catalyst', 'conditions_text': 'curated application-class seed representing aryl halide / Grignard cross-coupling', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Kumada cross-coupling chemistry. Adds a simple aryl-alkyl coupling example beyond the overview evidence. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'Brc1ccccc1', 'bromobenzene', 'reactants_text', 1), ('product', 'CCc1ccccc1', 'ethylbenzene', 'products_text', 1)]}, {'family': 'Kumada Cross-Coupling', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Kumada cross-coupling of 4-bromoanisole with phenylmagnesium bromide to 4-methoxybiphenyl.', 'reactants_text': '4-bromoanisole and phenylmagnesium bromide equivalent', 'products_text': '4-methoxybiphenyl', 'reagents_text': 'Ni or Pd catalyst', 'conditions_text': 'curated application-class seed adding a biaryl Kumada example', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Kumada cross-coupling chemistry. Adds an anisyl biaryl example for family depth. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'COc1ccc(Br)cc1', '4-bromoanisole', 'reactants_text', 1), ('product', 'COc1ccc(-c2ccccc2)cc1', '4-methoxybiphenyl', 'products_text', 1)]}], 'b': [{'family': 'Ley Oxidation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Ley oxidation of benzyl alcohol to benzaldehyde.', 'reactants_text': 'benzyl alcohol', 'products_text': 'benzaldehyde', 'reagents_text': 'TPAP/NMO', 'conditions_text': 'curated application-class seed representing mild oxidation of a benzylic alcohol', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Ley oxidation chemistry. Adds a benzyl alcohol oxidation example beyond the overview evidence. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'OCc1ccccc1', 'benzyl alcohol', 'reactants_text', 1), ('product', 'O=Cc1ccccc1', 'benzaldehyde', 'products_text', 1)]}, {'family': 'Ley Oxidation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Ley oxidation of cyclohexanol to cyclohexanone.', 'reactants_text': 'cyclohexanol', 'products_text': 'cyclohexanone', 'reagents_text': 'TPAP/NMO', 'conditions_text': 'curated application-class seed adding a cyclic secondary alcohol oxidation example', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Ley oxidation chemistry. Adds a cyclic secondary alcohol example for family depth. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'OC1CCCCC1', 'cyclohexanol', 'reactants_text', 1), ('product', 'O=C1CCCCC1', 'cyclohexanone', 'products_text', 1)]}, {'family': 'Ley Oxidation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Ley oxidation of 4-methoxybenzyl alcohol to 4-anisaldehyde.', 'reactants_text': '4-methoxybenzyl alcohol', 'products_text': '4-anisaldehyde', 'reagents_text': 'TPAP/NMO', 'conditions_text': 'curated hotfix seed adding a third distinct Ley oxidation example', 'notes_text': 'Manual curated hotfix seed added during the phase15 shallow-family completion hotfix for Ley oxidation chemistry. Adds an anisyl alcohol oxidation example so the family reaches rich pair count. [phase15_shallow_top10_completion_hotfix_v4]', 'molecules': [('reactant', 'COc1ccc(CO)cc1', '4-methoxybenzyl alcohol', 'reactants_text', 1), ('product', 'COc1ccc(C=O)cc1', '4-anisaldehyde', 'products_text', 1)]}, {'family': 'Lieben Haloform Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Lieben haloform reaction of acetophenone to benzoic acid.', 'reactants_text': 'acetophenone', 'products_text': 'benzoic acid', 'reagents_text': 'halogen, base', 'conditions_text': 'curated application-class seed representing cleavage of a methyl ketone to a carboxylic acid', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Lieben haloform chemistry. Adds an acetophenone cleavage example beyond the overview evidence. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CC(=O)c1ccccc1', 'acetophenone', 'reactants_text', 1), ('product', 'O=C(O)c1ccccc1', 'benzoic acid', 'products_text', 1)]}, {'family': 'Lieben Haloform Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Lieben haloform reaction of 2-butanone to propionic acid.', 'reactants_text': '2-butanone', 'products_text': 'propionic acid', 'reagents_text': 'halogen, base', 'conditions_text': 'curated application-class seed adding an aliphatic methyl ketone cleavage example', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Lieben haloform chemistry. Adds an aliphatic example for family depth. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CCC(C)=O', '2-butanone', 'reactants_text', 1), ('product', 'CCC(=O)O', 'propionic acid', 'products_text', 1)]}, {'family': 'Lossen Rearrangement', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Lossen rearrangement of benzohydroxamic acid to phenyl isocyanate.', 'reactants_text': 'benzohydroxamic acid', 'products_text': 'phenyl isocyanate', 'reagents_text': 'activating agent, base', 'conditions_text': 'curated application-class seed representing hydroxamic acid to isocyanate rearrangement', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Lossen rearrangement chemistry. Adds a benzohydroxamic acid example beyond the overview evidence. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=C(NO)c1ccccc1', 'benzohydroxamic acid', 'reactants_text', 1), ('product', 'O=C=Nc1ccccc1', 'phenyl isocyanate', 'products_text', 1)]}, {'family': 'Lossen Rearrangement', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Lossen rearrangement of p-toluohydroxamic acid to p-tolyl isocyanate.', 'reactants_text': 'p-toluohydroxamic acid', 'products_text': 'p-tolyl isocyanate', 'reagents_text': 'activating agent, base', 'conditions_text': 'curated application-class seed adding a substituted aryl hydroxamic acid example', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Lossen rearrangement chemistry. Adds a para-methyl aryl example for family depth. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'Cc1ccc(C(=O)NO)cc1', 'p-toluohydroxamic acid', 'reactants_text', 1), ('product', 'Cc1ccc(N=C=O)cc1', 'p-tolyl isocyanate', 'products_text', 1)]}, {'family': 'Luche Reduction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Luche reduction of cyclohex-2-en-1-one to cyclohex-2-en-1-ol.', 'reactants_text': 'cyclohex-2-en-1-one', 'products_text': 'cyclohex-2-en-1-ol', 'reagents_text': 'NaBH4, CeCl3', 'conditions_text': 'curated application-class seed representing 1,2-reduction of an enone to an allylic alcohol', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Luche reduction chemistry. Adds a cyclic enone reduction example beyond the overview evidence. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=C1C=CCCC1', 'cyclohex-2-en-1-one', 'reactants_text', 1), ('product', 'OC1=CCCCC1', 'cyclohex-2-en-1-ol', 'products_text', 1)]}, {'family': 'Luche Reduction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Luche reduction of benzalacetone to 4-phenylbut-3-en-2-ol.', 'reactants_text': 'benzalacetone', 'products_text': '4-phenylbut-3-en-2-ol', 'reagents_text': 'NaBH4, CeCl3', 'conditions_text': 'curated application-class seed adding an acyclic enone reduction example', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Luche reduction chemistry. Adds an acyclic enone example for family depth. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CC(=O)C=Cc1ccccc1', 'benzalacetone', 'reactants_text', 1), ('product', 'CC(O)C=Cc1ccccc1', '4-phenylbut-3-en-2-ol', 'products_text', 1)]}, {'family': 'Mannich Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Mannich reaction of acetophenone with formaldehyde and dimethylamine to beta-dimethylaminopropiophenone.', 'reactants_text': 'acetophenone, formaldehyde, dimethylamine', 'products_text': 'beta-dimethylaminopropiophenone', 'reagents_text': 'formaldehyde, dimethylamine, acid/base', 'conditions_text': 'curated application-class seed representing aminomethylation of an enolizable ketone', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Mannich reaction chemistry. Adds an acetophenone Mannich example beyond the overview evidence. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CC(=O)c1ccccc1', 'acetophenone', 'reactants_text', 1), ('product', 'CN(C)CC(=O)c1ccccc1', 'beta-dimethylaminopropiophenone', 'products_text', 1)]}, {'family': 'Mannich Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Mannich reaction of cyclohexanone with formaldehyde and dimethylamine to 2-(dimethylaminomethyl)cyclohexanone.', 'reactants_text': 'cyclohexanone, formaldehyde, dimethylamine', 'products_text': '2-(dimethylaminomethyl)cyclohexanone', 'reagents_text': 'formaldehyde, dimethylamine, acid/base', 'conditions_text': 'curated application-class seed adding a cyclic ketone Mannich example', 'notes_text': 'Manual curated application-class seed added during the phase15 5+5 shallow-family completion sprint for Mannich reaction chemistry. Adds a cyclohexanone example for family depth. [phase15_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=C1CCCCC1', 'cyclohexanone', 'reactants_text', 1), ('product', 'CN(C)CC1CCCCC(=O)1', '2-(dimethylaminomethyl)cyclohexanone', 'products_text', 1)]}]}

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
    """,(seed.get('anchor_image','phase14_anchor.jpg'), seed['family'])).fetchone()
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
    ap=argparse.ArgumentParser(description='Complete phase14 shallow top10 in 5+5 batches.')
    ap.add_argument('--db', default='app/labint.db')
    ap.add_argument('--report-dir', default='reports/family_completion_phase15_shallow_top10')
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
        suffix=f'phase15_shallow_top10_{args.batch}'
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
