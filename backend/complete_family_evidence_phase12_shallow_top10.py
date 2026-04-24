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

TAG = 'phase12_shallow_top10_completion_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES = {'Feist-Benary Furan Synthesis': 'Feist-Bénary Furan Synthesis', 'Fischer Indole Reaction': 'Fischer Indole Synthesis', 'Fleming Tamao Oxidation': 'Fleming-Tamao Oxidation', 'Friedel Crafts Acylation': 'Friedel-Crafts Acylation', 'Friedel Crafts Alkylation': 'Friedel-Crafts Alkylation', 'Gabriel Reaction': 'Gabriel Synthesis', 'Favorskii Reaction': 'Favorskii Rearrangement', 'Ferrier Reaction/Rearrangement': 'Ferrier Reaction', 'Evans Aldol': 'Evans Aldol Reaction'}

BATCH_FAMILIES = {'a': ['Feist-Bénary Furan Synthesis', 'Fischer Indole Synthesis', 'Fleming-Tamao Oxidation', 'Friedel-Crafts Acylation', 'Friedel-Crafts Alkylation'], 'b': ['Finkelstein Reaction', 'Gabriel Synthesis', 'Favorskii Rearrangement', 'Ferrier Reaction', 'Evans Aldol Reaction']}

SEEDS_BY_BATCH: Dict[str, List[Dict[str, Any]]] = {'a': [{'family': 'Feist-Bénary Furan Synthesis', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Feist-Bénary furan synthesis of phenacyl bromide with ethyl acetoacetate to a substituted furan ester.', 'reactants_text': 'phenacyl bromide and ethyl acetoacetate', 'products_text': 'ethyl 4-methyl-2-phenylfuran-3-carboxylate', 'reagents_text': 'base', 'conditions_text': 'curated application-class seed representing condensation of an alpha-haloketone with a beta-dicarbonyl compound', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Feist-Bénary furan synthesis chemistry. Adds a phenacyl bromide / beta-keto ester example beyond the overview evidence. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=C(CBr)c1ccccc1', 'phenacyl bromide', 'reactants_text', 1), ('reactant', 'CCOC(=O)CC(C)=O', 'ethyl acetoacetate', 'reactants_text', 1), ('product', 'CCOC(=O)c1oc(C)c(c1)c1ccccc1', 'ethyl 4-methyl-2-phenylfuran-3-carboxylate', 'products_text', 1)]}, {'family': 'Feist-Bénary Furan Synthesis', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Feist-Bénary furan synthesis of bromoacetone with acetylacetone to a methyl-substituted acetylfuran.', 'reactants_text': 'bromoacetone and acetylacetone', 'products_text': 'acetyl dimethylfuran product', 'reagents_text': 'base', 'conditions_text': 'curated application-class seed adding a second alpha-haloketone / 1,3-dicarbonyl example to the Feist-Bénary family', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Feist-Bénary furan synthesis chemistry. Adds a small-molecule dimethylfuran example for family depth. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CC(=O)CBr', 'bromoacetone', 'reactants_text', 1), ('reactant', 'CC(=O)CC(C)=O', 'acetylacetone', 'reactants_text', 1), ('product', 'CC(=O)c1oc(C)cc1C', 'acetyl dimethylfuran product', 'products_text', 1)]}, {'family': 'Fischer Indole Synthesis', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Fischer indole synthesis of 2-butanone with phenylhydrazine to 2-ethylindole.', 'reactants_text': '2-butanone and phenylhydrazine', 'products_text': '2-ethylindole', 'reagents_text': 'acid', 'conditions_text': 'curated application-class seed representing arylhydrazone rearrangement/cyclization to an indole', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Fischer indole synthesis chemistry. Provides a simple ketone-to-indole example beyond the overview evidence. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CCC(C)=O', '2-butanone', 'reactants_text', 1), ('reactant', 'NNc1ccccc1', 'phenylhydrazine', 'reactants_text', 1), ('product', 'CCc1[nH]c2ccccc2c1', '2-ethylindole', 'products_text', 1)]}, {'family': 'Fischer Indole Synthesis', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Fischer indole synthesis of cyclohexanone with phenylhydrazine to tetrahydrocarbazole.', 'reactants_text': 'cyclohexanone and phenylhydrazine', 'products_text': 'tetrahydrocarbazole', 'reagents_text': 'acid', 'conditions_text': 'curated application-class seed adding a cyclic ketone Fischer indole example for family depth', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Fischer indole synthesis chemistry. Adds a cyclic ketone example leading to tetrahydrocarbazole. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=C1CCCCC1', 'cyclohexanone', 'reactants_text', 1), ('reactant', 'NNc1ccccc1', 'phenylhydrazine', 'reactants_text', 1), ('product', 'c1ccc2c3c([nH]c2c1)CCCCC3', 'tetrahydrocarbazole', 'products_text', 1)]}, {'family': 'Fleming-Tamao Oxidation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Fleming-Tamao oxidation of benzyltrimethylsilane to benzyl alcohol.', 'reactants_text': 'benzyltrimethylsilane', 'products_text': 'benzyl alcohol', 'reagents_text': 'peroxide / fluoride oxidation sequence', 'conditions_text': 'curated application-class seed representing oxidation of a benzylic carbon-silicon bond to a carbon-oxygen bond', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Fleming-Tamao oxidation chemistry. Provides a benzylic organosilane oxidation example beyond the overview evidence. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'C[Si](C)(C)Cc1ccccc1', 'benzyltrimethylsilane', 'reactants_text', 1), ('product', 'OCc1ccccc1', 'benzyl alcohol', 'products_text', 1)]}, {'family': 'Fleming-Tamao Oxidation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Fleming-Tamao oxidation of 3-phenylpropyltrimethylsilane to 3-phenyl-1-propanol.', 'reactants_text': '3-phenylpropyltrimethylsilane', 'products_text': '3-phenyl-1-propanol', 'reagents_text': 'peroxide / fluoride oxidation sequence', 'conditions_text': 'curated application-class seed adding an aliphatic organosilane oxidation example to the Fleming-Tamao family', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Fleming-Tamao oxidation chemistry. Adds an aliphatic organosilane example for family depth. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'C[Si](C)(C)CCc1ccccc1', '3-phenylpropyltrimethylsilane', 'reactants_text', 1), ('product', 'OCCc1ccccc1', '3-phenyl-1-propanol', 'products_text', 1)]}, {'family': 'Friedel-Crafts Acylation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Friedel-Crafts acylation of benzene with acetyl chloride to acetophenone.', 'reactants_text': 'benzene and acetyl chloride', 'products_text': 'acetophenone', 'reagents_text': 'AlCl3', 'conditions_text': 'curated application-class seed representing Lewis-acid-promoted aromatic acylation', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Friedel-Crafts acylation chemistry. Provides a classic benzene acylation example beyond the overview evidence. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'c1ccccc1', 'benzene', 'reactants_text', 1), ('reactant', 'CC(=O)Cl', 'acetyl chloride', 'reactants_text', 1), ('product', 'CC(=O)c1ccccc1', 'acetophenone', 'products_text', 1)]}, {'family': 'Friedel-Crafts Acylation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Friedel-Crafts acylation of anisole with propionyl chloride to para-methoxypropiophenone.', 'reactants_text': 'anisole and propionyl chloride', 'products_text': 'para-methoxypropiophenone', 'reagents_text': 'AlCl3', 'conditions_text': 'curated application-class seed adding an activated arene acylation example to the Friedel-Crafts acylation family', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Friedel-Crafts acylation chemistry. Adds an anisole acylation example for family depth. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'COc1ccccc1', 'anisole', 'reactants_text', 1), ('reactant', 'CCC(=O)Cl', 'propionyl chloride', 'reactants_text', 1), ('product', 'CCC(=O)c1ccc(OC)cc1', 'para-methoxypropiophenone', 'products_text', 1)]}, {'family': 'Friedel-Crafts Alkylation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Friedel-Crafts alkylation of benzene with tert-butyl chloride to tert-butylbenzene.', 'reactants_text': 'benzene and tert-butyl chloride', 'products_text': 'tert-butylbenzene', 'reagents_text': 'AlCl3', 'conditions_text': 'curated application-class seed representing Lewis-acid-promoted aromatic alkylation', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Friedel-Crafts alkylation chemistry. Provides a classic tert-butylation example beyond the overview evidence. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'c1ccccc1', 'benzene', 'reactants_text', 1), ('reactant', 'CC(C)(C)Cl', 'tert-butyl chloride', 'reactants_text', 1), ('product', 'CC(C)(C)c1ccccc1', 'tert-butylbenzene', 'products_text', 1)]}, {'family': 'Friedel-Crafts Alkylation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Friedel-Crafts alkylation of toluene with isopropyl chloride to cymene.', 'reactants_text': 'toluene and isopropyl chloride', 'products_text': 'cymene', 'reagents_text': 'AlCl3', 'conditions_text': 'curated application-class seed adding a second activated-arene alkylation example to the Friedel-Crafts alkylation family', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Friedel-Crafts alkylation chemistry. Adds a toluene alkylation example for family depth. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'Cc1ccccc1', 'toluene', 'reactants_text', 1), ('reactant', 'CC(C)Cl', 'isopropyl chloride', 'reactants_text', 1), ('product', 'Cc1ccc(C(C)C)cc1', 'cymene', 'products_text', 1)]}], 'b': [{'family': 'Finkelstein Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Finkelstein reaction converting 1-bromobutane to 1-iodobutane.', 'reactants_text': '1-bromobutane', 'products_text': '1-iodobutane', 'reagents_text': 'NaI', 'conditions_text': 'curated application-class seed representing halide exchange under SN2 conditions', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Finkelstein reaction chemistry. Provides a simple primary alkyl halide exchange example beyond the overview evidence. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CCCCBr', '1-bromobutane', 'reactants_text', 1), ('product', 'CCCCI', '1-iodobutane', 'products_text', 1)]}, {'family': 'Finkelstein Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Finkelstein reaction converting benzyl chloride to benzyl iodide.', 'reactants_text': 'benzyl chloride', 'products_text': 'benzyl iodide', 'reagents_text': 'NaI', 'conditions_text': 'curated application-class seed adding a benzylic halide exchange example to the Finkelstein family', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Finkelstein reaction chemistry. Adds a benzylic substrate example for family depth. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'ClCc1ccccc1', 'benzyl chloride', 'reactants_text', 1), ('product', 'ICc1ccccc1', 'benzyl iodide', 'products_text', 1)]}, {'family': 'Gabriel Synthesis', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Gabriel synthesis of ethylamine from bromoethane via phthalimide alkylation and deprotection.', 'reactants_text': 'bromoethane', 'products_text': 'ethylamine', 'reagents_text': 'potassium phthalimide then hydrazinolysis', 'conditions_text': 'curated application-class seed representing primary amine synthesis from a primary alkyl halide', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Gabriel synthesis chemistry. Provides a simple primary alkyl halide to primary amine example beyond the overview evidence. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CCBr', 'bromoethane', 'reactants_text', 1), ('product', 'CCN', 'ethylamine', 'products_text', 1)]}, {'family': 'Gabriel Synthesis', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Gabriel synthesis of benzylamine from benzyl bromide via phthalimide alkylation and deprotection.', 'reactants_text': 'benzyl bromide', 'products_text': 'benzylamine', 'reagents_text': 'potassium phthalimide then hydrazinolysis', 'conditions_text': 'curated application-class seed adding a benzylic substrate example to the Gabriel synthesis family', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Gabriel synthesis chemistry. Adds a benzylic primary amine example for family depth. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'BrCc1ccccc1', 'benzyl bromide', 'reactants_text', 1), ('product', 'NCc1ccccc1', 'benzylamine', 'products_text', 1)]}, {'family': 'Favorskii Rearrangement', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Favorskii rearrangement of alpha-bromocyclohexanone to cyclopentanecarboxylic acid.', 'reactants_text': 'alpha-bromocyclohexanone', 'products_text': 'cyclopentanecarboxylic acid', 'reagents_text': 'base', 'conditions_text': 'curated application-class seed representing ring contraction of an alpha-haloketone under Favorskii conditions', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Favorskii rearrangement chemistry. Provides a ring-contraction example beyond the overview evidence. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=C1CCCCC(Br)1', 'alpha-bromocyclohexanone', 'reactants_text', 1), ('product', 'O=C(O)C1CCCC1', 'cyclopentanecarboxylic acid', 'products_text', 1)]}, {'family': 'Favorskii Rearrangement', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Favorskii rearrangement of alpha-bromocyclopentanone to cyclobutanecarboxylic acid.', 'reactants_text': 'alpha-bromocyclopentanone', 'products_text': 'cyclobutanecarboxylic acid', 'reagents_text': 'base', 'conditions_text': 'curated application-class seed adding a second ring-contraction example to the Favorskii family', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Favorskii rearrangement chemistry. Adds a cyclopentanone ring-contraction example for family depth. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=C1CCCC(Br)1', 'alpha-bromocyclopentanone', 'reactants_text', 1), ('product', 'O=C(O)C1CCC1', 'cyclobutanecarboxylic acid', 'products_text', 1)]}, {'family': 'Ferrier Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Ferrier reaction of a glycal with methanol to a 2,3-unsaturated methyl glycoside.', 'reactants_text': 'glycal model substrate and methanol', 'products_text': 'methyl 2,3-unsaturated glycoside', 'reagents_text': 'acid catalyst', 'conditions_text': 'curated application-class seed representing allylic substitution of a glycal by an alcohol nucleophile', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Ferrier reaction chemistry. Provides a methanol trapping example beyond the overview evidence. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'OC1OC=CC(O)C1O', 'glycal model substrate', 'reactants_text', 1), ('reactant', 'CO', 'methanol', 'reactants_text', 1), ('product', 'COC1OC(O)C=CC1O', 'methyl 2,3-unsaturated glycoside', 'products_text', 1)]}, {'family': 'Ferrier Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Ferrier reaction of a glycal with allyl alcohol to an allyl 2,3-unsaturated glycoside.', 'reactants_text': 'glycal model substrate and allyl alcohol', 'products_text': 'allyl 2,3-unsaturated glycoside', 'reagents_text': 'acid catalyst', 'conditions_text': 'curated application-class seed adding a second alcohol nucleophile example to the Ferrier family', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Ferrier reaction chemistry. Adds an allyl alcohol trapping example for family depth. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'OC1OC=CC(O)C1O', 'glycal model substrate', 'reactants_text', 1), ('reactant', 'C=CCO', 'allyl alcohol', 'reactants_text', 1), ('product', 'C=CCOC1OC(O)C=CC1O', 'allyl 2,3-unsaturated glycoside', 'products_text', 1)]}, {'family': 'Evans Aldol Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Evans aldol reaction of an N-propionyl oxazolidinone enolate with benzaldehyde to a beta-hydroxy imide.', 'reactants_text': 'N-propionyl oxazolidinone and benzaldehyde', 'products_text': 'beta-hydroxy imide adduct', 'reagents_text': 'boron enolate / aldehyde', 'conditions_text': 'curated application-class seed representing auxiliary-controlled aldol addition to an aromatic aldehyde', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Evans aldol chemistry. Provides an aromatic aldehyde example beyond the overview evidence. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CCC(=O)N1CCOC(=O)C1', 'N-propionyl oxazolidinone', 'reactants_text', 1), ('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1), ('product', 'CCC(O)C(c1ccccc1)C(=O)N1CCOC(=O)C1', 'beta-hydroxy imide adduct', 'products_text', 1)]}, {'family': 'Evans Aldol Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Evans aldol reaction of an N-propionyl oxazolidinone enolate with butyraldehyde to a beta-hydroxy imide.', 'reactants_text': 'N-propionyl oxazolidinone and butyraldehyde', 'products_text': 'alkyl beta-hydroxy imide adduct', 'reagents_text': 'boron enolate / aldehyde', 'conditions_text': 'curated application-class seed adding an aliphatic aldehyde example to the Evans aldol family', 'notes_text': 'Manual curated application-class seed added during the phase12 5+5 shallow-family completion sprint for Evans aldol chemistry. Adds an aliphatic aldehyde example for family depth. [phase12_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CCC(=O)N1CCOC(=O)C1', 'N-propionyl oxazolidinone', 'reactants_text', 1), ('reactant', 'CCCC=O', 'butyraldehyde', 'reactants_text', 1), ('product', 'CCCC(O)CC(=O)N1CCOC(=O)C1', 'alkyl beta-hydroxy imide adduct', 'products_text', 1)]}]}

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
    ap = argparse.ArgumentParser(description='Complete shallow-family sprint phase12 top10 in 5+5 batches.')
    ap.add_argument('--db', default='app/labint.db')
    ap.add_argument('--report-dir', default='reports/family_completion_phase12_shallow_top10')
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
        suffix = f'phase12_shallow_top10_{args.batch}'
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
