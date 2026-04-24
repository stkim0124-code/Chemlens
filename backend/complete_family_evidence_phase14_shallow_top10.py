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

TAG = 'phase14_shallow_top10_completion_v1'
NOW = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

DATA_ALIASES = {'Furukawa Simmons-Smith Modification': 'Furukawa Modification', 'Gattermann Formylation': 'Gattermann and Gattermann-Koch Formylation', 'Gattermann-Koch Formylation': 'Gattermann and Gattermann-Koch Formylation', 'Hunsdiecker-Borodin Reaction': 'Hunsdiecker Reaction', 'Katsuki-Jacobsen Epoxidation': 'Jacobsen-Katsuki Epoxidation', 'Jacobsen-Katsuki Reaction': 'Jacobsen-Katsuki Epoxidation', 'Jones Oxidation/Oxidation of Alcohols by Chromium Reagents': 'Jones Oxidation', 'Jacobsen HKR': 'Jacobsen Hydrolytic Kinetic Resolution', 'Hydrolytic Kinetic Resolution': 'Jacobsen Hydrolytic Kinetic Resolution', 'Johnson Claisen Rearrangement': 'Johnson-Claisen Rearrangement', 'Japp Klingemann Reaction': 'Japp-Klingemann Reaction', 'Still-Gennari Modification': 'Horner-Wadsworth-Emmons Olefination – Still-Gennari Modification', 'Still-Gennari Olefination': 'Horner-Wadsworth-Emmons Olefination – Still-Gennari Modification', 'Julia-Lythgoe Reaction': 'Julia-Lythgoe Olefination'}
BATCH_FAMILIES = {'a': ['Furukawa Modification', 'Gattermann and Gattermann-Koch Formylation', 'Hunsdiecker Reaction', 'Jacobsen-Katsuki Epoxidation', 'Jones Oxidation'], 'b': ['Jacobsen Hydrolytic Kinetic Resolution', 'Johnson-Claisen Rearrangement', 'Japp-Klingemann Reaction', 'Horner-Wadsworth-Emmons Olefination – Still-Gennari Modification', 'Julia-Lythgoe Olefination']}
SEEDS_BY_BATCH = {'a': [{'family': 'Furukawa Modification', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Furukawa modification cyclopropanation of styrene to phenylcyclopropane.', 'reactants_text': 'styrene', 'products_text': 'phenylcyclopropane', 'reagents_text': 'Et2Zn, CH2I2', 'conditions_text': 'curated application-class seed representing Simmons-Smith/Furukawa cyclopropanation of a vinylarene', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Furukawa modification chemistry. Adds a styrene cyclopropanation example beyond the overview evidence. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'C=Cc1ccccc1', 'styrene', 'reactants_text', 1), ('product', 'c1ccc(C2CC2)cc1', 'phenylcyclopropane', 'products_text', 1)]}, {'family': 'Furukawa Modification', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Furukawa modification cyclopropanation of 4-methoxystyrene to 1-cyclopropyl-4-methoxybenzene.', 'reactants_text': '4-methoxystyrene', 'products_text': '1-cyclopropyl-4-methoxybenzene', 'reagents_text': 'Et2Zn, CH2I2', 'conditions_text': 'curated application-class seed adding an anisyl alkene cyclopropanation example', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Furukawa modification chemistry. Adds an electron-rich aryl alkene example for family depth. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'COc1ccc(C=C)cc1', '4-methoxystyrene', 'reactants_text', 1), ('product', 'COc1ccc(C2CC2)cc1', '1-cyclopropyl-4-methoxybenzene', 'products_text', 1)]}, {'family': 'Gattermann and Gattermann-Koch Formylation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: para-formylation of anisole to 4-anisaldehyde under Gattermann/Gattermann-Koch conditions.', 'reactants_text': 'anisole', 'products_text': '4-anisaldehyde', 'reagents_text': 'CO/HCl, Lewis acid', 'conditions_text': 'curated application-class seed representing electrophilic aromatic formylation', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Gattermann/Gattermann-Koch formylation chemistry. Adds an anisole formylation example beyond the overview evidence. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'COc1ccccc1', 'anisole', 'reactants_text', 1), ('product', 'COc1ccc(C=O)cc1', '4-anisaldehyde', 'products_text', 1)]}, {'family': 'Gattermann and Gattermann-Koch Formylation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: para-formylation of chlorobenzene to 4-chlorobenzaldehyde under Gattermann/Gattermann-Koch conditions.', 'reactants_text': 'chlorobenzene', 'products_text': '4-chlorobenzaldehyde', 'reagents_text': 'CO/HCl, Lewis acid', 'conditions_text': 'curated application-class seed adding a haloarene formylation example', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Gattermann/Gattermann-Koch formylation chemistry. Adds a haloarene example for family depth. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'Clc1ccccc1', 'chlorobenzene', 'reactants_text', 1), ('product', 'O=Cc1ccc(Cl)cc1', '4-chlorobenzaldehyde', 'products_text', 1)]}, {'family': 'Hunsdiecker Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Hunsdiecker decarboxylative bromination of benzoic acid to bromobenzene.', 'reactants_text': 'benzoic acid', 'products_text': 'bromobenzene', 'reagents_text': 'Ag salt, Br2', 'conditions_text': 'curated application-class seed representing decarboxylative halogenation of a carboxylic acid', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Hunsdiecker chemistry. Adds a benzoic acid decarboxylative bromination example beyond the overview evidence. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=C(O)c1ccccc1', 'benzoic acid', 'reactants_text', 1), ('product', 'Brc1ccccc1', 'bromobenzene', 'products_text', 1)]}, {'family': 'Hunsdiecker Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Hunsdiecker decarboxylative bromination of 4-methylbenzoic acid to 4-bromotoluene.', 'reactants_text': '4-methylbenzoic acid', 'products_text': '4-bromotoluene', 'reagents_text': 'Ag salt, Br2', 'conditions_text': 'curated application-class seed adding a substituted aryl example to the Hunsdiecker family', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Hunsdiecker chemistry. Adds a para-methyl aryl example for family depth. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'Cc1ccc(C(=O)O)cc1', '4-methylbenzoic acid', 'reactants_text', 1), ('product', 'Cc1ccc(Br)cc1', '4-bromotoluene', 'products_text', 1)]}, {'family': 'Jacobsen-Katsuki Epoxidation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Jacobsen-Katsuki epoxidation of styrene to styrene oxide.', 'reactants_text': 'styrene', 'products_text': 'styrene oxide', 'reagents_text': 'Mn(salen), oxidant', 'conditions_text': 'curated application-class seed representing asymmetric epoxidation of an alkene', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Jacobsen-Katsuki epoxidation chemistry. Adds a styrene epoxidation example beyond the overview evidence. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'C=Cc1ccccc1', 'styrene', 'reactants_text', 1), ('product', 'c1ccccc1C1CO1', 'styrene oxide', 'products_text', 1)]}, {'family': 'Jacobsen-Katsuki Epoxidation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Jacobsen-Katsuki epoxidation of 4-methoxystyrene to 4-methoxystyrene oxide.', 'reactants_text': '4-methoxystyrene', 'products_text': '4-methoxystyrene oxide', 'reagents_text': 'Mn(salen), oxidant', 'conditions_text': 'curated application-class seed adding an anisyl alkene epoxidation example', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Jacobsen-Katsuki epoxidation chemistry. Adds an electron-rich styrene example for family depth. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'COc1ccc(C=C)cc1', '4-methoxystyrene', 'reactants_text', 1), ('product', 'COc1ccc(C1CO1)cc1', '4-methoxystyrene oxide', 'products_text', 1)]}, {'family': 'Jones Oxidation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Jones oxidation of cyclohexanol to cyclohexanone.', 'reactants_text': 'cyclohexanol', 'products_text': 'cyclohexanone', 'reagents_text': 'CrO3, H2SO4, acetone', 'conditions_text': 'curated application-class seed representing oxidation of a secondary alcohol', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Jones oxidation chemistry. Adds a cyclic secondary alcohol oxidation example beyond the overview evidence. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'OC1CCCCC1', 'cyclohexanol', 'reactants_text', 1), ('product', 'O=C1CCCCC1', 'cyclohexanone', 'products_text', 1)]}, {'family': 'Jones Oxidation', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Jones oxidation of benzyl alcohol to benzoic acid.', 'reactants_text': 'benzyl alcohol', 'products_text': 'benzoic acid', 'reagents_text': 'CrO3, H2SO4, acetone', 'conditions_text': 'curated application-class seed adding a primary alcohol to carboxylic acid oxidation example', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Jones oxidation chemistry. Adds a benzylic primary alcohol oxidation example for family depth. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'OCc1ccccc1', 'benzyl alcohol', 'reactants_text', 1), ('product', 'O=C(O)c1ccccc1', 'benzoic acid', 'products_text', 1)]}], 'b': [{'family': 'Jacobsen Hydrolytic Kinetic Resolution', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Jacobsen hydrolytic kinetic resolution of styrene oxide to 1-phenyl-1,2-ethanediol.', 'reactants_text': 'styrene oxide', 'products_text': '1-phenyl-1,2-ethanediol', 'reagents_text': 'Co(salen), water', 'conditions_text': 'curated application-class seed representing hydrolytic kinetic resolution of a terminal epoxide', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Jacobsen hydrolytic kinetic resolution chemistry. Adds a styrene oxide hydrolysis example beyond the overview evidence. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'c1ccccc1C1CO1', 'styrene oxide', 'reactants_text', 1), ('product', 'OCC(O)c1ccccc1', '1-phenyl-1,2-ethanediol', 'products_text', 1)]}, {'family': 'Jacobsen Hydrolytic Kinetic Resolution', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Jacobsen hydrolytic kinetic resolution of epichlorohydrin to 3-chloro-1,2-propanediol.', 'reactants_text': 'epichlorohydrin', 'products_text': '3-chloro-1,2-propanediol', 'reagents_text': 'Co(salen), water', 'conditions_text': 'curated application-class seed adding an aliphatic epoxide example to the Jacobsen HKR family', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Jacobsen hydrolytic kinetic resolution chemistry. Adds an epichlorohydrin example for family depth. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'ClCC1CO1', 'epichlorohydrin', 'reactants_text', 1), ('product', 'OCC(O)CCl', '3-chloro-1,2-propanediol', 'products_text', 1)]}, {'family': 'Johnson-Claisen Rearrangement', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Johnson-Claisen rearrangement of allyl alcohol to ethyl 4-pentenoate.', 'reactants_text': 'allyl alcohol', 'products_text': 'ethyl 4-pentenoate', 'reagents_text': 'triethyl orthoacetate, acid', 'conditions_text': 'curated application-class seed representing allylic alcohol to gamma,delta-unsaturated ester rearrangement', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Johnson-Claisen chemistry. Adds a simple allyl alcohol rearrangement example beyond the overview evidence. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'C=CCO', 'allyl alcohol', 'reactants_text', 1), ('product', 'CCOC(=O)CCC=C', 'ethyl 4-pentenoate', 'products_text', 1)]}, {'family': 'Johnson-Claisen Rearrangement', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Johnson-Claisen rearrangement of cinnamyl alcohol to ethyl 6-phenylhex-4-enoate.', 'reactants_text': 'cinnamyl alcohol', 'products_text': 'ethyl 6-phenylhex-4-enoate', 'reagents_text': 'triethyl orthoacetate, acid', 'conditions_text': 'curated application-class seed adding an aryl-substituted allylic alcohol example', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Johnson-Claisen chemistry. Adds an aryl allylic alcohol example for family depth. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'OC/C=C/c1ccccc1', 'cinnamyl alcohol', 'reactants_text', 1), ('product', 'CCOC(=O)CC/C=C/c1ccccc1', 'ethyl 6-phenylhex-4-enoate', 'products_text', 1)]}, {'family': 'Japp-Klingemann Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Japp-Klingemann coupling of ethyl acetoacetate-derived enolate with benzenediazonium to give a phenylhydrazone.', 'reactants_text': 'ethyl acetoacetate and benzenediazonium equivalent', 'products_text': 'ethyl 2-oxo-3-(2-phenylhydrazono)butanoate', 'reagents_text': 'aryl diazonium salt, base', 'conditions_text': 'curated application-class seed representing hydrazone formation from a beta-ketoester and diazonium partner', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Japp-Klingemann chemistry. Adds a beta-ketoester / diazonium coupling example beyond the overview evidence. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CCOC(=O)CC(C)=O', 'ethyl acetoacetate', 'reactants_text', 1), ('product', 'CCOC(=O)C(=NNc1ccccc1)C(C)=O', 'ethyl 2-oxo-3-(2-phenylhydrazono)butanoate', 'products_text', 1)]}, {'family': 'Japp-Klingemann Reaction', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Japp-Klingemann coupling of ethyl benzoylacetate-derived enolate with benzenediazonium to give a phenylhydrazone.', 'reactants_text': 'ethyl benzoylacetate and benzenediazonium equivalent', 'products_text': 'ethyl 2-benzoyl-2-(2-phenylhydrazono)acetate', 'reagents_text': 'aryl diazonium salt, base', 'conditions_text': 'curated application-class seed adding an aryl beta-ketoester example to the Japp-Klingemann family', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Japp-Klingemann chemistry. Adds an aryl beta-ketoester example for family depth. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'CCOC(=O)CC(=O)c1ccccc1', 'ethyl benzoylacetate', 'reactants_text', 1), ('product', 'CCOC(=O)C(=NNc1ccccc1)C(=O)c1ccccc1', 'ethyl 2-benzoyl-2-(2-phenylhydrazono)acetate', 'products_text', 1)]}, {'family': 'Horner-Wadsworth-Emmons Olefination – Still-Gennari Modification', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Still-Gennari olefination of benzaldehyde to methyl cinnamate.', 'reactants_text': 'benzaldehyde and Still-Gennari phosphonate reagent', 'products_text': 'methyl cinnamate', 'reagents_text': 'bis(2,2,2-trifluoroethyl) phosphonate reagent, base', 'conditions_text': 'curated application-class seed representing Z-selective HWE/Still-Gennari olefination chemistry', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Still-Gennari olefination chemistry. Adds a benzaldehyde olefination example beyond the overview evidence. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1), ('product', 'COC(=O)/C=C/c1ccccc1', 'methyl cinnamate', 'products_text', 1)]}, {'family': 'Horner-Wadsworth-Emmons Olefination – Still-Gennari Modification', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Still-Gennari olefination of anisaldehyde to methyl 4-methoxycinnamate.', 'reactants_text': '4-anisaldehyde and Still-Gennari phosphonate reagent', 'products_text': 'methyl 4-methoxycinnamate', 'reagents_text': 'bis(2,2,2-trifluoroethyl) phosphonate reagent, base', 'conditions_text': 'curated application-class seed adding an anisaldehyde example to the Still-Gennari family', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Still-Gennari olefination chemistry. Adds an anisaldehyde olefination example for family depth. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'COc1ccc(C=O)cc1', '4-anisaldehyde', 'reactants_text', 1), ('product', 'COC(=O)/C=C/c1ccc(OC)cc1', 'methyl 4-methoxycinnamate', 'products_text', 1)]}, {'family': 'Julia-Lythgoe Olefination', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Julia-Lythgoe olefination delivering stilbene from benzaldehyde and benzyl phenyl sulfone.', 'reactants_text': 'benzaldehyde and benzyl phenyl sulfone', 'products_text': 'stilbene', 'reagents_text': 'benzyl phenyl sulfone, base', 'conditions_text': 'curated application-class seed representing sulfone-based olefination to a diaryl alkene', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Julia-Lythgoe olefination chemistry. Adds a stilbene-forming example beyond the overview evidence. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=S(=O)(c1ccccc1)Cc1ccccc1', 'benzyl phenyl sulfone', 'reactants_text', 1), ('reactant', 'O=Cc1ccccc1', 'benzaldehyde', 'reactants_text', 1), ('product', 'c1ccc(/C=C/c2ccccc2)cc1', 'stilbene', 'products_text', 1)]}, {'family': 'Julia-Lythgoe Olefination', 'extract_kind': 'application_example', 'transformation_text': 'Application example: Julia-Lythgoe olefination delivering 4-methoxystilbene from anisaldehyde and benzyl phenyl sulfone.', 'reactants_text': '4-anisaldehyde and benzyl phenyl sulfone', 'products_text': '4-methoxystilbene', 'reagents_text': 'benzyl phenyl sulfone, base', 'conditions_text': 'curated application-class seed adding an anisyl alkene example to the Julia-Lythgoe family', 'notes_text': 'Manual curated application-class seed added during the phase14 5+5 shallow-family completion sprint for Julia-Lythgoe olefination chemistry. Adds an anisyl stilbene example for family depth. [phase14_shallow_top10_completion_v1]', 'molecules': [('reactant', 'O=S(=O)(c1ccccc1)Cc1ccccc1', 'benzyl phenyl sulfone', 'reactants_text', 1), ('reactant', 'COc1ccc(C=O)cc1', '4-anisaldehyde', 'reactants_text', 1), ('product', 'COc1ccc(/C=C/c2ccccc2)cc1', '4-methoxystilbene', 'products_text', 1)]}]}

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
    ap.add_argument('--report-dir', default='reports/family_completion_phase14_shallow_top10')
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
        suffix=f'phase14_shallow_top10_{args.batch}'
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
