from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from app.labint_frontmatter_batch_restore_utils import (
    apply_frontmatter_batch_generic,
    load_abbreviation_seeds,
    load_family_pattern_seeds,
    load_page_knowledge_seeds,
)

FRONTMATTER_BATCH38_VERSION = 'labint_frontmatter_batch38_v1_20260412'
SOURCE_LABEL = 'named_reactions_frontmatter_batch38'
LATEST_SOURCE_ZIP = 'backend_batch15_38_frontmatter_cumulative_patch_20260412.zip'

PAGE_KNOWLEDGE_SEEDS = load_page_knowledge_seeds(38, SOURCE_LABEL)
FAMILY_PATTERN_SEEDS = load_family_pattern_seeds(38, FRONTMATTER_BATCH38_VERSION)
BATCH38_SOURCE_PAGES = {
    'Al(O-i-Pr)3': 320,
    'TsOH': 321,
    'PtO2': 321,
    'Cl3CCN': 323,
    'PdCl2(PhCN)2': 323,
    'TBAF': 323,
    'KH': 325,
    'BF3·Et2O': 327,
    'P2O5': 327,
    'PPA': 326,
    'Mn(OAc)3·2H2O': 327,
    'NH4OAc/AcOH': 328,
    'AcONH4': 329,
    'PPTS': 329,
    'CSA': 329,
    'SEM': 329,
    'Ti(Oi-Pr)4': 329,
    'AlMe3': 325,
}
CURATED_BATCH38_ABBREVIATIONS = load_abbreviation_seeds(
    38,
    SOURCE_LABEL,
    source_pages=BATCH38_SOURCE_PAGES,
    default_note='batch38 frontmatter abbreviation seed',
)
PAGE_ENTITY_SEEDS = [
    {'page_label': 'p320', 'entity_text': 'Oppenauer Oxidation', 'canonical_name': 'Oppenauer Oxidation', 'entity_type': 'reaction_family', 'family_name': 'Oppenauer Oxidation', 'notes': 'Overview page family anchor for the aluminum-alkoxide oxidation.', 'confidence': 0.92},
    {'page_label': 'p320', 'entity_text': 'secondary alcohol or hydroxy ketone + ketone hydride acceptor', 'canonical_name': None, 'entity_type': 'reactant_class', 'family_name': 'Oppenauer Oxidation', 'notes': 'Canonical substrate class for Oppenauer oxidation.', 'confidence': 0.84},
    {'page_label': 'p320', 'entity_text': 'ketone or aldehyde product', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Oppenauer Oxidation', 'notes': 'Characteristic carbonyl product generated under Oppenauer conditions.', 'confidence': 0.84},
    {'page_label': 'p321', 'entity_text': 'Oppenauer Oxidation', 'canonical_name': 'Oppenauer Oxidation', 'entity_type': 'reaction_family', 'family_name': 'Oppenauer Oxidation', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p321', 'entity_text': 'Estrone', 'canonical_name': 'Estrone', 'entity_type': 'target_molecule', 'family_name': 'Oppenauer Oxidation', 'notes': 'Steroid obtained through modified Oppenauer oxidation and aromatization.', 'confidence': 0.85},
    {'page_label': 'p321', 'entity_text': '(±)-Hirsutene', 'canonical_name': '(±)-Hirsutene', 'entity_type': 'target_molecule', 'family_name': 'Oppenauer Oxidation', 'notes': 'Linearly fused triquinane assembled after oxidation-generated enone formation.', 'confidence': 0.85},
    {'page_label': 'p321', 'entity_text': '(+)-Lycodoline', 'canonical_name': '(+)-Lycodoline', 'entity_type': 'target_molecule', 'family_name': 'Oppenauer Oxidation', 'notes': 'Lycopodium alkaloid accessed through a modified Oppenauer oxidation sequence.', 'confidence': 0.85},
    {'page_label': 'p322', 'entity_text': 'Overman Rearrangement', 'canonical_name': 'Overman Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Overman Rearrangement', 'notes': 'Overview page family anchor for the allylic trichloroacetimidate rearrangement.', 'confidence': 0.92},
    {'page_label': 'p322', 'entity_text': 'allylic trichloroacetimidate', 'canonical_name': None, 'entity_type': 'reactant_class', 'family_name': 'Overman Rearrangement', 'notes': 'Canonical allylic imidate substrate used in Overman rearrangement.', 'confidence': 0.84},
    {'page_label': 'p322', 'entity_text': 'allylic trichloroacetamide / allylic amine precursor', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Overman Rearrangement', 'notes': 'Characteristic rearranged allylic amide product class.', 'confidence': 0.84},
    {'page_label': 'p323', 'entity_text': 'Overman Rearrangement', 'canonical_name': 'Overman Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Overman Rearrangement', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p323', 'entity_text': 'Sphingofungin E', 'canonical_name': 'Sphingofungin E', 'entity_type': 'target_molecule', 'family_name': 'Overman Rearrangement', 'notes': 'Natural product assembled by stereocontrolled Overman rearrangement at C5.', 'confidence': 0.85},
    {'page_label': 'p323', 'entity_text': '(±)-Pancratistatin', 'canonical_name': '(±)-Pancratistatin', 'entity_type': 'target_molecule', 'family_name': 'Overman Rearrangement', 'notes': 'Target bearing stereoselectively installed allylic nitrogen via Overman rearrangement.', 'confidence': 0.85},
    {'page_label': 'p323', 'entity_text': '(−)-Cryptopleurine', 'canonical_name': '(−)-Cryptopleurine', 'entity_type': 'target_molecule', 'family_name': 'Overman Rearrangement', 'notes': 'Phenanthroquinolizidine alkaloid synthesized through a thermal Overman rearrangement.', 'confidence': 0.85},
    {'page_label': 'p324', 'entity_text': 'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement', 'canonical_name': 'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement', 'notes': 'Overview page family anchor for oxy-Cope chemistry.', 'confidence': 0.92},
    {'page_label': 'p324', 'entity_text': '1,5-dien-3-ol or dienolate', 'canonical_name': None, 'entity_type': 'reactant_class', 'family_name': 'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement', 'notes': 'Canonical substrate class for oxy-Cope and anionic oxy-Cope rearrangements.', 'confidence': 0.84},
    {'page_label': 'p324', 'entity_text': 'δ,ε-unsaturated carbonyl compound', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement', 'notes': 'Characteristic rearrangement product after enol/enolate tautomerization or protonation.', 'confidence': 0.84},
    {'page_label': 'p325', 'entity_text': 'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement', 'canonical_name': 'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p325', 'entity_text': 'Spinosyn A', 'canonical_name': 'Spinosyn A', 'entity_type': 'target_molecule', 'family_name': 'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement', 'notes': 'Key tricyclic intermediate for spinosyn A obtained through an anionic oxy-Cope rearrangement.', 'confidence': 0.85},
    {'page_label': 'p325', 'entity_text': '(+)-Precapnelladiene', 'canonical_name': '(+)-Precapnelladiene', 'entity_type': 'target_molecule', 'family_name': 'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement', 'notes': 'Sesquiterpene synthesized through low-temperature anion-accelerated oxy-Cope rearrangement.', 'confidence': 0.85},
    {'page_label': 'p325', 'entity_text': '2-Acetoxy[5]helicene', 'canonical_name': '2-Acetoxy[5]helicene', 'entity_type': 'target_molecule', 'family_name': 'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement', 'notes': 'Helicene target assembled by sequential aromatic oxy-Cope rearrangements.', 'confidence': 0.85},
    {'page_label': 'p326', 'entity_text': 'Paal-Knorr Furan Synthesis', 'canonical_name': 'Paal-Knorr Furan Synthesis', 'entity_type': 'reaction_family', 'family_name': 'Paal-Knorr Furan Synthesis', 'notes': 'Overview page family anchor for furan-forming cyclodehydration.', 'confidence': 0.92},
    {'page_label': 'p326', 'entity_text': '1,4-dicarbonyl compound or surrogate', 'canonical_name': None, 'entity_type': 'reactant_class', 'family_name': 'Paal-Knorr Furan Synthesis', 'notes': 'Canonical precursor class for Paal-Knorr furan synthesis.', 'confidence': 0.84},
    {'page_label': 'p326', 'entity_text': 'substituted furan', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Paal-Knorr Furan Synthesis', 'notes': 'Characteristic heteroaromatic product of the Paal-Knorr furan synthesis.', 'confidence': 0.84},
    {'page_label': 'p327', 'entity_text': 'Paal-Knorr Furan Synthesis', 'canonical_name': 'Paal-Knorr Furan Synthesis', 'entity_type': 'reaction_family', 'family_name': 'Paal-Knorr Furan Synthesis', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p327', 'entity_text': 'Bisfuran macrocycle', 'canonical_name': 'Bisfuran macrocycle', 'entity_type': 'target_molecule', 'family_name': 'Paal-Knorr Furan Synthesis', 'notes': 'Macrocyclic bisfuran assembled by Paal-Knorr cyclodehydration of a tetraketone precursor.', 'confidence': 0.85},
    {'page_label': 'p327', 'entity_text': 'A soluble nonacenetriquinone', 'canonical_name': 'A soluble nonacenetriquinone', 'entity_type': 'target_molecule', 'family_name': 'Paal-Knorr Furan Synthesis', 'notes': 'Extended aromatic quinone framework obtained after BF3·Et2O-promoted Paal-Knorr furan formation.', 'confidence': 0.85},
    {'page_label': 'p327', 'entity_text': '7-Furyl-substituted quinolone', 'canonical_name': '7-Furyl-substituted quinolone', 'entity_type': 'target_molecule', 'family_name': 'Paal-Knorr Furan Synthesis', 'notes': 'Quinolone scaffold bearing a furan installed through Paal-Knorr cyclization.', 'confidence': 0.85},
    {'page_label': 'p328', 'entity_text': 'Paal-Knorr Pyrrole Synthesis', 'canonical_name': 'Paal-Knorr Pyrrole Synthesis', 'entity_type': 'reaction_family', 'family_name': 'Paal-Knorr Pyrrole Synthesis', 'notes': 'Overview page family anchor for pyrrole-forming Paal-Knorr condensation.', 'confidence': 0.92},
    {'page_label': 'p328', 'entity_text': '1,4-dicarbonyl compound + ammonia or primary amine', 'canonical_name': None, 'entity_type': 'reactant_class', 'family_name': 'Paal-Knorr Pyrrole Synthesis', 'notes': 'Canonical substrate pairing for the Paal-Knorr pyrrole synthesis.', 'confidence': 0.84},
    {'page_label': 'p328', 'entity_text': 'substituted pyrrole', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Paal-Knorr Pyrrole Synthesis', 'notes': 'Characteristic heteroaromatic product formed under Paal-Knorr pyrrole conditions.', 'confidence': 0.84},
    {'page_label': 'p329', 'entity_text': 'Paal-Knorr Pyrrole Synthesis', 'canonical_name': 'Paal-Knorr Pyrrole Synthesis', 'entity_type': 'reaction_family', 'family_name': 'Paal-Knorr Pyrrole Synthesis', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p329', 'entity_text': 'Calix[6]pyrrole', 'canonical_name': 'Calix[6]pyrrole', 'entity_type': 'target_molecule', 'family_name': 'Paal-Knorr Pyrrole Synthesis', 'notes': 'Macrocyclic pyrrole assembled from an oxidized bisfuran-derived dodecaketo precursor using ammonium acetate.', 'confidence': 0.85},
    {'page_label': 'p329', 'entity_text': 'Roseophilin', 'canonical_name': 'Roseophilin', 'entity_type': 'target_molecule', 'family_name': 'Paal-Knorr Pyrrole Synthesis', 'notes': 'Natural product synthesized through Paal-Knorr cyclization of a trisubstituted pyrrole precursor.', 'confidence': 0.85},
    {'page_label': 'p329', 'entity_text': 'Precursor for heme and porphyrin synthesis', 'canonical_name': 'Precursor for heme and porphyrin synthesis', 'entity_type': 'target_molecule', 'family_name': 'Paal-Knorr Pyrrole Synthesis', 'notes': 'Tetrasubstituted pyrrole intermediate prepared on large scale using ammonium carbonate conditions.', 'confidence': 0.85},
    {'page_label': 'p329', 'entity_text': 'Magnolamide', 'canonical_name': 'Magnolamide', 'entity_type': 'target_molecule', 'family_name': 'Paal-Knorr Pyrrole Synthesis', 'notes': 'Natural product synthesized using a titanium isopropoxide-mediated Paal-Knorr pyrrole synthesis.', 'confidence': 0.85},
]


def apply_frontmatter_batch38(db_path: str | Path) -> Dict[str, Any]:
    return apply_frontmatter_batch_generic(
        db_path=db_path,
        source_label=SOURCE_LABEL,
        version=FRONTMATTER_BATCH38_VERSION,
        latest_source_zip=LATEST_SOURCE_ZIP,
        batch_no=38,
        page_knowledge_seeds=PAGE_KNOWLEDGE_SEEDS,
        family_pattern_seeds=FAMILY_PATTERN_SEEDS,
        abbreviation_seeds=CURATED_BATCH38_ABBREVIATIONS,
        page_entity_seeds=PAGE_ENTITY_SEEDS,
    )
