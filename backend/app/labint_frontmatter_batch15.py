from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from app.labint_frontmatter_batch_restore_utils import (
    apply_frontmatter_batch_generic,
    load_abbreviation_seeds,
    load_family_pattern_seeds,
    load_page_knowledge_seeds,
)

FRONTMATTER_BATCH15_VERSION = 'labint_frontmatter_batch15_v1_20260412'
SOURCE_LABEL = 'named_reactions_frontmatter_batch15'
LATEST_SOURCE_ZIP = 'chemlens_frontmatter_batch15_patch_20260412.zip'

PAGE_KNOWLEDGE_SEEDS = load_page_knowledge_seeds(15, SOURCE_LABEL)
FAMILY_PATTERN_SEEDS = load_family_pattern_seeds(15, FRONTMATTER_BATCH15_VERSION)
BATCH15_SOURCE_PAGES = {
    'PPA': 95,
    'DDQ': 95,
    'mCPBA': 97,
    'PCC': 99,
    'CuBr': 99,
    'TsOH': 101,
    'BH3·DMS': 101,
    'NaH': 102,
    'DMSO': 103,
    'THF': 103,
    'LiOMe': 103,
    'CH2Cl2': 99,
}
CURATED_BATCH15_ABBREVIATIONS = load_abbreviation_seeds(
    15,
    SOURCE_LABEL,
    source_pages=BATCH15_SOURCE_PAGES,
    default_note='batch15 frontmatter abbreviation seed',
)
PAGE_ENTITY_SEEDS = [
    {'page_label': 'p94', 'entity_text': 'Combes Quinoline Synthesis', 'canonical_name': 'Combes Quinoline Synthesis', 'entity_type': 'reaction_family', 'family_name': 'Combes Quinoline Synthesis', 'notes': 'Named quinoline annulation family overview.', 'confidence': 0.92},
    {'page_label': 'p94', 'entity_text': 'Schiff base', 'canonical_name': 'Schiff Base', 'entity_type': 'mechanistic_concept', 'family_name': 'Combes Quinoline Synthesis', 'notes': 'Imine intermediate explicitly highlighted in the overview mechanism.', 'confidence': 0.87},
    {'page_label': 'p94', 'entity_text': '4-hydroxyquinoline', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Combes Quinoline Synthesis', 'notes': 'Characteristic product class from beta-ketoester variants.', 'confidence': 0.85},
    {'page_label': 'p95', 'entity_text': 'Combes Quinoline Synthesis', 'canonical_name': 'Combes Quinoline Synthesis', 'entity_type': 'reaction_family', 'family_name': 'Combes Quinoline Synthesis', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p95', 'entity_text': 'PPA', 'canonical_name': 'polyphosphoric acid', 'entity_type': 'reagent', 'family_name': 'Combes Quinoline Synthesis', 'notes': 'Cyclodehydrating acid used in several application examples.', 'confidence': 0.86},
    {'page_label': 'p95', 'entity_text': 'Imidazobenzodiazepine', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Combes Quinoline Synthesis', 'notes': 'Representative fused heterocycle accessed on the application page.', 'confidence': 0.85},
    {'page_label': 'p96', 'entity_text': 'Cope Elimination / Cope Reaction', 'canonical_name': 'Cope Elimination / Cope Reaction', 'entity_type': 'reaction_family', 'family_name': 'Cope Elimination / Cope Reaction', 'notes': 'Named elimination family overview.', 'confidence': 0.92},
    {'page_label': 'p96', 'entity_text': 'syn elimination', 'canonical_name': None, 'entity_type': 'mechanistic_concept', 'family_name': 'Cope Elimination / Cope Reaction', 'notes': 'The overview stresses stereospecific syn elimination from amine N-oxides.', 'confidence': 0.87},
    {'page_label': 'p96', 'entity_text': 'tertiary amine N-oxide', 'canonical_name': None, 'entity_type': 'reactant_class', 'family_name': 'Cope Elimination / Cope Reaction', 'notes': 'Canonical substrate class for the Cope elimination.', 'confidence': 0.86},
    {'page_label': 'p97', 'entity_text': 'Cope Elimination / Cope Reaction', 'canonical_name': 'Cope Elimination / Cope Reaction', 'entity_type': 'reaction_family', 'family_name': 'Cope Elimination / Cope Reaction', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p97', 'entity_text': 'mCPBA', 'canonical_name': 'meta-chloroperoxybenzoic acid', 'entity_type': 'reagent', 'family_name': 'Cope Elimination / Cope Reaction', 'notes': 'Common oxidant used to generate N-oxides before elimination.', 'confidence': 0.86},
    {'page_label': 'p97', 'entity_text': '(1S)-10-Methylenecamphor', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Cope Elimination / Cope Reaction', 'notes': 'Taxoid intermediate generated via N-oxide elimination in the final application.', 'confidence': 0.85},
    {'page_label': 'p98', 'entity_text': 'Cope Rearrangement', 'canonical_name': 'Cope Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Cope Rearrangement', 'notes': 'Named [3,3]-sigmatropic rearrangement family overview.', 'confidence': 0.92},
    {'page_label': 'p98', 'entity_text': '[3,3]-sigmatropic rearrangement', 'canonical_name': None, 'entity_type': 'mechanistic_concept', 'family_name': 'Cope Rearrangement', 'notes': 'Core mechanistic descriptor on the overview page.', 'confidence': 0.87},
    {'page_label': 'p98', 'entity_text': '1,5-diene', 'canonical_name': None, 'entity_type': 'reactant_class', 'family_name': 'Cope Rearrangement', 'notes': 'Canonical substrate class for the rearrangement.', 'confidence': 0.86},
    {'page_label': 'p99', 'entity_text': 'Cope Rearrangement', 'canonical_name': 'Cope Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Cope Rearrangement', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p99', 'entity_text': '(+)-Asteriscanolide', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Cope Rearrangement', 'notes': 'Medium-ring natural product assembled through a divinylcyclobutane Cope rearrangement.', 'confidence': 0.86},
    {'page_label': 'p99', 'entity_text': '(+)-Tremulenolide A', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Cope Rearrangement', 'notes': 'Application example derived from Rh-catalyzed cyclopropanation followed by Cope rearrangement.', 'confidence': 0.85},
    {'page_label': 'p100', 'entity_text': 'Corey-Bakshi-Shibata Reduction (CBS Reduction)', 'canonical_name': 'Corey-Bakshi-Shibata Reduction (CBS Reduction)', 'entity_type': 'reaction_family', 'family_name': 'Corey-Bakshi-Shibata Reduction (CBS Reduction)', 'notes': 'Named asymmetric reduction family overview.', 'confidence': 0.92},
    {'page_label': 'p100', 'entity_text': 'oxazaborolidine catalyst', 'canonical_name': None, 'entity_type': 'mechanistic_concept', 'family_name': 'Corey-Bakshi-Shibata Reduction (CBS Reduction)', 'notes': 'Chiral boron catalyst class central to the CBS mechanism.', 'confidence': 0.87},
    {'page_label': 'p100', 'entity_text': 'secondary alcohol', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Corey-Bakshi-Shibata Reduction (CBS Reduction)', 'notes': 'Characteristic product class of ketone CBS reduction.', 'confidence': 0.86},
    {'page_label': 'p101', 'entity_text': 'Corey-Bakshi-Shibata Reduction (CBS Reduction)', 'canonical_name': 'Corey-Bakshi-Shibata Reduction (CBS Reduction)', 'entity_type': 'reaction_family', 'family_name': 'Corey-Bakshi-Shibata Reduction (CBS Reduction)', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p101', 'entity_text': 'Prostaglandin E1', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Corey-Bakshi-Shibata Reduction (CBS Reduction)', 'notes': 'Early complex-molecule example showcasing high-yield CBS reduction.', 'confidence': 0.86},
    {'page_label': 'p101', 'entity_text': 'Dysidiolide', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Corey-Bakshi-Shibata Reduction (CBS Reduction)', 'notes': 'Marine natural product target employing oxazaborolidine-catalyzed reduction.', 'confidence': 0.85},
    {'page_label': 'p102', 'entity_text': 'Corey-Chaykovsky Epoxidation and Cyclopropanation', 'canonical_name': 'Corey-Chaykovsky Epoxidation and Cyclopropanation', 'entity_type': 'reaction_family', 'family_name': 'Corey-Chaykovsky Epoxidation and Cyclopropanation', 'notes': 'Named sulfur ylide transfer family overview.', 'confidence': 0.92},
    {'page_label': 'p102', 'entity_text': 'sulfur ylide', 'canonical_name': None, 'entity_type': 'mechanistic_concept', 'family_name': 'Corey-Chaykovsky Epoxidation and Cyclopropanation', 'notes': 'Shared reactive intermediate for both epoxidation and cyclopropanation branches.', 'confidence': 0.87},
    {'page_label': 'p102', 'entity_text': 'epoxide', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Corey-Chaykovsky Epoxidation and Cyclopropanation', 'notes': 'Canonical product class from aldehyde and ketone substrates.', 'confidence': 0.86},
    {'page_label': 'p103', 'entity_text': 'Corey-Chaykovsky Epoxidation and Cyclopropanation', 'canonical_name': 'Corey-Chaykovsky Epoxidation and Cyclopropanation', 'entity_type': 'reaction_family', 'family_name': 'Corey-Chaykovsky Epoxidation and Cyclopropanation', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p103', 'entity_text': '(+)-Phyllanthocin', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Corey-Chaykovsky Epoxidation and Cyclopropanation', 'notes': 'Chemoselective epoxide-installation application highlighted at the top of the page.', 'confidence': 0.86},
    {'page_label': 'p103', 'entity_text': '(±)-Isovelleral', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Corey-Chaykovsky Epoxidation and Cyclopropanation', 'notes': 'Late-stage cyclopropanation application shown at the bottom of the page.', 'confidence': 0.85},
]


def apply_frontmatter_batch15(db_path: str | Path) -> Dict[str, Any]:
    return apply_frontmatter_batch_generic(
        db_path=db_path,
        source_label=SOURCE_LABEL,
        version=FRONTMATTER_BATCH15_VERSION,
        latest_source_zip=LATEST_SOURCE_ZIP,
        batch_no=15,
        page_knowledge_seeds=PAGE_KNOWLEDGE_SEEDS,
        family_pattern_seeds=FAMILY_PATTERN_SEEDS,
        abbreviation_seeds=CURATED_BATCH15_ABBREVIATIONS,
        page_entity_seeds=PAGE_ENTITY_SEEDS,
        replace_existing_page_entities=True,
    )


if __name__ == '__main__':
    from pprint import pprint
    pprint(apply_frontmatter_batch15(Path(__file__).resolve().parent / 'labint.db'))
