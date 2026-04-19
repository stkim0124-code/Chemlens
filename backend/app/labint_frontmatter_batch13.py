from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from app.labint_frontmatter_batch_restore_utils import (
    apply_frontmatter_batch_generic,
    load_abbreviation_seeds,
    load_family_pattern_seeds,
    load_page_knowledge_seeds,
)

FRONTMATTER_BATCH13_VERSION = 'labint_frontmatter_batch13_v2_20260412'
SOURCE_LABEL = 'named_reactions_frontmatter_batch13'
LATEST_SOURCE_ZIP = 'chemlens_backend_db_schema_frontmatter_batch13_patch_20260411.zip'

PAGE_KNOWLEDGE_SEEDS = load_page_knowledge_seeds(13, SOURCE_LABEL)
FAMILY_PATTERN_SEEDS = load_family_pattern_seeds(13, FRONTMATTER_BATCH13_VERSION)
BATCH13_SOURCE_PAGES = {
    'Cl3CCO2Na': 84,
    'PhH': 84,
    'CHX3': 84,
    'TiCl4': 89,
    'LDA': 90,
    'TMSCl': 90,
    'HMPA': 90,
    'Et3N': 91,
    'TBSCl': 91,
    'NaH': 82,
    'KOH': 82,
    'MeI': 82,
    'CS2': 82,
}

CURATED_BATCH13_ABBREVIATIONS = load_abbreviation_seeds(
    13,
    SOURCE_LABEL,
    source_pages=BATCH13_SOURCE_PAGES,
    default_note='batch13 frontmatter abbreviation seed',
)
PAGE_ENTITY_SEEDS = [
    {'page_label': 'p82', 'entity_text': 'Chugaev Elimination Reaction', 'canonical_name': 'Chugaev Elimination Reaction', 'entity_type': 'reaction_family', 'family_name': 'Chugaev Elimination Reaction', 'notes': 'Named reaction family overview.', 'confidence': 0.92},
    {'page_label': 'p82', 'entity_text': 'xanthate ester pyrolysis', 'canonical_name': 'Xanthate Ester Pyrolysis', 'entity_type': 'mechanistic_concept', 'family_name': 'Chugaev Elimination Reaction', 'notes': 'Thermal pyrolysis of alkyl xanthates.', 'confidence': 0.86},
    {'page_label': 'p82', 'entity_text': 'alkene', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Chugaev Elimination Reaction', 'notes': 'Dehydration product class emphasized on overview page.', 'confidence': 0.86},
    {'page_label': 'p83', 'entity_text': 'Chugaev Elimination Reaction', 'canonical_name': 'Chugaev Elimination Reaction', 'entity_type': 'reaction_family', 'family_name': 'Chugaev Elimination Reaction', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p83', 'entity_text': 'CS2', 'canonical_name': 'carbon disulfide', 'entity_type': 'reagent', 'family_name': 'Chugaev Elimination Reaction', 'notes': 'Key reagent used in xanthate formation before pyrolysis.', 'confidence': 0.86},
    {'page_label': 'p83', 'entity_text': 'Kinamycins', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Chugaev Elimination Reaction', 'notes': 'Natural product family highlighted among applications.', 'confidence': 0.86},
    {'page_label': 'p84', 'entity_text': 'Ciamician-Dennstedt Rearrangement', 'canonical_name': 'Ciamician-Dennstedt Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Ciamician-Dennstedt Rearrangement', 'notes': 'Named reaction family overview.', 'confidence': 0.92},
    {'page_label': 'p84', 'entity_text': 'dihalocarbene', 'canonical_name': None, 'entity_type': 'mechanistic_concept', 'family_name': 'Ciamician-Dennstedt Rearrangement', 'notes': 'Reactive intermediate responsible for ring expansion.', 'confidence': 0.86},
    {'page_label': 'p84', 'entity_text': '3-halopyridine', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Ciamician-Dennstedt Rearrangement', 'notes': 'Characteristic product class from pyrrole ring expansion.', 'confidence': 0.86},
    {'page_label': 'p85', 'entity_text': 'Ciamician-Dennstedt Rearrangement', 'canonical_name': 'Ciamician-Dennstedt Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Ciamician-Dennstedt Rearrangement', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p85', 'entity_text': 'sodium trichloroacetate', 'canonical_name': 'sodium trichloroacetate', 'entity_type': 'reagent', 'family_name': 'Ciamician-Dennstedt Rearrangement', 'notes': 'Carbene precursor used in macrocyclic applications.', 'confidence': 0.86},
    {'page_label': 'p85', 'entity_text': 'calix[4]pyridine', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Ciamician-Dennstedt Rearrangement', 'notes': 'Representative macrocyclic target scaffold from applications.', 'confidence': 0.86},
    {'page_label': 'p86', 'entity_text': 'Claisen Condensation / Claisen Reaction', 'canonical_name': 'Claisen Condensation / Claisen Reaction', 'entity_type': 'reaction_family', 'family_name': 'Claisen Condensation / Claisen Reaction', 'notes': 'Named reaction family overview.', 'confidence': 0.92},
    {'page_label': 'p86', 'entity_text': 'beta-keto ester', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Claisen Condensation / Claisen Reaction', 'notes': 'Canonical product type of Claisen condensation.', 'confidence': 0.86},
    {'page_label': 'p86', 'entity_text': 'Dieckmann condensation', 'canonical_name': 'Dieckmann Condensation', 'entity_type': 'mechanistic_concept', 'family_name': 'Claisen Condensation / Claisen Reaction', 'notes': 'Intramolecular variant highlighted on overview page.', 'confidence': 0.86},
    {'page_label': 'p87', 'entity_text': 'Claisen Condensation / Claisen Reaction', 'canonical_name': 'Claisen Condensation / Claisen Reaction', 'entity_type': 'reaction_family', 'family_name': 'Claisen Condensation / Claisen Reaction', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p87', 'entity_text': 'mixed Claisen condensation', 'canonical_name': None, 'entity_type': 'mechanistic_concept', 'family_name': 'Claisen Condensation / Claisen Reaction', 'notes': 'Mixed Claisen variant is central in one application.', 'confidence': 0.86},
    {'page_label': 'p87', 'entity_text': 'Justicidin B', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Claisen Condensation / Claisen Reaction', 'notes': 'Natural product target highlighted on application page.', 'confidence': 0.86},
    {'page_label': 'p88', 'entity_text': 'Claisen Rearrangement', 'canonical_name': 'Claisen Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Claisen Rearrangement', 'notes': 'Named reaction family overview.', 'confidence': 0.92},
    {'page_label': 'p88', 'entity_text': '[3,3]-sigmatropic rearrangement', 'canonical_name': None, 'entity_type': 'mechanistic_concept', 'family_name': 'Claisen Rearrangement', 'notes': 'Core mechanistic descriptor on overview page.', 'confidence': 0.86},
    {'page_label': 'p88', 'entity_text': 'allyl vinyl ether', 'canonical_name': None, 'entity_type': 'reactant_class', 'family_name': 'Claisen Rearrangement', 'notes': 'Canonical substrate class for Claisen rearrangement.', 'confidence': 0.86},
    {'page_label': 'p89', 'entity_text': 'Claisen Rearrangement', 'canonical_name': 'Claisen Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Claisen Rearrangement', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p89', 'entity_text': 'Tebbe methylenation', 'canonical_name': 'Tebbe Olefination', 'entity_type': 'mechanistic_concept', 'family_name': 'Claisen Rearrangement', 'notes': 'Precursor-forming method highlighted before rearrangement.', 'confidence': 0.86},
    {'page_label': 'p89', 'entity_text': 'Saudin', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Claisen Rearrangement', 'notes': 'Representative natural product target from applications.', 'confidence': 0.86},
    {'page_label': 'p90', 'entity_text': 'Claisen-Ireland Rearrangement', 'canonical_name': 'Claisen-Ireland Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Claisen-Ireland Rearrangement', 'notes': 'Named reaction family overview.', 'confidence': 0.92},
    {'page_label': 'p90', 'entity_text': 'O-silyl ketene acetal', 'canonical_name': None, 'entity_type': 'reactant_class', 'family_name': 'Claisen-Ireland Rearrangement', 'notes': 'Key trapped enolate intermediate for the rearrangement.', 'confidence': 0.86},
    {'page_label': 'p90', 'entity_text': 'gamma,delta-unsaturated acid', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Claisen-Ireland Rearrangement', 'notes': 'Characteristic hydrolysis product class.', 'confidence': 0.86},
    {'page_label': 'p91', 'entity_text': 'Claisen-Ireland Rearrangement', 'canonical_name': 'Claisen-Ireland Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Claisen-Ireland Rearrangement', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p91', 'entity_text': 'TMSCl', 'canonical_name': 'trimethylsilyl chloride', 'entity_type': 'reagent', 'family_name': 'Claisen-Ireland Rearrangement', 'notes': 'Silylating agent used to access ketene acetals.', 'confidence': 0.86},
    {'page_label': 'p91', 'entity_text': 'Ebelactone A', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Claisen-Ireland Rearrangement', 'notes': 'Representative natural product target highlighted on the page.', 'confidence': 0.86},
]


def apply_frontmatter_batch13(db_path: str | Path) -> Dict[str, Any]:
    return apply_frontmatter_batch_generic(
        db_path=db_path,
        source_label=SOURCE_LABEL,
        version=FRONTMATTER_BATCH13_VERSION,
        latest_source_zip=LATEST_SOURCE_ZIP,
        batch_no=13,
        page_knowledge_seeds=PAGE_KNOWLEDGE_SEEDS,
        family_pattern_seeds=FAMILY_PATTERN_SEEDS,
        abbreviation_seeds=CURATED_BATCH13_ABBREVIATIONS,
        page_entity_seeds=PAGE_ENTITY_SEEDS,
        replace_existing_page_entities=True,
    )


if __name__ == '__main__':
    from pprint import pprint
    pprint(apply_frontmatter_batch13(Path(__file__).resolve().parent / 'labint.db'))
