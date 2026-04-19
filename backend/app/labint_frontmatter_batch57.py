from __future__ import annotations
from pathlib import Path
from typing import Any, Dict

from app.labint_frontmatter_batch_restore_utils import (
    apply_frontmatter_batch_generic,
    load_abbreviation_seeds,
    load_family_pattern_seeds,
    load_page_knowledge_seeds,
)

FRONTMATTER_BATCH57_VERSION = 'labint_frontmatter_batch57_v1_20260416'
SOURCE_LABEL = 'named_reactions_frontmatter_batch57'
LATEST_SOURCE_ZIP = 'backend_batch15_57_frontmatter_cumulative_patch_20260416.zip'

PAGE_KNOWLEDGE_SEEDS = load_page_knowledge_seeds(57, SOURCE_LABEL)
FAMILY_PATTERN_SEEDS = load_family_pattern_seeds(57, FRONTMATTER_BATCH57_VERSION)
CURATED_BATCH57_ABBREVIATIONS = load_abbreviation_seeds(
    57,
    SOURCE_LABEL,
    source_pages={},
    default_note='batch57 frontmatter abbreviation seed',
)

PAGE_ENTITY_SEEDS = [
    {'page_label': 'p554', 'entity_text': 'Appendix', 'entity_type': 'document_section', 'family_name': None, 'notes': 'Top-level appendix section introducing navigational tables for named reactions.', 'canonical_name': 'Appendix', 'confidence': 0.99},
    {'page_label': 'p554', 'entity_text': 'Chronological Reaction Index', 'entity_type': 'index_type', 'family_name': None, 'notes': 'Appendix subsection that organizes named reactions by year of discovery.', 'canonical_name': 'Chronological Reaction Index', 'confidence': 0.96},
    {'page_label': 'p554', 'entity_text': 'Reaction Categories', 'entity_type': 'index_type', 'family_name': None, 'notes': 'Appendix subsection that groups named reactions by synthetic objective.', 'canonical_name': 'Reaction Categories', 'confidence': 0.96},
    {'page_label': 'p554', 'entity_text': 'Affected Functional Groups', 'entity_type': 'index_type', 'family_name': None, 'notes': 'Appendix subsection that maps reactions from the perspective of changed functionality.', 'canonical_name': 'Affected Functional Groups', 'confidence': 0.95},
    {'page_label': 'p554', 'entity_text': 'Preparation of Functional Groups', 'entity_type': 'index_type', 'family_name': None, 'notes': 'Appendix subsection that maps target functional groups and precursor groups to named transformations.', 'canonical_name': 'Preparation of Functional Groups', 'confidence': 0.95},
    {'page_label': 'p555', 'entity_text': 'List of Named Reactions in Chronological Order of Their Discovery', 'entity_type': 'index_type', 'family_name': None, 'notes': 'Historical index of named reactions with discovery year and page references.', 'canonical_name': 'Chronological Reaction Index', 'confidence': 0.98},
    {'page_label': 'p560', 'entity_text': 'Reaction Categories', 'entity_type': 'index_type', 'family_name': None, 'notes': 'Category table organizing named reactions by synthetic use.', 'canonical_name': 'Reaction Categories', 'confidence': 0.98},
    {'page_label': 'p570', 'entity_text': 'Affected Functional Groups', 'entity_type': 'index_type', 'family_name': None, 'notes': 'Lookup table organized around affected or newly formed functional groups.', 'canonical_name': 'Affected Functional Groups', 'confidence': 0.98},
    {'page_label': 'p574', 'entity_text': 'Preparation of Functional Groups', 'entity_type': 'index_type', 'family_name': None, 'notes': 'Lookup table organized around target functional groups, substrate groups, and named transformations.', 'canonical_name': 'Preparation of Functional Groups', 'confidence': 0.98},
    {'page_label': 'p560', 'entity_text': 'synthetic use', 'entity_type': 'meta_concept', 'family_name': None, 'notes': 'Category table column describing the practical synthetic role of each named reaction.', 'canonical_name': 'synthetic use', 'confidence': 0.84},
    {'page_label': 'p570', 'entity_text': 'newly formed functional group', 'entity_type': 'meta_concept', 'family_name': None, 'notes': 'Functional-group lookup concept used to identify transformations from desired product functionality.', 'canonical_name': 'newly formed functional group', 'confidence': 0.84},
    {'page_label': 'p574', 'entity_text': 'target functional group', 'entity_type': 'meta_concept', 'family_name': None, 'notes': 'Retrosynthetic lookup concept used to find named reactions that install a target functionality.', 'canonical_name': 'target functional group', 'confidence': 0.84},
]

def apply_frontmatter_batch57(db_path: str | Path) -> Dict[str, Any]:
    return apply_frontmatter_batch_generic(
        db_path=db_path,
        source_label=SOURCE_LABEL,
        version=FRONTMATTER_BATCH57_VERSION,
        latest_source_zip=LATEST_SOURCE_ZIP,
        batch_no=57,
        page_knowledge_seeds=PAGE_KNOWLEDGE_SEEDS,
        family_pattern_seeds=FAMILY_PATTERN_SEEDS,
        abbreviation_seeds=CURATED_BATCH57_ABBREVIATIONS,
        page_entity_seeds=PAGE_ENTITY_SEEDS,
        replace_existing_page_entities=True,
    )
