from __future__ import annotations
from pathlib import Path
from typing import Any, Dict

from app.labint_frontmatter_batch_restore_utils import (
    apply_frontmatter_batch_generic,
    load_abbreviation_seeds,
    load_family_pattern_seeds,
    load_page_knowledge_seeds,
)

FRONTMATTER_BATCH58_VERSION = 'labint_frontmatter_batch58_v1_20260416'
SOURCE_LABEL = 'named_reactions_frontmatter_batch58'
LATEST_SOURCE_ZIP = 'backend_batch15_58_frontmatter_cumulative_patch_20260416.zip'

PAGE_KNOWLEDGE_SEEDS = load_page_knowledge_seeds(58, SOURCE_LABEL)
FAMILY_PATTERN_SEEDS = load_family_pattern_seeds(58, FRONTMATTER_BATCH58_VERSION)
CURATED_BATCH58_ABBREVIATIONS = load_abbreviation_seeds(58, SOURCE_LABEL, source_pages={}, default_note='batch58 frontmatter abbreviation seed')
PAGE_ENTITY_SEEDS = [
    {'page_label': 'p583', 'entity_text': 'References', 'entity_type': 'document_section', 'family_name': None, 'notes': 'Backmatter bibliography section for the named reaction handbook.', 'canonical_name': 'References', 'confidence': 0.99},
    {'page_label': 'p740', 'entity_text': 'Alphabetical Index', 'entity_type': 'document_section', 'family_name': None, 'notes': 'Backmatter alphabetical index covering named reactions, targets, and related terms.', 'canonical_name': 'Alphabetical Index', 'confidence': 0.99},
]

def apply_frontmatter_batch58(db_path: str | Path) -> Dict[str, Any]:
    return apply_frontmatter_batch_generic(
        db_path=db_path,
        source_label=SOURCE_LABEL,
        version=FRONTMATTER_BATCH58_VERSION,
        latest_source_zip=LATEST_SOURCE_ZIP,
        batch_no=58,
        page_knowledge_seeds=PAGE_KNOWLEDGE_SEEDS,
        family_pattern_seeds=FAMILY_PATTERN_SEEDS,
        abbreviation_seeds=CURATED_BATCH58_ABBREVIATIONS,
        page_entity_seeds=PAGE_ENTITY_SEEDS,
        replace_existing_page_entities=True,
    )
