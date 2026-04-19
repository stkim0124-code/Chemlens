from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from app.labint_frontmatter_batch_restore_utils import (
    apply_frontmatter_batch_generic,
    load_abbreviation_seeds,
    load_family_pattern_seeds,
    load_page_knowledge_seeds,
)

FRONTMATTER_BATCH14_VERSION = 'labint_frontmatter_batch14_v1_20260411'
SOURCE_LABEL = 'named_reactions_frontmatter_batch14'
LATEST_SOURCE_ZIP = 'chemlens_backend_db_schema_frontmatter_batch14_patch_20260411.zip'

PAGE_KNOWLEDGE_SEEDS = load_page_knowledge_seeds(14, SOURCE_LABEL)
FAMILY_PATTERN_SEEDS = load_family_pattern_seeds(14, FRONTMATTER_BATCH14_VERSION)
CURATED_BATCH14_ABBREVIATIONS = load_abbreviation_seeds(
    14,
    SOURCE_LABEL,
    source_pages={'Zn(Hg)': 92, 'conc. HCl': 92, 'dry HX': 92, 'Zn/HCl': 93, 'AcOH/HCl': 93},
    default_note=None,
)
PAGE_ENTITY_SEEDS = [
    {'page_label': 'p92', 'entity_text': 'Clemmensen Reduction', 'canonical_name': 'Clemmensen Reduction', 'entity_type': 'reaction_family', 'family_name': 'Clemmensen Reduction', 'notes': 'Named reduction family overview page.', 'confidence': 0.92},
    {'page_label': 'p92', 'entity_text': 'Zn(Hg)', 'canonical_name': 'zinc amalgam', 'entity_type': 'reagent', 'family_name': 'Clemmensen Reduction', 'notes': 'Canonical Clemmensen reducing system component.', 'confidence': 0.88},
    {'page_label': 'p92', 'entity_text': 'conc. HCl', 'canonical_name': 'concentrated hydrochloric acid', 'entity_type': 'acid', 'family_name': 'Clemmensen Reduction', 'notes': 'Strong acid used in the classic protocol.', 'confidence': 0.86},
    {'page_label': 'p93', 'entity_text': '5-Epi-Pumiliotoxin C', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Clemmensen Reduction', 'notes': 'Endgame deoxygenation application in pumiliotoxin synthesis.', 'confidence': 0.87},
    {'page_label': 'p93', 'entity_text': 'Lepadiformine', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Clemmensen Reduction', 'notes': 'Late-stage ketone removal under Clemmensen conditions.', 'confidence': 0.85},
    {'page_label': 'p93', 'entity_text': 'Tetracyclic undecane derivative', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Clemmensen Reduction', 'notes': 'Novel tetracyclic undecane example on the application page.', 'confidence': 0.82},
]


def apply_frontmatter_batch14(db_path: str | Path) -> Dict[str, Any]:
    return apply_frontmatter_batch_generic(
        db_path=db_path,
        source_label=SOURCE_LABEL,
        version=FRONTMATTER_BATCH14_VERSION,
        latest_source_zip=LATEST_SOURCE_ZIP,
        batch_no=14,
        page_knowledge_seeds=PAGE_KNOWLEDGE_SEEDS,
        family_pattern_seeds=FAMILY_PATTERN_SEEDS,
        abbreviation_seeds=CURATED_BATCH14_ABBREVIATIONS,
        page_entity_seeds=PAGE_ENTITY_SEEDS,
        replace_existing_page_entities=True,
    )


if __name__ == '__main__':
    from pprint import pprint
    pprint(apply_frontmatter_batch14(Path(__file__).resolve().parent / 'labint.db'))
