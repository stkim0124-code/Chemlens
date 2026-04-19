from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from app.labint_frontmatter_batch_restore_utils import (
    apply_frontmatter_batch_generic,
    load_abbreviation_seeds,
    load_family_pattern_seeds,
    load_page_knowledge_seeds,
)

FRONTMATTER_BATCH37_VERSION = 'labint_frontmatter_batch37_v1_20260412'
SOURCE_LABEL = 'named_reactions_frontmatter_batch37'
LATEST_SOURCE_ZIP = 'backend_batch15_37_frontmatter_cumulative_patch_20260412.zip'

PAGE_KNOWLEDGE_SEEDS = load_page_knowledge_seeds(37, SOURCE_LABEL)
FAMILY_PATTERN_SEEDS = load_family_pattern_seeds(37, FRONTMATTER_BATCH37_VERSION)
BATCH37_SOURCE_PAGES = {"Co2(CO)8": 314, "BF3\u00b7OEt2": 315, "CAN": 315, "NMO": 314, "TMANO": 314, "TBAF": 315, "BINAP-Ru catalyst": 316, "H2": 316, "HCO2H/NEt3": 317, "CrCl2": 318, "NiCl2": 318, "DMF": 318, "DMP": 319, "LiBH4": 319, "NHK reaction": 319}
CURATED_BATCH37_ABBREVIATIONS = load_abbreviation_seeds(
    37,
    SOURCE_LABEL,
    source_pages=BATCH37_SOURCE_PAGES,
    default_note='batch37 frontmatter abbreviation seed',
)
PAGE_ENTITY_SEEDS = [{'page_label': 'p314', 'entity_text': 'Nicholas Reaction', 'canonical_name': 'Nicholas Reaction', 'entity_type': 'reaction_family', 'family_name': 'Nicholas Reaction', 'notes': 'Cobalt-alkyne decomplexation/substitution family overview.', 'confidence': 0.92}, {'page_label': 'p314', 'entity_text': 'propargylic alcohol / Co2(CO)6-alkyne complex', 'canonical_name': None, 'entity_type': 'reactant_class', 'family_name': 'Nicholas Reaction', 'notes': 'Typical cobalt-stabilized propargylic substrate used in Nicholas chemistry.', 'confidence': 0.84}, {'page_label': 'p314', 'entity_text': 'substituted alkyne or enyne product', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Nicholas Reaction', 'notes': 'Characteristic product class obtained after nucleophilic trapping and decomplexation.', 'confidence': 0.84}, {'page_label': 'p315', 'entity_text': 'Nicholas Reaction', 'canonical_name': 'Nicholas Reaction', 'entity_type': 'reaction_family', 'family_name': 'Nicholas Reaction', 'notes': 'Application page family anchor.', 'confidence': 0.92}, {'page_label': 'p315', 'entity_text': 'β-Lactam precursor to thienamycin', 'canonical_name': 'β-Lactam precursor to thienamycin', 'entity_type': 'target_molecule', 'family_name': 'Nicholas Reaction', 'notes': 'Target precursor assembled using a Nicholas reaction sequence.', 'confidence': 0.85}, {'page_label': 'p315', 'entity_text': '(+)-Epoxydictymene', 'canonical_name': '(+)-Epoxydictymene', 'entity_type': 'target_molecule', 'family_name': 'Nicholas Reaction', 'notes': 'Natural product accessed by tandem intramolecular Nicholas and Pauson-Khand chemistry.', 'confidence': 0.85}, {'page_label': 'p316', 'entity_text': 'Noyori Asymmetric Hydrogenation', 'canonical_name': 'Noyori Asymmetric Hydrogenation', 'entity_type': 'reaction_family', 'family_name': 'Noyori Asymmetric Hydrogenation', 'notes': 'Ru-BINAP asymmetric hydrogenation family overview.', 'confidence': 0.92}, {'page_label': 'p316', 'entity_text': 'functionalized alkene or beta-keto ester', 'canonical_name': None, 'entity_type': 'reactant_class', 'family_name': 'Noyori Asymmetric Hydrogenation', 'notes': 'Typical substrate classes for Noyori asymmetric hydrogenation.', 'confidence': 0.84}, {'page_label': 'p316', 'entity_text': 'enantioenriched alcohol / acid / amine', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Noyori Asymmetric Hydrogenation', 'notes': 'Characteristic product classes formed under Noyori conditions.', 'confidence': 0.84}, {'page_label': 'p317', 'entity_text': 'Noyori Asymmetric Hydrogenation', 'canonical_name': 'Noyori Asymmetric Hydrogenation', 'entity_type': 'reaction_family', 'family_name': 'Noyori Asymmetric Hydrogenation', 'notes': 'Application page family anchor.', 'confidence': 0.92}, {'page_label': 'p317', 'entity_text': '(-)-Haliclonadiamine', 'canonical_name': '(-)-Haliclonadiamine', 'entity_type': 'target_molecule', 'family_name': 'Noyori Asymmetric Hydrogenation', 'notes': 'Pentacyclic alkaloid synthesized through a key Noyori asymmetric hydrogenation.', 'confidence': 0.85}, {'page_label': 'p317', 'entity_text': '(-)-Morphine', 'canonical_name': '(-)-Morphine', 'entity_type': 'target_molecule', 'family_name': 'Noyori Asymmetric Hydrogenation', 'notes': 'Alkaloid prepared using Noyori asymmetric transfer hydrogenation of a tetrahydroisoquinoline intermediate.', 'confidence': 0.85}, {'page_label': 'p318', 'entity_text': 'Nozaki-Hiyama-Kishi Reaction', 'canonical_name': 'Nozaki-Hiyama-Kishi Reaction', 'entity_type': 'reaction_family', 'family_name': 'Nozaki-Hiyama-Kishi Reaction', 'notes': 'CrCl2-mediated NHK coupling family overview.', 'confidence': 0.92}, {'page_label': 'p318', 'entity_text': 'allyl/vinyl/aryl/alkynyl halide + aldehyde or ketone', 'canonical_name': None, 'entity_type': 'reactant_class', 'family_name': 'Nozaki-Hiyama-Kishi Reaction', 'notes': 'Canonical substrate pairing for NHK coupling.', 'confidence': 0.84}, {'page_label': 'p318', 'entity_text': 'allylic or homoallylic alcohol', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Nozaki-Hiyama-Kishi Reaction', 'notes': 'Characteristic alcohol product of Nozaki-Hiyama-Kishi coupling.', 'confidence': 0.84}, {'page_label': 'p319', 'entity_text': 'Nozaki-Hiyama-Kishi Reaction', 'canonical_name': 'Nozaki-Hiyama-Kishi Reaction', 'entity_type': 'reaction_family', 'family_name': 'Nozaki-Hiyama-Kishi Reaction', 'notes': 'Application page family anchor.', 'confidence': 0.92}, {'page_label': 'p319', 'entity_text': 'Deacetoxycalyconin Acetate', 'canonical_name': 'Deacetoxycalyconin Acetate', 'entity_type': 'target_molecule', 'family_name': 'Nozaki-Hiyama-Kishi Reaction', 'notes': 'Natural-product intermediate obtained through intramolecular NHK coupling.', 'confidence': 0.85}, {'page_label': 'p319', 'entity_text': 'C1-C19 Fragment of (-)-mycalolide', 'canonical_name': 'C1-C19 Fragment of (-)-mycalolide', 'entity_type': 'target_molecule', 'family_name': 'Nozaki-Hiyama-Kishi Reaction', 'notes': 'Fragment assembled by NHK coupling of vinyl iodide and aldehyde subunits.', 'confidence': 0.85}]


def apply_frontmatter_batch37(db_path: str | Path) -> Dict[str, Any]:
    return apply_frontmatter_batch_generic(
        db_path=db_path,
        source_label=SOURCE_LABEL,
        version=FRONTMATTER_BATCH37_VERSION,
        latest_source_zip=LATEST_SOURCE_ZIP,
        batch_no=37,
        page_knowledge_seeds=PAGE_KNOWLEDGE_SEEDS,
        family_pattern_seeds=FAMILY_PATTERN_SEEDS,
        abbreviation_seeds=CURATED_BATCH37_ABBREVIATIONS,
        page_entity_seeds=PAGE_ENTITY_SEEDS,
    )
