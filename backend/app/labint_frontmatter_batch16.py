from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from app.labint_frontmatter_batch_restore_utils import (
    apply_frontmatter_batch_generic,
    load_abbreviation_seeds,
    load_family_pattern_seeds,
    load_page_knowledge_seeds,
)

FRONTMATTER_BATCH16_VERSION = 'labint_frontmatter_batch16_v1_20260412'
SOURCE_LABEL = 'named_reactions_frontmatter_batch16'
LATEST_SOURCE_ZIP = 'chemlens_frontmatter_batch16_patch_20260412.zip'

PAGE_KNOWLEDGE_SEEDS = load_page_knowledge_seeds(16, SOURCE_LABEL)
FAMILY_PATTERN_SEEDS = load_family_pattern_seeds(16, FRONTMATTER_BATCH16_VERSION)
BATCH16_SOURCE_PAGES = {
    'CBr4': 104,
    'Ph3P': 104,
    'n-BuLi': 104,
    'NCS': 106,
    'DMS': 106,
    'Et3N': 106,
    'PPh3': 108,
    'AgClO4': 109,
    'P(OEt)3': 110,
    'LHMDS': 113,
    'NaI': 113,
    "Lawesson's reagent": 113,
}
CURATED_BATCH16_ABBREVIATIONS = load_abbreviation_seeds(
    16,
    SOURCE_LABEL,
    source_pages=BATCH16_SOURCE_PAGES,
    default_note='batch16 frontmatter abbreviation seed',
)
PAGE_ENTITY_SEEDS = [
    {'page_label': 'p104', 'entity_text': 'Corey-Fuchs Alkyne Synthesis', 'canonical_name': 'Corey-Fuchs Alkyne Synthesis', 'entity_type': 'reaction_family', 'family_name': 'Corey-Fuchs Alkyne Synthesis', 'notes': 'Named aldehyde-to-alkyne homologation family overview.', 'confidence': 0.92},
    {'page_label': 'p104', 'entity_text': 'dibromoolefin', 'canonical_name': None, 'entity_type': 'intermediate_class', 'family_name': 'Corey-Fuchs Alkyne Synthesis', 'notes': 'Characteristic intermediate generated before strong-base conversion to an acetylide.', 'confidence': 0.87},
    {'page_label': 'p104', 'entity_text': 'terminal alkyne', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Corey-Fuchs Alkyne Synthesis', 'notes': 'Canonical product class of the homologation sequence.', 'confidence': 0.86},
    {'page_label': 'p105', 'entity_text': 'Corey-Fuchs Alkyne Synthesis', 'canonical_name': 'Corey-Fuchs Alkyne Synthesis', 'entity_type': 'reaction_family', 'family_name': 'Corey-Fuchs Alkyne Synthesis', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p105', 'entity_text': '4,4a-Didehydrohimandravine', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Corey-Fuchs Alkyne Synthesis', 'notes': 'Alkaloid target assembled using a one-pot Corey-Fuchs sequence en route to a vinylstannane intermediate.', 'confidence': 0.86},
    {'page_label': 'p105', 'entity_text': '(+)-Taylorione', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Corey-Fuchs Alkyne Synthesis', 'notes': 'Natural product application featuring monitored dibromoolefin formation and alkyne generation from an advanced aldehyde.', 'confidence': 0.85},
    {'page_label': 'p106', 'entity_text': 'Corey-Kim Oxidation', 'canonical_name': 'Corey-Kim Oxidation', 'entity_type': 'reaction_family', 'family_name': 'Corey-Kim Oxidation', 'notes': 'Named mild alcohol oxidation family overview.', 'confidence': 0.92},
    {'page_label': 'p106', 'entity_text': 'alkoxysulfonium salt', 'canonical_name': None, 'entity_type': 'intermediate_class', 'family_name': 'Corey-Kim Oxidation', 'notes': 'Key reactive intermediate highlighted in the mechanistic scheme.', 'confidence': 0.87},
    {'page_label': 'p106', 'entity_text': 'aldehyde', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Corey-Kim Oxidation', 'notes': 'Representative oxidation product class from primary alcohol substrates.', 'confidence': 0.85},
    {'page_label': 'p107', 'entity_text': 'Corey-Kim Oxidation', 'canonical_name': 'Corey-Kim Oxidation', 'entity_type': 'reaction_family', 'family_name': 'Corey-Kim Oxidation', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p107', 'entity_text': '(+)-Cephalotaxine', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Corey-Kim Oxidation', 'notes': 'Late-stage cis-vicinal diol oxidation to the corresponding diketo intermediate in cephalotaxine synthesis.', 'confidence': 0.86},
    {'page_label': 'p107', 'entity_text': 'LY426965', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Corey-Kim Oxidation', 'notes': 'Medicinal chemistry example where the allylic alcohol is converted into the corresponding chloride precursor via Corey-Kim oxidation.', 'confidence': 0.85},
    {'page_label': 'p108', 'entity_text': 'Corey-Nicolaou Macrolactonization', 'canonical_name': 'Corey-Nicolaou Macrolactonization', 'entity_type': 'reaction_family', 'family_name': 'Corey-Nicolaou Macrolactonization', 'notes': 'Named macrolactonization family overview.', 'confidence': 0.92},
    {'page_label': 'p108', 'entity_text': '2-pyridinethiol ester', 'canonical_name': None, 'entity_type': 'intermediate_class', 'family_name': 'Corey-Nicolaou Macrolactonization', 'notes': 'Activated acyl intermediate central to the Corey-Nicolaou protocol.', 'confidence': 0.87},
    {'page_label': 'p108', 'entity_text': 'medium- or large-ring lactone', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Corey-Nicolaou Macrolactonization', 'notes': 'Canonical product class explicitly shown in the overview scheme.', 'confidence': 0.85},
    {'page_label': 'p109', 'entity_text': 'Corey-Nicolaou Macrolactonization', 'canonical_name': 'Corey-Nicolaou Macrolactonization', 'entity_type': 'reaction_family', 'family_name': 'Corey-Nicolaou Macrolactonization', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p109', 'entity_text': '(-)-Aplyolide A', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Corey-Nicolaou Macrolactonization', 'notes': 'Marine natural product target obtained by selective 16-membered macrolactonization under Corey-Nicolaou conditions.', 'confidence': 0.86},
    {'page_label': 'p109', 'entity_text': '(-)-Tuckolide', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Corey-Nicolaou Macrolactonization', 'notes': 'Ten-membered lactone target highlighting selectivity of the Corey-Nicolaou method relative to other macrolactonization protocols.', 'confidence': 0.85},
    {'page_label': 'p110', 'entity_text': 'Corey-Winter Olefination', 'canonical_name': 'Corey-Winter Olefination', 'entity_type': 'reaction_family', 'family_name': 'Corey-Winter Olefination', 'notes': 'Named diol-to-alkene deoxygenation family overview.', 'confidence': 0.92},
    {'page_label': 'p110', 'entity_text': 'cyclic 1,2-thionocarbonate', 'canonical_name': None, 'entity_type': 'intermediate_class', 'family_name': 'Corey-Winter Olefination', 'notes': 'Characteristic cyclic intermediate formed from the vicinal diol before phosphite elimination.', 'confidence': 0.87},
    {'page_label': 'p110', 'entity_text': 'alkene', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Corey-Winter Olefination', 'notes': 'Canonical product class of the olefination sequence.', 'confidence': 0.85},
    {'page_label': 'p111', 'entity_text': 'Corey-Winter Olefination', 'canonical_name': 'Corey-Winter Olefination', 'entity_type': 'reaction_family', 'family_name': 'Corey-Winter Olefination', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p111', 'entity_text': 'Radiosumin', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Corey-Winter Olefination', 'notes': 'Peptidic natural product synthesis using Corey-Winter installation of a key trans-1,3-diene subunit.', 'confidence': 0.86},
    {'page_label': 'p111', 'entity_text': 'L-(+)-Swainsonine', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Corey-Winter Olefination', 'notes': 'Application example where vicinal diol deoxygenation is used near the endgame of swainsonine synthesis.', 'confidence': 0.85},
    {'page_label': 'p112', 'entity_text': 'Cornforth Rearrangement', 'canonical_name': 'Cornforth Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Cornforth Rearrangement', 'notes': 'Named oxazole skeletal rearrangement family overview.', 'confidence': 0.92},
    {'page_label': 'p112', 'entity_text': 'dicarbonylnitrile ylide', 'canonical_name': None, 'entity_type': 'mechanistic_concept', 'family_name': 'Cornforth Rearrangement', 'notes': 'Reactive intermediate proposed for the oxazole isomerization mechanism.', 'confidence': 0.87},
    {'page_label': 'p112', 'entity_text': '4-carbonyl-substituted oxazole', 'canonical_name': None, 'entity_type': 'reactant_class', 'family_name': 'Cornforth Rearrangement', 'notes': 'Canonical substrate class shown on the overview page.', 'confidence': 0.85},
    {'page_label': 'p113', 'entity_text': 'Cornforth Rearrangement', 'canonical_name': 'Cornforth Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Cornforth Rearrangement', 'notes': 'Application page family anchor.', 'confidence': 0.92},
    {'page_label': 'p113', 'entity_text': '5-Aminothiazole derivatives', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Cornforth Rearrangement', 'notes': 'Thiazole products obtained after thionation and Cornforth rearrangement of oxazole precursors.', 'confidence': 0.86},
    {'page_label': 'p113', 'entity_text': 'Imidazo[5,1-b]-2,3-dihydrooxazole', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Cornforth Rearrangement', 'notes': 'Fused heterocycle produced from a rearrangement precursor followed by thermal cyclization.', 'confidence': 0.85},
]


def apply_frontmatter_batch16(db_path: str | Path) -> Dict[str, Any]:
    return apply_frontmatter_batch_generic(
        db_path=db_path,
        source_label=SOURCE_LABEL,
        version=FRONTMATTER_BATCH16_VERSION,
        latest_source_zip=LATEST_SOURCE_ZIP,
        batch_no=16,
        page_knowledge_seeds=PAGE_KNOWLEDGE_SEEDS,
        family_pattern_seeds=FAMILY_PATTERN_SEEDS,
        abbreviation_seeds=CURATED_BATCH16_ABBREVIATIONS,
        page_entity_seeds=PAGE_ENTITY_SEEDS,
    )


if __name__ == '__main__':
    from pprint import pprint
    pprint(apply_frontmatter_batch16(Path(__file__).resolve().parent / 'labint.db'))
