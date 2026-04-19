from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from app.labint_frontmatter_batch_restore_utils import (
    apply_frontmatter_batch_generic,
    load_abbreviation_seeds,
    load_family_pattern_seeds,
    load_page_knowledge_seeds,
)

FRONTMATTER_BATCH19_VERSION = 'labint_frontmatter_batch19_v1_20260412'
SOURCE_LABEL = 'named_reactions_frontmatter_batch19'
LATEST_SOURCE_ZIP = 'backend_batch15_19_frontmatter_cumulative_patch_20260412.zip'

PAGE_KNOWLEDGE_SEEDS = load_page_knowledge_seeds(19, SOURCE_LABEL)
FAMILY_PATTERN_SEEDS = load_family_pattern_seeds(19, FRONTMATTER_BATCH19_VERSION)
BATCH19_SOURCE_PAGES = {'DMP': 136,
 'IBX': 136,
 'KBrO3': 136,
 'KOtBu': 139,
 'LiAlH4': 135,
 'LiHMDS': 139,
 'NaClO2': 137,
 'NaNO2': 134,
 'Oxone': 136,
 'PDC': 143,
 'PIDA': 141,
 'TEMPO': 137,
 'TFAA': 143,
 'TMSCN': 135,
 'TMSOTf': 138,
 'ZnCl2': 143}
CURATED_BATCH19_ABBREVIATIONS = load_abbreviation_seeds(
    19,
    SOURCE_LABEL,
    source_pages=BATCH19_SOURCE_PAGES,
    default_note='batch19 frontmatter abbreviation seed',
)
PAGE_ENTITY_SEEDS = [{'page_label': 'p134', 'entity_text': 'Demjanov and Tiffeneau-Demjanov Rearrangement', 'entity_type': 'reaction_family', 'canonical_name': 'Demjanov and Tiffeneau-Demjanov Rearrangement', 'family_name': 'Demjanov and Tiffeneau-Demjanov Rearrangement', 'notes': 'Named ring-expansion family overview.', 'confidence': 0.92}, {'page_label': 'p134', 'entity_text': '1,2-alkyl shift', 'entity_type': 'mechanistic_concept', 'canonical_name': None, 'family_name': 'Demjanov and Tiffeneau-Demjanov Rearrangement', 'notes': 'Key rearrangement step following diazonium-ion formation and nitrogen loss.', 'confidence': 0.87}, {'page_label': 'p134', 'entity_text': 'cycloalkanone homologue', 'entity_type': 'product_class', 'canonical_name': None, 'family_name': 'Demjanov and Tiffeneau-Demjanov Rearrangement', 'notes': 'Representative ring-expanded product class produced in the Tiffeneau-Demjanov variant.', 'confidence': 0.85}, {'page_label': 'p135', 'entity_text': 'Demjanov and Tiffeneau-Demjanov Rearrangement', 'entity_type': 'reaction_family', 'canonical_name': 'Demjanov and Tiffeneau-Demjanov Rearrangement', 'family_name': 'Demjanov and Tiffeneau-Demjanov Rearrangement', 'notes': 'Application page family anchor.', 'confidence': 0.92}, {'page_label': 'p135', 'entity_text': 'Homobrexan-2-one', 'entity_type': 'target_molecule', 'canonical_name': None, 'family_name': 'Demjanov and Tiffeneau-Demjanov Rearrangement', 'notes': 'Ring-expanded target accessed through a Tiffeneau-Demjanov sequence from brexan-2-one.', 'confidence': 0.86}, {'page_label': 'p135', 'entity_text': 'Homospectinomycin', 'entity_type': 'target_molecule', 'canonical_name': None, 'family_name': 'Demjanov and Tiffeneau-Demjanov Rearrangement', 'notes': 'Expanded spectinomycin analogue featured as a rearrangement application.', 'confidence': 0.85}, {'page_label': 'p136', 'entity_text': 'Dess-Martin Oxidation', 'entity_type': 'reaction_family', 'canonical_name': 'Dess-Martin Oxidation', 'family_name': 'Dess-Martin Oxidation', 'notes': 'Named hypervalent-iodine oxidation family overview.', 'confidence': 0.92}, {'page_label': 'p136', 'entity_text': 'Dess-Martin periodinane', 'entity_type': 'reagent', 'canonical_name': 'Dess-Martin periodinane', 'family_name': 'Dess-Martin Oxidation', 'notes': 'Soluble iodine(V) oxidant central to the named reaction.', 'confidence': 0.87}, {'page_label': 'p136', 'entity_text': 'carbonyl compound', 'entity_type': 'product_class', 'canonical_name': None, 'family_name': 'Dess-Martin Oxidation', 'notes': 'Canonical aldehyde/ketone product class formed from alcohol or oxime oxidation.', 'confidence': 0.85}, {'page_label': 'p137', 'entity_text': 'Dess-Martin Oxidation', 'entity_type': 'reaction_family', 'canonical_name': 'Dess-Martin Oxidation', 'family_name': 'Dess-Martin Oxidation', 'notes': 'Application page family anchor.', 'confidence': 0.92}, {'page_label': 'p137', 'entity_text': 'Ustiloxin D', 'entity_type': 'target_molecule', 'canonical_name': None, 'family_name': 'Dess-Martin Oxidation', 'notes': 'Late-stage aldehyde formation and oxidation sequence highlighted in the total synthesis.', 'confidence': 0.86}, {'page_label': 'p137', 'entity_text': 'CP-molecule core with gamma-hydroxy lactone moiety', 'entity_type': 'target_molecule', 'canonical_name': None, 'family_name': 'Dess-Martin Oxidation', 'notes': 'Advanced oxidized core obtained through one-pot and cascade DMP-mediated oxidations.', 'confidence': 0.85}, {'page_label': 'p138', 'entity_text': 'Dieckmann Condensation', 'entity_type': 'reaction_family', 'canonical_name': 'Dieckmann Condensation', 'family_name': 'Dieckmann Condensation', 'notes': 'Named intramolecular Claisen condensation family overview.', 'confidence': 0.92}, {'page_label': 'p138', 'entity_text': 'Thorpe-Ziegler cyclization', 'entity_type': 'related_reaction', 'canonical_name': None, 'family_name': 'Dieckmann Condensation', 'notes': 'Related nitrile cyclization showcased alongside the Dieckmann condensation.', 'confidence': 0.87}, {'page_label': 'p138', 'entity_text': 'cyclic beta-keto ester', 'entity_type': 'product_class', 'canonical_name': None, 'family_name': 'Dieckmann Condensation', 'notes': 'Canonical product class from intramolecular Claisen ring closure.', 'confidence': 0.85}, {'page_label': 'p139', 'entity_text': 'Dieckmann Condensation', 'entity_type': 'reaction_family', 'canonical_name': 'Dieckmann Condensation', 'family_name': 'Dieckmann Condensation', 'notes': 'Application page family anchor.', 'confidence': 0.92}, {'page_label': 'p139', 'entity_text': 'Mycophenolic acid', 'entity_type': 'target_molecule', 'canonical_name': None, 'family_name': 'Dieckmann Condensation', 'notes': 'Highly substituted aromatic natural product accessed through Michael addition plus Dieckmann annulation.', 'confidence': 0.86}, {'page_label': 'p139', 'entity_text': '(-)-Galbonolide B', 'entity_type': 'target_molecule', 'canonical_name': None, 'family_name': 'Dieckmann Condensation', 'notes': 'Macro-Dieckmann cyclization example highlighted for 14-membered lactone formation.', 'confidence': 0.85}, {'page_label': 'p140', 'entity_text': 'Diels-Alder Cycloaddition', 'entity_type': 'reaction_family', 'canonical_name': 'Diels-Alder Cycloaddition', 'family_name': 'Diels-Alder Cycloaddition', 'notes': 'Named [4+2] cycloaddition family overview.', 'confidence': 0.92}, {'page_label': 'p140', 'entity_text': 'normal electron-demand Diels-Alder reaction', 'entity_type': 'mechanistic_concept', 'canonical_name': None, 'family_name': 'Diels-Alder Cycloaddition', 'notes': 'Electron-rich diene/electron-poor dienophile mode emphasized on the overview page.', 'confidence': 0.87}, {'page_label': 'p140', 'entity_text': 'endo 1,2-product (ortho)', 'entity_type': 'product_class', 'canonical_name': None, 'family_name': 'Diels-Alder Cycloaddition', 'notes': 'Representative regiochemical outcome shown in the overview schemes.', 'confidence': 0.85}, {'page_label': 'p141', 'entity_text': 'Diels-Alder Cycloaddition', 'entity_type': 'reaction_family', 'canonical_name': 'Diels-Alder Cycloaddition', 'family_name': 'Diels-Alder Cycloaddition', 'notes': 'Application page family anchor.', 'confidence': 0.92}, {'page_label': 'p141', 'entity_text': 'Asatone', 'entity_type': 'target_molecule', 'canonical_name': None, 'family_name': 'Diels-Alder Cycloaddition', 'notes': 'Natural product formed after oxidative dearomatization and intermolecular Diels-Alder dimerization.', 'confidence': 0.86}, {'page_label': 'p141', 'entity_text': 'Rubrolone aglycon', 'entity_type': 'target_molecule', 'canonical_name': None, 'family_name': 'Diels-Alder Cycloaddition', 'notes': 'Aglycon target assembled by cyclopropene-containing intermolecular Diels-Alder chemistry.', 'confidence': 0.85}, {'page_label': 'p142', 'entity_text': 'Dienone-Phenol Rearrangement', 'entity_type': 'reaction_family', 'canonical_name': 'Dienone-Phenol Rearrangement', 'family_name': 'Dienone-Phenol Rearrangement', 'notes': 'Named rearomatizing alkyl-migration family overview.', 'confidence': 0.92}, {'page_label': 'p142', 'entity_text': '1,3-alkyl migration', 'entity_type': 'mechanistic_concept', 'canonical_name': None, 'family_name': 'Dienone-Phenol Rearrangement', 'notes': 'Overall migration pattern often rationalized as sequential 1,2-shifts in cyclohexadienone systems.', 'confidence': 0.87}, {'page_label': 'p142', 'entity_text': '3,4-disubstituted phenol', 'entity_type': 'product_class', 'canonical_name': None, 'family_name': 'Dienone-Phenol Rearrangement', 'notes': 'Representative rearomatized product class from para-dienone rearrangement.', 'confidence': 0.85}, {'page_label': 'p143', 'entity_text': 'Dienone-Phenol Rearrangement', 'entity_type': 'reaction_family', 'canonical_name': 'Dienone-Phenol Rearrangement', 'family_name': 'Dienone-Phenol Rearrangement', 'notes': 'Application page family anchor.', 'confidence': 0.92}, {'page_label': 'p143', 'entity_text': 'tetrasubstituted phenol', 'entity_type': 'product_class', 'canonical_name': None, 'family_name': 'Dienone-Phenol Rearrangement', 'notes': 'Photochemically generated substituted phenol product class highlighted on the application page.', 'confidence': 0.86}, {'page_label': 'p143', 'entity_text': 'bis C-aryl glycoside', 'entity_type': 'target_molecule', 'canonical_name': None, 'family_name': 'Dienone-Phenol Rearrangement', 'notes': 'Kidamycin-model glycal substrate class undergoing Lewis-acid-mediated dienone-phenol rearrangement.', 'confidence': 0.85}]


def apply_frontmatter_batch19(db_path: str | Path) -> Dict[str, Any]:
    return apply_frontmatter_batch_generic(
        db_path=db_path,
        source_label=SOURCE_LABEL,
        version=FRONTMATTER_BATCH19_VERSION,
        latest_source_zip=LATEST_SOURCE_ZIP,
        batch_no=19,
        page_knowledge_seeds=PAGE_KNOWLEDGE_SEEDS,
        family_pattern_seeds=FAMILY_PATTERN_SEEDS,
        abbreviation_seeds=CURATED_BATCH19_ABBREVIATIONS,
        page_entity_seeds=PAGE_ENTITY_SEEDS,
    )


if __name__ == '__main__':
    from pprint import pprint
    pprint(apply_frontmatter_batch19(Path(__file__).resolve().parent / 'labint.db'))
