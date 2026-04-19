from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from app.labint_frontmatter_batch_restore_utils import (
    apply_frontmatter_batch_generic,
    load_abbreviation_seeds,
    load_family_pattern_seeds,
    load_page_knowledge_seeds,
)

FRONTMATTER_BATCH17_VERSION = 'labint_frontmatter_batch17_v1_20260412'
SOURCE_LABEL = 'named_reactions_frontmatter_batch17'
LATEST_SOURCE_ZIP = 'backend_batch15_17_frontmatter_cumulative_patch_20260412.zip'

PAGE_KNOWLEDGE_SEEDS = load_page_knowledge_seeds(17, SOURCE_LABEL)
FAMILY_PATTERN_SEEDS = load_family_pattern_seeds(17, FRONTMATTER_BATCH17_VERSION)
BATCH17_SOURCE_PAGES = {
    'Pb(OAc)4': 114,
    'PTSA': 115,
    'DPPA': 116,
    'TMSN3': 116,
    'NaN3': 116,
    'H2O2': 118,
    'PhSeO2H': 119,
    'Ac2O': 120,
    'DMAP': 121,
    '(EtCO)2O': 121,
    'Tf2O': 123,
    'TBAF': 123,
    'TIPSCl': 123,
}
CURATED_BATCH17_ABBREVIATIONS = load_abbreviation_seeds(
    17,
    SOURCE_LABEL,
    source_pages=BATCH17_SOURCE_PAGES,
    default_note='batch17 frontmatter abbreviation seed',
)
PAGE_ENTITY_SEEDS = [{'page_label': 'p114', 'entity_text': 'Criegee Oxidation', 'canonical_name': 'Criegee Oxidation', 'entity_type': 'reaction_family', 'family_name': 'Criegee Oxidation', 'notes': 'Named vicinal-diol oxidative cleavage family overview.', 'confidence': 0.92}, {'page_label': 'p114', 'entity_text': 'lead glycolate intermediate', 'canonical_name': None, 'entity_type': 'intermediate_class', 'family_name': 'Criegee Oxidation', 'notes': 'Mechanistic lead-bound glycolate species proposed before C-C bond fragmentation.', 'confidence': 0.87}, {'page_label': 'p114', 'entity_text': 'carbonyl compounds', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Criegee Oxidation', 'notes': 'Canonical cleavage products explicitly shown on the overview page.', 'confidence': 0.85}, {'page_label': 'p115', 'entity_text': 'Criegee Oxidation', 'canonical_name': 'Criegee Oxidation', 'entity_type': 'reaction_family', 'family_name': 'Criegee Oxidation', 'notes': 'Application page family anchor.', 'confidence': 0.92}, {'page_label': 'p115', 'entity_text': 'Halicholactone', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Criegee Oxidation', 'notes': 'Marine metabolite target prepared through low-temperature Criegee cleavage of an advanced vicinal diol.', 'confidence': 0.86}, {'page_label': 'p115', 'entity_text': '(+)-Pyrenolide B', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Criegee Oxidation', 'notes': 'Ring-enlarged natural product target obtained after dihydroxylation and Criegee oxidation.', 'confidence': 0.85}, {'page_label': 'p116', 'entity_text': 'Curtius Rearrangement', 'canonical_name': 'Curtius Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Curtius Rearrangement', 'notes': 'Named acyl azide to isocyanate rearrangement family overview.', 'confidence': 0.92}, {'page_label': 'p116', 'entity_text': 'isocyanate', 'canonical_name': None, 'entity_type': 'intermediate_class', 'family_name': 'Curtius Rearrangement', 'notes': 'Key reactive intermediate generated after nitrogen extrusion from the acyl azide.', 'confidence': 0.87}, {'page_label': 'p116', 'entity_text': 'carbamate', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Curtius Rearrangement', 'notes': 'Representative product class formed by trapping the isocyanate with an alcohol.', 'confidence': 0.85}, {'page_label': 'p117', 'entity_text': 'Curtius Rearrangement', 'canonical_name': 'Curtius Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Curtius Rearrangement', 'notes': 'Application page family anchor.', 'confidence': 0.92}, {'page_label': 'p117', 'entity_text': '(-)-Cytoxazone', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Curtius Rearrangement', 'notes': 'Oxazolidinone natural product assembled by Curtius rearrangement and in situ isocyanate capture.', 'confidence': 0.86}, {'page_label': 'p117', 'entity_text': 'Pancratistatin', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Curtius Rearrangement', 'notes': 'Complex alkaloid target accessed through a late-stage carbamate-forming Curtius sequence.', 'confidence': 0.85}, {'page_label': 'p118', 'entity_text': 'Dakin Oxidation', 'canonical_name': 'Dakin Oxidation', 'entity_type': 'reaction_family', 'family_name': 'Dakin Oxidation', 'notes': 'Named activated aryl carbonyl-to-phenol oxidation family overview.', 'confidence': 0.92}, {'page_label': 'p118', 'entity_text': 'O-acylphenol', 'canonical_name': None, 'entity_type': 'intermediate_class', 'family_name': 'Dakin Oxidation', 'notes': 'Canonical rearrangement intermediate produced after hydroperoxide addition and 1,2-aryl shift.', 'confidence': 0.87}, {'page_label': 'p118', 'entity_text': 'substituted phenol', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Dakin Oxidation', 'notes': 'Product class shown as the endpoint of the Dakin oxidation sequence.', 'confidence': 0.85}, {'page_label': 'p119', 'entity_text': 'Dakin Oxidation', 'canonical_name': 'Dakin Oxidation', 'entity_type': 'reaction_family', 'family_name': 'Dakin Oxidation', 'notes': 'Application page family anchor.', 'confidence': 0.92}, {'page_label': 'p119', 'entity_text': 'Vineomycinone B2 methyl ester', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Dakin Oxidation', 'notes': 'Anthraquinone target obtained using a modified Dakin oxidation as a key oxygenation step.', 'confidence': 0.86}, {'page_label': 'p119', 'entity_text': 'Selectively protected L-Dopa derivative', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Dakin Oxidation', 'notes': 'Protected amino-acid derivative accessed through aryl formate formation and hydrolysis after modified Dakin oxidation.', 'confidence': 0.85}, {'page_label': 'p120', 'entity_text': 'Dakin-West Reaction', 'canonical_name': 'Dakin-West Reaction', 'entity_type': 'reaction_family', 'family_name': 'Dakin-West Reaction', 'notes': 'Named amino-acid to alpha-acylamino ketone family overview.', 'confidence': 0.92}, {'page_label': 'p120', 'entity_text': 'oxazolone', 'canonical_name': None, 'entity_type': 'intermediate_class', 'family_name': 'Dakin-West Reaction', 'notes': 'Key cyclic intermediate highlighted in the mechanism of the Dakin-West reaction.', 'confidence': 0.87}, {'page_label': 'p120', 'entity_text': 'alpha-acylamino ketone', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Dakin-West Reaction', 'notes': 'Canonical product class shown in the overview scheme.', 'confidence': 0.85}, {'page_label': 'p121', 'entity_text': 'Dakin-West Reaction', 'canonical_name': 'Dakin-West Reaction', 'entity_type': 'reaction_family', 'family_name': 'Dakin-West Reaction', 'notes': 'Application page family anchor.', 'confidence': 0.92}, {'page_label': 'p121', 'entity_text': 'Ketomethylene pseudopeptide analogue', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Dakin-West Reaction', 'notes': 'Peptidomimetic product assembled using a modified Dakin-West ketone-forming sequence.', 'confidence': 0.86}, {'page_label': 'p121', 'entity_text': '3,9-Diazabicyclo[3.3.1]non-6-en-2-one scaffold', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Dakin-West Reaction', 'notes': 'Constrained bicyclic medicinal-chemistry scaffold prepared by sequential Dakin-West and Pictet-Spengler chemistry.', 'confidence': 0.85}, {'page_label': 'p122', 'entity_text': 'Danheiser Benzannulation', 'canonical_name': 'Danheiser Benzannulation', 'entity_type': 'reaction_family', 'family_name': 'Danheiser Benzannulation', 'notes': 'Named cyclobutenone/diazoketone annulation family overview.', 'confidence': 0.92}, {'page_label': 'p122', 'entity_text': 'vinylketene', 'canonical_name': None, 'entity_type': 'intermediate_class', 'family_name': 'Danheiser Benzannulation', 'notes': 'Key annulation intermediate generated from cyclobutenones or unsaturated alpha-diazoketones.', 'confidence': 0.87}, {'page_label': 'p122', 'entity_text': 'highly substituted aromatic ring', 'canonical_name': None, 'entity_type': 'product_class', 'family_name': 'Danheiser Benzannulation', 'notes': 'Canonical product class shown in the overview diagram.', 'confidence': 0.85}, {'page_label': 'p123', 'entity_text': 'Danheiser Benzannulation', 'canonical_name': 'Danheiser Benzannulation', 'entity_type': 'reaction_family', 'family_name': 'Danheiser Benzannulation', 'notes': 'Application page family anchor.', 'confidence': 0.92}, {'page_label': 'p123', 'entity_text': 'Hylazole', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Danheiser Benzannulation', 'notes': 'Marine carbazole alkaloid target assembled using a modified Danheiser benzannulation from a diazoketone precursor.', 'confidence': 0.86}, {'page_label': 'p123', 'entity_text': '(-)-Cylindrocyclophane F', 'canonical_name': None, 'entity_type': 'target_molecule', 'family_name': 'Danheiser Benzannulation', 'notes': 'Macrocyclic natural product synthesis using Danheiser benzannulation to build an advanced aromatic intermediate.', 'confidence': 0.85}]


def apply_frontmatter_batch17(db_path: str | Path) -> Dict[str, Any]:
    return apply_frontmatter_batch_generic(
        db_path=db_path,
        source_label=SOURCE_LABEL,
        version=FRONTMATTER_BATCH17_VERSION,
        latest_source_zip=LATEST_SOURCE_ZIP,
        batch_no=17,
        page_knowledge_seeds=PAGE_KNOWLEDGE_SEEDS,
        family_pattern_seeds=FAMILY_PATTERN_SEEDS,
        abbreviation_seeds=CURATED_BATCH17_ABBREVIATIONS,
        page_entity_seeds=PAGE_ENTITY_SEEDS,
    )


if __name__ == '__main__':
    from pprint import pprint
    pprint(apply_frontmatter_batch17(Path(__file__).resolve().parent / 'labint.db'))
