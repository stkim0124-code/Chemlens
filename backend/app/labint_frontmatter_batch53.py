from __future__ import annotations
from pathlib import Path
from typing import Any, Dict

from app.labint_frontmatter_batch_restore_utils import (
    apply_frontmatter_batch_generic,
    load_abbreviation_seeds,
    load_family_pattern_seeds,
    load_page_knowledge_seeds,
)

FRONTMATTER_BATCH53_VERSION = 'labint_frontmatter_batch53_v1_20260415'
SOURCE_LABEL = 'named_reactions_frontmatter_batch53'
LATEST_SOURCE_ZIP = 'backend_batch15_53_frontmatter_cumulative_patch_20260412.zip'

PAGE_KNOWLEDGE_SEEDS = load_page_knowledge_seeds(53, SOURCE_LABEL)
FAMILY_PATTERN_SEEDS = load_family_pattern_seeds(53, FRONTMATTER_BATCH53_VERSION)
BATCH53_SOURCE_PAGES = {
    "s-BuLi": 467,
    "TMEDA": 467,
    "CuTC": 467,
    "POCl3": 468,
    "DMF": 468,
    "PDC": 469,
    "HMPA": 471,
    "TMSI": 471,
    "InCl3": 473,
    "PdCl2": 474,
    "CuCl": 474,
    "TBHP": 474,
    "Na2PdCl4": 474,
    "Cu(OAc)2": 475,
    "(n-Bu)4NCl": 475,
}
CURATED_BATCH53_ABBREVIATIONS = load_abbreviation_seeds(
    53,
    SOURCE_LABEL,
    source_pages=BATCH53_SOURCE_PAGES,
    default_note='batch53 frontmatter abbreviation seed',
)
PAGE_ENTITY_SEEDS = [
    {'page_label': 'p466', 'entity_text': 'Ullmann Reaction / Coupling / Biaryl Synthesis', 'entity_type': 'reaction_family', 'family_name': 'Ullmann Reaction / Coupling / Biaryl Synthesis', 'notes': 'Canonical overview for copper-mediated Ullmann biaryl coupling.', 'canonical_name': 'Ullmann Reaction / Coupling / Biaryl Synthesis', 'confidence': 0.99},
    {'page_label': 'p466', 'entity_text': 'symmetrical biaryl', 'entity_type': 'product_class', 'family_name': 'Ullmann Reaction / Coupling / Biaryl Synthesis', 'notes': 'Classical product type in the original Ullmann homocoupling.', 'canonical_name': 'symmetrical biaryl', 'confidence': 0.87},
    {'page_label': 'p466', 'entity_text': 'unsymmetrical biaryl', 'entity_type': 'product_class', 'family_name': 'Ullmann Reaction / Coupling / Biaryl Synthesis', 'notes': 'Cross-coupled biaryl product class under Ullmann conditions.', 'canonical_name': 'unsymmetrical biaryl', 'confidence': 0.86},
    {'page_label': 'p466', 'entity_text': 'Cu-powder', 'entity_type': 'reagent', 'family_name': 'Ullmann Reaction / Coupling / Biaryl Synthesis', 'notes': 'Activated copper powder used in classical Ullmann coupling.', 'canonical_name': 'copper powder', 'confidence': 0.85},
    {'page_label': 'p466', 'entity_text': 'aryl halide', 'entity_type': 'substrate_class', 'family_name': 'Ullmann Reaction / Coupling / Biaryl Synthesis', 'notes': 'Core electrophile class for biaryl-forming Ullmann coupling.', 'canonical_name': 'aryl halide', 'confidence': 0.84},

    {'page_label': 'p467', 'entity_text': 'Tortuosine', 'entity_type': 'target_molecule', 'family_name': 'Ullmann Reaction / Coupling / Biaryl Synthesis', 'notes': 'Total synthesis featuring a Ziegler-modified Ullmann biaryl coupling.', 'canonical_name': 'Tortuosine', 'confidence': 0.88},
    {'page_label': 'p467', 'entity_text': '(-)-Mastigophorene A', 'entity_type': 'target_molecule', 'family_name': 'Ullmann Reaction / Coupling / Biaryl Synthesis', 'notes': 'Atroposelective oxazoline-mediated Ullmann coupling application.', 'canonical_name': '(-)-Mastigophorene A', 'confidence': 0.87},
    {'page_label': 'p467', 'entity_text': 'Taspine', 'entity_type': 'target_molecule', 'family_name': 'Ullmann Reaction / Coupling / Biaryl Synthesis', 'notes': 'Biaryl bond established by classical Ullmann coupling.', 'canonical_name': 'Taspine', 'confidence': 0.88},
    {'page_label': 'p467', 'entity_text': "[2,2'-Bithiophenyl]", 'entity_type': 'target_molecule', 'family_name': 'Ullmann Reaction / Coupling / Biaryl Synthesis', 'notes': 'Example of heteroaryl coupling under CuTC-mediated mild Ullmann conditions.', 'canonical_name': "[2,2'-Bithiophenyl]", 'confidence': 0.84},
    {'page_label': 'p467', 'entity_text': 'CuTC', 'entity_type': 'abbreviation', 'family_name': 'Ullmann Reaction / Coupling / Biaryl Synthesis', 'notes': 'Copper(I) thiophene-2-carboxylate used in a mild room-temperature Ullmann example.', 'canonical_name': 'copper(I) thiophene-2-carboxylate', 'confidence': 0.90},

    {'page_label': 'p468', 'entity_text': 'Vilsmeier-Haack Formylation', 'entity_type': 'reaction_family', 'family_name': 'Vilsmeier-Haack Formylation', 'notes': 'Canonical overview for Vilsmeier-Haack electrophilic formylation.', 'canonical_name': 'Vilsmeier-Haack Formylation', 'confidence': 0.99},
    {'page_label': 'p468', 'entity_text': 'Vilsmeier reagent', 'entity_type': 'intermediate', 'family_name': 'Vilsmeier-Haack Formylation', 'notes': 'Electrophilic iminium/chloroiminium reagent generated from DMF and an acid chloride.', 'canonical_name': 'Vilsmeier reagent', 'confidence': 0.93},
    {'page_label': 'p468', 'entity_text': 'POCl3', 'entity_type': 'abbreviation', 'family_name': 'Vilsmeier-Haack Formylation', 'notes': 'Phosphoryl chloride abbreviation central to Vilsmeier reagent formation.', 'canonical_name': 'phosphoryl chloride', 'confidence': 0.90},
    {'page_label': 'p468', 'entity_text': 'DMF', 'entity_type': 'abbreviation', 'family_name': 'Vilsmeier-Haack Formylation', 'notes': 'N,N-dimethylformamide abbreviation used to form the Vilsmeier reagent.', 'canonical_name': 'N,N-dimethylformamide', 'confidence': 0.90},
    {'page_label': 'p468', 'entity_text': 'heteroaromatic aldehyde', 'entity_type': 'product_class', 'family_name': 'Vilsmeier-Haack Formylation', 'notes': 'Representative product class from heteroaromatic Vilsmeier formylation.', 'canonical_name': 'heteroaromatic aldehyde', 'confidence': 0.85},

    {'page_label': 'p469', 'entity_text': '(-)-Calanolide A', 'entity_type': 'target_molecule', 'family_name': 'Vilsmeier-Haack Formylation', 'notes': 'Coumarin lactone substrate formylated regioselectively in total synthesis.', 'canonical_name': '(-)-Calanolide A', 'confidence': 0.88},
    {'page_label': 'p469', 'entity_text': 'FR-900482', 'entity_type': 'target_molecule', 'family_name': 'Vilsmeier-Haack Formylation', 'notes': 'Indole substrate formylated at C3 prior to decarbonylation en route to the core.', 'canonical_name': 'FR-900482', 'confidence': 0.88},
    {'page_label': 'p469', 'entity_text': '(±)-Illudin C', 'entity_type': 'target_molecule', 'family_name': 'Vilsmeier-Haack Formylation', 'notes': 'Enol ether precursor converted to an aldehyde regioisomer using Vilsmeier conditions.', 'canonical_name': '(±)-Illudin C', 'confidence': 0.87},
    {'page_label': 'p469', 'entity_text': 'Homofascaplysin C', 'entity_type': 'target_molecule', 'family_name': 'Vilsmeier-Haack Formylation', 'notes': '12H-pyridoindole pigment prepared through Vilsmeier formylation.', 'canonical_name': 'Homofascaplysin C', 'confidence': 0.87},
    {'page_label': 'p469', 'entity_text': 'PDC', 'entity_type': 'abbreviation', 'family_name': 'Vilsmeier-Haack Formylation', 'notes': 'Oxidant used after reduction/deprotection in one application sequence.', 'canonical_name': 'pyridinium dichromate', 'confidence': 0.89},

    {'page_label': 'p470', 'entity_text': 'Vinylcyclopropane-Cyclopentene Rearrangement', 'entity_type': 'reaction_family', 'family_name': 'Vinylcyclopropane-Cyclopentene Rearrangement', 'notes': 'Canonical overview for rearrangement of vinylcyclopropanes to cyclopentenes.', 'canonical_name': 'Vinylcyclopropane-Cyclopentene Rearrangement', 'confidence': 0.99},
    {'page_label': 'p470', 'entity_text': 'vinylcyclopropane', 'entity_type': 'substrate_class', 'family_name': 'Vinylcyclopropane-Cyclopentene Rearrangement', 'notes': 'Characteristic strained substrate class of the rearrangement.', 'canonical_name': 'vinylcyclopropane', 'confidence': 0.90},
    {'page_label': 'p470', 'entity_text': 'cyclopentene', 'entity_type': 'product_class', 'family_name': 'Vinylcyclopropane-Cyclopentene Rearrangement', 'notes': 'Rearranged cyclopentene product class obtained from vinylcyclopropanes.', 'canonical_name': 'cyclopentene', 'confidence': 0.88},
    {'page_label': 'p470', 'entity_text': 'retro-ene reaction', 'entity_type': 'competing_pathway', 'family_name': 'Vinylcyclopropane-Cyclopentene Rearrangement', 'notes': 'Competing pathway emphasized in the overview.', 'canonical_name': 'retro-ene reaction', 'confidence': 0.85},
    {'page_label': 'p470', 'entity_text': 'flash vacuum pyrolysis', 'entity_type': 'condition_keyword', 'family_name': 'Vinylcyclopropane-Cyclopentene Rearrangement', 'notes': 'Common high-temperature method used to trigger the rearrangement.', 'canonical_name': 'flash vacuum pyrolysis', 'confidence': 0.84},

    {'page_label': 'p471', 'entity_text': '(+)-Δ9(12)-Capnellene', 'entity_type': 'target_molecule', 'family_name': 'Vinylcyclopropane-Cyclopentene Rearrangement', 'notes': 'Photoinduced rearrangement route to both enantiomers of capnellene derivative.', 'canonical_name': '(+)-Δ9(12)-Capnellene', 'confidence': 0.86},
    {'page_label': 'p471', 'entity_text': '(+)-Antheridic acid', 'entity_type': 'target_molecule', 'family_name': 'Vinylcyclopropane-Cyclopentene Rearrangement', 'notes': 'Lewis-acid-mediated rearrangement used in enantioselective synthesis.', 'canonical_name': '(+)-Antheridic acid', 'confidence': 0.87},
    {'page_label': 'p471', 'entity_text': '(-)-Retigeranic acid', 'entity_type': 'target_molecule', 'family_name': 'Vinylcyclopropane-Cyclopentene Rearrangement', 'notes': 'Thermal rearrangement of a bicyclic enolate-derived precursor used in total synthesis.', 'canonical_name': '(-)-Retigeranic acid', 'confidence': 0.88},
    {'page_label': 'p471', 'entity_text': '(-)-Specionin', 'entity_type': 'target_molecule', 'family_name': 'Vinylcyclopropane-Cyclopentene Rearrangement', 'notes': 'Low-temperature rearrangement application in iridoid synthesis.', 'canonical_name': '(-)-Specionin', 'confidence': 0.87},
    {'page_label': 'p471', 'entity_text': 'TMSI', 'entity_type': 'abbreviation', 'family_name': 'Vinylcyclopropane-Cyclopentene Rearrangement', 'notes': 'Trimethylsilyl iodide used after vinylcyclopropane formation in a specionin example.', 'canonical_name': 'trimethylsilyl iodide', 'confidence': 0.89},

    {'page_label': 'p472', 'entity_text': 'von Pechmann Reaction', 'entity_type': 'reaction_family', 'family_name': 'von Pechmann Reaction', 'notes': 'Canonical overview for acid-catalyzed coumarin synthesis from phenols and β-keto esters.', 'canonical_name': 'von Pechmann Reaction', 'confidence': 0.99},
    {'page_label': 'p472', 'entity_text': 'coumarin', 'entity_type': 'product_class', 'family_name': 'von Pechmann Reaction', 'notes': 'Characteristic heterocyclic product of the von Pechmann condensation.', 'canonical_name': 'coumarin', 'confidence': 0.91},
    {'page_label': 'p472', 'entity_text': 'β-keto ester', 'entity_type': 'substrate_class', 'family_name': 'von Pechmann Reaction', 'notes': 'Key carbonyl partner in the classical Pechmann condensation.', 'canonical_name': 'β-keto ester', 'confidence': 0.88},
    {'page_label': 'p472', 'entity_text': 'resorcinol', 'entity_type': 'substrate_example', 'family_name': 'von Pechmann Reaction', 'notes': 'Classical phenol substrate in early Pechmann examples.', 'canonical_name': 'resorcinol', 'confidence': 0.84},
    {'page_label': 'p472', 'entity_text': '7-hydroxycoumarin', 'entity_type': 'product_example', 'family_name': 'von Pechmann Reaction', 'notes': 'Historical coumarin example generated from resorcinol and malic acid.', 'canonical_name': '7-hydroxycoumarin', 'confidence': 0.85},

    {'page_label': 'p473', 'entity_text': 'Pyridoangelicin', 'entity_type': 'target_molecule', 'family_name': 'von Pechmann Reaction', 'notes': 'Angular pyridopsoralen scaffold assembled using von Pechmann reaction conditions.', 'canonical_name': 'Pyridoangelicin', 'confidence': 0.86},
    {'page_label': 'p473', 'entity_text': '4-substituted coumarin', 'entity_type': 'product_class', 'family_name': 'von Pechmann Reaction', 'notes': 'Mild InCl3-catalyzed preparation highlighted in the applications page.', 'canonical_name': '4-substituted coumarin', 'confidence': 0.86},
    {'page_label': 'p473', 'entity_text': 'benzopsoralen derivative', 'entity_type': 'product_class', 'family_name': 'von Pechmann Reaction', 'notes': 'PUVA-related scaffold accessed through Pechmann condensation.', 'canonical_name': 'benzopsoralen derivative', 'confidence': 0.84},
    {'page_label': 'p473', 'entity_text': 'retinoid X receptor modulator', 'entity_type': 'bioactive_scaffold', 'family_name': 'von Pechmann Reaction', 'notes': 'Dimer-selective RXR modulator synthesized using a Pechmann step.', 'canonical_name': 'dimer-selective retinoid X receptor modulator', 'confidence': 0.83},
    {'page_label': 'p473', 'entity_text': 'InCl3', 'entity_type': 'abbreviation', 'family_name': 'von Pechmann Reaction', 'notes': 'Indium(III) chloride used as a mild catalyst for 4-substituted coumarin synthesis.', 'canonical_name': 'indium(III) chloride', 'confidence': 0.89},

    {'page_label': 'p474', 'entity_text': 'Wacker Oxidation', 'entity_type': 'reaction_family', 'family_name': 'Wacker Oxidation', 'notes': 'Canonical overview for Pd/Cu-catalyzed oxidation of alkenes.', 'canonical_name': 'Wacker Oxidation', 'confidence': 0.99},
    {'page_label': 'p474', 'entity_text': 'Wacker-Smidt process', 'entity_type': 'variant', 'family_name': 'Wacker Oxidation', 'notes': 'Industrial oxidation of ethylene to acetaldehyde highlighted on the overview page.', 'canonical_name': 'Wacker-Smidt process', 'confidence': 0.93},
    {'page_label': 'p474', 'entity_text': 'methyl ketone', 'entity_type': 'product_class', 'family_name': 'Wacker Oxidation', 'notes': 'Typical product class from terminal alkene Wacker oxidation.', 'canonical_name': 'methyl ketone', 'confidence': 0.90},
    {'page_label': 'p474', 'entity_text': 'Wacker-type oxidation', 'entity_type': 'variant', 'family_name': 'Wacker Oxidation', 'notes': 'Intramolecular or heteroatom-assisted nucleophile capture variant of Wacker oxidation.', 'canonical_name': 'Wacker-type oxidation', 'confidence': 0.91},
    {'page_label': 'p474', 'entity_text': 'Na2PdCl4', 'entity_type': 'abbreviation', 'family_name': 'Wacker Oxidation', 'notes': 'Catalyst precursor used in one carbonyl-selective oxidation variant.', 'canonical_name': 'sodium tetrachloropalladate(II)', 'confidence': 0.90},

    {'page_label': 'p475', 'entity_text': '(-)-Sclerophytin A', 'entity_type': 'target_molecule', 'family_name': 'Wacker Oxidation', 'notes': 'Putative structure synthesis employing Wacker oxidation of a terminal alkene.', 'canonical_name': '(-)-Sclerophytin A', 'confidence': 0.87},
    {'page_label': 'p475', 'entity_text': '(-)-Hennoxazole A', 'entity_type': 'target_molecule', 'family_name': 'Wacker Oxidation', 'notes': 'Modified Wacker oxidation used in the synthesis of the antiviral natural product.', 'canonical_name': '(-)-Hennoxazole A', 'confidence': 0.88},
    {'page_label': 'p475', 'entity_text': 'skeleton of himandrine', 'entity_type': 'target_fragment', 'family_name': 'Wacker Oxidation', 'notes': 'Intramolecular Wacker-type oxidation used to forge the hexacyclic himandrine skeleton.', 'canonical_name': 'skeleton of himandrine', 'confidence': 0.86},
    {'page_label': 'p475', 'entity_text': '18-epi-tricyclic core of garsubellin A', 'entity_type': 'target_fragment', 'family_name': 'Wacker Oxidation', 'notes': 'Wacker process used to assemble the tetrahydrofuran ring in a garsubellin A core.', 'canonical_name': '18-epi-tricyclic core of garsubellin A', 'confidence': 0.86},
    {'page_label': 'p475', 'entity_text': 'Cu(OAc)2', 'entity_type': 'abbreviation', 'family_name': 'Wacker Oxidation', 'notes': 'Copper(II) acetate used as co-oxidant in a modified Wacker oxidation example.', 'canonical_name': 'copper(II) acetate', 'confidence': 0.89},
]


def apply_frontmatter_batch53(db_path: str | Path) -> Dict[str, Any]:
    return apply_frontmatter_batch_generic(
        db_path=db_path,
        source_label=SOURCE_LABEL,
        version=FRONTMATTER_BATCH53_VERSION,
        latest_source_zip=LATEST_SOURCE_ZIP,
        batch_no=53,
        page_knowledge_seeds=PAGE_KNOWLEDGE_SEEDS,
        family_pattern_seeds=FAMILY_PATTERN_SEEDS,
        abbreviation_seeds=CURATED_BATCH53_ABBREVIATIONS,
        page_entity_seeds=PAGE_ENTITY_SEEDS,
        replace_existing_page_entities=True,
    )
