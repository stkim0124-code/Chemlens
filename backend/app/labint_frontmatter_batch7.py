from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from app.labint_frontmatter import ensure_frontmatter_schema, normalize_key, _table_exists

FRONTMATTER_BATCH7_VERSION = 'labint_frontmatter_batch7_v1_20260411'

PAGE_KNOWLEDGE_SEEDS = [
    {
        'source_label': 'named_reactions_frontmatter_batch7',
        'page_label': 'p22',
        'page_no': 22,
        'title': 'Aza-Cope Rearrangement – Importance and Mechanism',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'canonical_overview',
        'summary': 'Overview page for the aza-Cope rearrangement describing nitrogen-substituted 1,5-dienes that isomerize through a [3,3]-sigmatropic rearrangement related to the Cope rearrangement, including 1-aza-, 2-aza-, 3-aza-, and diaza variants. The page emphasizes lower activation barriers for charged aza-diene systems and a chairlike transition state.',
        'family_names': 'Aza-Cope Rearrangement',
        'reference_family_name': 'Aza-Cope Rearrangement',
        'notes': 'Explicitly cross-links aza-Cope chemistry to the Cope rearrangement and aza-Claisen terminology; synthetic application shown involves tandem cationic aza-Cope/Mannich cyclization toward FR901483.',
        'image_filename': 'named reactions_74.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch7',
        'page_label': 'p23',
        'page_no': 23,
        'title': 'Aza-Cope Rearrangement – Synthetic Applications',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'application_example',
        'summary': 'Application page for the aza-Cope rearrangement featuring tandem 2-aza-Cope/solvolysis to N-benzylallylglycine and sequential anionic 2-aza-Cope/Mannich strategies in alkaloid synthesis, including (+)-gelsemine and strychnine-related intermediates.',
        'family_names': 'Aza-Cope Rearrangement',
        'reference_family_name': 'Aza-Cope Rearrangement',
        'notes': 'Applications emphasize iminium-ion or alkoxide-triggered aza-Cope manifolds and their merger with Mannich cyclization in complex aza-polycycle assembly.',
        'image_filename': 'named reactions_75.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch7',
        'page_label': 'p24',
        'page_no': 24,
        'title': 'Aza-Wittig Reaction – Importance and Mechanism',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'canonical_overview',
        'summary': 'Overview page for the aza-Wittig reaction describing formation of iminophosphoranes from organic azides and phosphines through the Staudinger reaction, followed by condensation with carbonyl compounds to furnish imines or Schiff bases with triphenylphosphine oxide release.',
        'family_names': 'Aza-Wittig Reaction',
        'reference_family_name': 'Aza-Wittig Reaction',
        'notes': 'Mechanism panel shows phosphazide formation, nitrogen extrusion, iminophosphorane generation, oxazaphosphetane formation, and collapse to a C=N product. Synthetic application on the page gives solid-phase access to trisubstituted guanidines.',
        'image_filename': 'named reactions_76.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch7',
        'page_label': 'p25',
        'page_no': 25,
        'title': 'Aza-Wittig Reaction – Synthetic Applications',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'application_example',
        'summary': 'Application page for the aza-Wittig reaction highlighting intramolecular aza-Wittig cyclizations and tandem sequences in the total syntheses of (-)-stemospironine, (-)-benzomalvin A, and (+)-phloeodictine A1.',
        'family_names': 'Aza-Wittig Reaction',
        'reference_family_name': 'Aza-Wittig Reaction',
        'notes': 'Examples stress Staudinger-initiated iminophosphorane formation, intramolecular aza-Wittig ring closure, and downstream cyclization or retro-Diels–Alder logic in alkaloid synthesis.',
        'image_filename': 'named reactions_77.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch7',
        'page_label': 'p26',
        'page_no': 26,
        'title': 'Aza-[2,3]-Wittig Rearrangement – Importance and Mechanism',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'canonical_overview',
        'summary': 'Overview page for the aza-[2,3]-Wittig rearrangement describing the [2,3]-sigmatropic migration of α-metalated allylic amines to homoallylic amines via an envelope-like five-membered transition state. The page contrasts this process with the oxygen-series Wittig rearrangement and notes distinctions from Stevens and Sommelet-Hauser rearrangements.',
        'family_names': 'Aza-[2,3]-Wittig Rearrangement',
        'reference_family_name': 'Aza-[2,3]-Wittig Rearrangement',
        'notes': 'Synthetic application shown on the page uses aza-[2,3]-Wittig rearrangement as a stereochemical installation step in the synthesis of (+)-kainic acid.',
        'image_filename': 'named reactions_78.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch7',
        'page_label': 'p27',
        'page_no': 27,
        'title': 'Aza-[2,3]-Wittig Rearrangement – Synthetic Applications',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'application_example',
        'summary': 'Application page for the aza-[2,3]-Wittig rearrangement including ring-opening rearrangements of vinylaziridines to unsaturated piperidines and base-promoted rearrangement of N-alkyl-N-allyl α-amino esters to N-alkyl-C-allyl glycine esters.',
        'family_names': 'Aza-[2,3]-Wittig Rearrangement',
        'reference_family_name': 'Aza-[2,3]-Wittig Rearrangement',
        'notes': 'Page highlights strong-base-promoted allylic amine rearrangements, anti diastereoselectivity, and competition with [1,2]-rearrangement pathways in tertiary amine systems.',
        'image_filename': 'named reactions_79.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch7',
        'page_label': 'p28',
        'page_no': 28,
        'title': 'Baeyer-Villiger Oxidation/Rearrangement – Importance and Mechanism',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'canonical_overview',
        'summary': 'Overview page for the Baeyer-Villiger oxidation/rearrangement describing oxidation of ketones to esters and cyclic ketones to lactones or hydroxy acids by peroxyacids. The page summarizes migratory aptitude, stereochemical retention at the migrating carbon, and the Criegee-intermediate mechanism.',
        'family_names': 'Baeyer-Villiger Oxidation/Rearrangement',
        'reference_family_name': 'Baeyer-Villiger Oxidation/Rearrangement',
        'notes': 'Mechanism panel features peroxyacid addition to a ketone, Criegee intermediate formation, migration anti to the peroxide leaving group, and ester/lactone formation; page lists common oxidants such as mCPBA and peracetic acid.',
        'image_filename': 'named reactions_80.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch7',
        'page_label': 'p29',
        'page_no': 29,
        'title': 'Baeyer-Villiger Oxidation/Rearrangement – Synthetic Applications',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'application_example',
        'summary': 'Application page for Baeyer-Villiger oxidation showing access to a C1-methyl glucitol derivative, the functionalized CD ring of Taxol, cage-annulated ethers, and a rearranged intermediate used in the synthesis of (+)-farnesiferol C.',
        'family_names': 'Baeyer-Villiger Oxidation/Rearrangement',
        'reference_family_name': 'Baeyer-Villiger Oxidation/Rearrangement',
        'notes': 'Applications emphasize oxidative ring expansion or oxygen insertion in densely functionalized ketones and cage systems, often using mCPBA or peroxytrifluoroacetic acid conditions.',
        'image_filename': 'named reactions_81.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch7',
        'page_label': 'p30',
        'page_no': 30,
        'title': 'Baker-Venkataraman Rearrangement – Importance and Mechanism',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'canonical_overview',
        'summary': 'Overview page for the Baker-Venkataraman rearrangement describing base-catalyzed conversion of aromatic ortho-acyloxyketones into aromatic β-diketones, an important entry to chromones, flavones, isoflavones, and coumarins.',
        'family_names': 'Baker-Venkataraman Rearrangement',
        'reference_family_name': 'Baker-Venkataraman Rearrangement',
        'notes': 'Mechanism page shows enolate formation at the ketone α-position, intramolecular acyl transfer through a tetrahedral intermediate, and collapse to the aromatic β-diketone; page also includes an aklanonic-acid synthesis example.',
        'image_filename': 'named reactions_82.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch7',
        'page_label': 'p31',
        'page_no': 31,
        'title': 'Baker-Venkataraman Rearrangement – Synthetic Applications',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'application_example',
        'summary': 'Application page for the Baker-Venkataraman rearrangement featuring carbamoyl Baker-Venkataraman access to substituted 4-hydroxycoumarins, construction of the chromone system of stigmatellin A, and domino annulation to a benz[b]indeno[2,1-e]pyran-10,11-dione scaffold.',
        'family_names': 'Baker-Venkataraman Rearrangement',
        'reference_family_name': 'Baker-Venkataraman Rearrangement',
        'notes': 'Applications extend the classical rearrangement beyond simple β-diketone formation to heterocycle and natural-product-oriented synthesis.',
        'image_filename': 'named reactions_83.jpg',
    },
]

CURATED_BATCH7_ABBREVIATIONS = [
    {'alias': 'PTSA', 'canonical_name': 'p-toluenesulfonic acid', 'entity_type': 'acid', 'source_page': 22},
    {'alias': 'KH', 'canonical_name': 'potassium hydride', 'entity_type': 'base', 'source_page': 23},
    {'alias': '18-crown-6', 'canonical_name': '18-crown-6', 'entity_type': 'additive', 'source_page': 23},
    {'alias': 'PPh3', 'canonical_name': 'triphenylphosphine', 'entity_type': 'reagent', 'source_page': 24},
    {'alias': 'NaBH4', 'canonical_name': 'sodium borohydride', 'entity_type': 'reagent', 'source_page': 25},
    {'alias': 'K2CO3', 'canonical_name': 'potassium carbonate', 'entity_type': 'base', 'source_page': 27},
    {'alias': 'MeLi', 'canonical_name': 'methyllithium', 'entity_type': 'reagent', 'source_page': 27},
    {'alias': 'mCPBA', 'canonical_name': 'meta-chloroperoxybenzoic acid', 'entity_type': 'oxidant', 'source_page': 28},
    {'alias': 'TFAA', 'canonical_name': 'trifluoroacetic anhydride', 'entity_type': 'reagent', 'source_page': 29},
    {'alias': 'LiH', 'canonical_name': 'lithium hydride', 'entity_type': 'base', 'source_page': 30},
    {'alias': 's-BuLi', 'canonical_name': 'sec-butyllithium', 'entity_type': 'base', 'source_page': 31},
    {'alias': 'ZnCl2', 'canonical_name': 'zinc chloride', 'entity_type': 'Lewis acid', 'source_page': 31},
    {'alias': 'DIBAH', 'canonical_name': 'diisobutylaluminum hydride', 'entity_type': 'reagent', 'source_page': 31},
]

FAMILY_PATTERN_SEEDS = [
    {
        'family_name': 'Aza-Cope Rearrangement',
        'family_class': 'sigmatropic rearrangement',
        'transformation_type': 'aza-substituted 1,5-diene [3,3]-sigmatropic rearrangement',
        'mechanism_type': 'pericyclic',
        'reactant_pattern_text': 'neutral or cationic aza-1,5-diene / iminium / imine-derived allylamine precursor',
        'product_pattern_text': 'rearranged iminium, enamine, or aza-diene product, often captured in tandem cyclization sequences',
        'key_reagents_clue': 'heat|PTSA|benzene|KH|18-crown-6|Mannich cyclization|iminium ion',
        'common_solvents': 'benzene|THF|MeOH|MeCN',
        'common_conditions': 'thermal or cationic/anionic aza-Cope rearrangement via chairlike transition state; often merged with Mannich cyclization or solvolysis',
        'synonym_names': '1-aza-Cope rearrangement|2-aza-Cope rearrangement|3-aza-Cope rearrangement|diaza-Cope rearrangement|aza-Cope/Mannich rearrangement',
        'description_short': 'Aza-Cope rearrangement is the [3,3]-sigmatropic isomerization of aza-substituted 1,5-dienes, especially powerful in cationic tandem aza-Cope/Mannich alkaloid synthesis.',
        'overview_count': 1,
        'application_count': 1,
        'mechanism_count': 1,
        'seeded_from': 'frontmatter_batch7_named_reaction_pages',
    },
    {
        'family_name': 'Aza-Wittig Reaction',
        'family_class': 'imine formation',
        'transformation_type': 'iminophosphorane condensation with carbonyl compounds to form imines',
        'mechanism_type': 'Staudinger / aza-Wittig sequence',
        'reactant_pattern_text': 'organic azide plus phosphine to iminophosphorane, then aldehyde, ketone, isocyanate, or related electrophile',
        'product_pattern_text': 'imine / Schiff base, guanidine precursor, or ring-closed nitrogen heterocycle after intramolecular aza-Wittig',
        'key_reagents_clue': 'PPh3|azide|iminophosphorane|Staudinger reaction|isothiocyanate|intramolecular aza-Wittig',
        'common_solvents': 'THF|toluene|DMSO|CH2Cl2',
        'common_conditions': 'phosphine-mediated azide reduction to iminophosphorane followed by carbonyl trapping and phosphine oxide extrusion',
        'synonym_names': 'intramolecular aza-Wittig reaction|aza-Wittig cyclization',
        'description_short': 'The aza-Wittig reaction converts azide-derived iminophosphoranes and carbonyl compounds into C=N products and is widely used for heterocycle construction through intra- and intermolecular variants.',
        'overview_count': 1,
        'application_count': 1,
        'mechanism_count': 1,
        'seeded_from': 'frontmatter_batch7_named_reaction_pages',
    },
    {
        'family_name': 'Aza-[2,3]-Wittig Rearrangement',
        'family_class': 'sigmatropic rearrangement',
        'transformation_type': 'metalated allylic amine to homoallylic amine [2,3]-sigmatropic rearrangement',
        'mechanism_type': 'pericyclic / anionic rearrangement',
        'reactant_pattern_text': 'α-metalated allylic tertiary amine or allylic ammonium precursor',
        'product_pattern_text': 'homoallylic secondary or tertiary amine with new C-C bond and transposed heteroatom connectivity',
        'key_reagents_clue': 'RLi|LDA|KH|18-crown-6|MeLi|DBU|K2CO3|DMF',
        'common_solvents': 'THF|DMF|hexanes|MeCN',
        'common_conditions': 'strong-base-induced deprotonation or quaternization followed by concerted aza-[2,3]-sigmatropic rearrangement through an envelope-like transition state',
        'synonym_names': 'aza-2,3-Wittig rearrangement|[2,3]-aza-Wittig rearrangement',
        'description_short': 'Aza-[2,3]-Wittig rearrangement moves allylic amine frameworks into homoallylic amines through a stereospecific anionic [2,3]-sigmatropic shift and is distinct from Stevens or Sommelet-Hauser chemistry.',
        'overview_count': 1,
        'application_count': 1,
        'mechanism_count': 1,
        'seeded_from': 'frontmatter_batch7_named_reaction_pages',
    },
    {
        'family_name': 'Baeyer-Villiger Oxidation/Rearrangement',
        'family_class': 'oxidation / rearrangement',
        'transformation_type': 'ketone to ester or lactone via peroxyacid oxidation',
        'mechanism_type': 'Criegee rearrangement',
        'reactant_pattern_text': 'acyclic ketone or cyclic ketone with peroxyacid or peroxide oxidant',
        'product_pattern_text': 'ester, lactone, or hydroxy-acid derivative after migratory oxygen insertion',
        'key_reagents_clue': 'mCPBA|peroxyacid|peracetic acid|H2O2|TFAA|Criegee intermediate',
        'common_solvents': 'CH2Cl2|DCM|AcOH',
        'common_conditions': 'peroxyacid oxidation with migration anti to peroxide leaving group; migratory aptitude governs regioselectivity',
        'synonym_names': 'Baeyer-Villiger oxidation|Baeyer-Villiger rearrangement|B-V oxidation',
        'description_short': 'Baeyer-Villiger oxidation inserts oxygen next to a carbonyl through Criegee-intermediate formation and alkyl migration, transforming ketones into esters or lactones.',
        'overview_count': 1,
        'application_count': 1,
        'mechanism_count': 1,
        'seeded_from': 'frontmatter_batch7_named_reaction_pages',
    },
    {
        'family_name': 'Baker-Venkataraman Rearrangement',
        'family_class': 'acyl transfer rearrangement',
        'transformation_type': 'ortho-acyloxy aryl ketone to aromatic β-diketone rearrangement',
        'mechanism_type': 'base-promoted intramolecular acyl transfer',
        'reactant_pattern_text': 'aromatic ortho-acyloxyketone or related O-acylated phenolic ketone',
        'product_pattern_text': 'aromatic β-diketone, often cyclized downstream to chromone, coumarin, flavone, or related heterocycle',
        'key_reagents_clue': 'KOH|KOtBu|NaH|Na metal|KH|LiH|s-BuLi|ZnCl2|DIBAH|TFA',
        'common_solvents': 'DMSO|pyridine|THF|DCM|benzene|PhCH3',
        'common_conditions': 'base-induced enolate formation followed by intramolecular acyl transfer; often followed by acid-promoted cyclization to chromones or coumarins',
        'synonym_names': 'carbamoyl Baker-Venkataraman rearrangement|Baker-Venkataraman reaction',
        'description_short': 'The Baker-Venkataraman rearrangement converts ortho-acyloxy aryl ketones into β-diketones that serve as versatile precursors to chromones, flavones, coumarins, and natural-product cores.',
        'overview_count': 1,
        'application_count': 1,
        'mechanism_count': 1,
        'seeded_from': 'frontmatter_batch7_named_reaction_pages',
    },
]

PAGE_ENTITY_SEEDS = [
    {'page_label': 'p22', 'entity_text': 'Cope rearrangement', 'canonical_name': 'Cope Rearrangement', 'entity_type': 'related_reaction', 'family_name': 'Aza-Cope Rearrangement', 'notes': 'Overview explicitly presents aza-Cope as nitrogen-substituted Cope chemistry.', 'confidence': 0.92},
    {'page_label': 'p22', 'entity_text': 'Aza-Claisen rearrangement', 'canonical_name': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)', 'entity_type': 'related_reaction', 'family_name': 'Aza-Cope Rearrangement', 'notes': 'Page labels 3-aza-Cope as aza-Claisen terminology.', 'confidence': 0.93},
    {'page_label': 'p22', 'entity_text': 'FR901483', 'canonical_name': 'FR901483', 'entity_type': 'target_molecule', 'family_name': 'Aza-Cope Rearrangement', 'notes': 'Tandem cationic aza-Cope/Mannich cyclization example.', 'confidence': 0.91},
    {'page_label': 'p23', 'entity_text': 'N-Benzylallylglycine', 'canonical_name': 'N-Benzylallylglycine', 'entity_type': 'target_molecule', 'family_name': 'Aza-Cope Rearrangement', 'notes': '2-aza-Cope/solvolysis product.', 'confidence': 0.9},
    {'page_label': 'p23', 'entity_text': '(+)-Gelsemine', 'canonical_name': '(+)-Gelsemine', 'entity_type': 'target_molecule', 'family_name': 'Aza-Cope Rearrangement', 'notes': 'Sequential anionic 2-aza-Cope/Mannich synthesis example.', 'confidence': 0.91},
    {'page_label': 'p23', 'entity_text': 'Strychnine', 'canonical_name': 'Strychnine', 'entity_type': 'target_molecule', 'family_name': 'Aza-Cope Rearrangement', 'notes': 'Aza-Cope/Mannich strategy referenced for enantioselective strychnine work.', 'confidence': 0.88},
    {'page_label': 'p23', 'entity_text': 'Wieland-Gumlich aldehyde', 'canonical_name': 'Wieland-Gumlich aldehyde', 'entity_type': 'target_molecule', 'family_name': 'Aza-Cope Rearrangement', 'notes': 'Referenced alongside strychnine synthesis.', 'confidence': 0.86},
    {'page_label': 'p24', 'entity_text': 'Staudinger reaction', 'canonical_name': 'Staudinger Reaction', 'entity_type': 'related_reaction', 'family_name': 'Aza-Wittig Reaction', 'notes': 'Explicit precursor step to iminophosphorane formation.', 'confidence': 0.94},
    {'page_label': 'p24', 'entity_text': 'Schiff base', 'canonical_name': 'Schiff base', 'entity_type': 'product_class', 'family_name': 'Aza-Wittig Reaction', 'notes': 'Canonical aza-Wittig product class.', 'confidence': 0.92},
    {'page_label': 'p24', 'entity_text': 'Trisubstituted guanidines', 'canonical_name': 'Trisubstituted guanidines', 'entity_type': 'product_class', 'family_name': 'Aza-Wittig Reaction', 'notes': 'Solid-phase aza-Wittig application.', 'confidence': 0.9},
    {'page_label': 'p25', 'entity_text': '(-)-Stemospironine', 'canonical_name': '(-)-Stemospironine', 'entity_type': 'target_molecule', 'family_name': 'Aza-Wittig Reaction', 'notes': 'Aza-Wittig ring-closure application in Stemona alkaloid synthesis.', 'confidence': 0.91},
    {'page_label': 'p25', 'entity_text': '(-)-Benzomalvin A', 'canonical_name': '(-)-Benzomalvin A', 'entity_type': 'target_molecule', 'family_name': 'Aza-Wittig Reaction', 'notes': 'Two sequential intramolecular aza-Wittig cyclizations used.', 'confidence': 0.91},
    {'page_label': 'p25', 'entity_text': '(+)-Phloeodictine A1', 'canonical_name': '(+)-Phloeodictine A1', 'entity_type': 'target_molecule', 'family_name': 'Aza-Wittig Reaction', 'notes': 'Tandem aza-Wittig/retro-Diels–Alder logic in total synthesis.', 'confidence': 0.9},
    {'page_label': 'p26', 'entity_text': 'Stevens rearrangement', 'canonical_name': 'Stevens Rearrangement', 'entity_type': 'related_reaction', 'family_name': 'Aza-[2,3]-Wittig Rearrangement', 'notes': 'Overview explicitly distinguishes the two reaction classes.', 'confidence': 0.93},
    {'page_label': 'p26', 'entity_text': 'Sommelet-Hauser rearrangement', 'canonical_name': 'Sommelet-Hauser Rearrangement', 'entity_type': 'related_reaction', 'family_name': 'Aza-[2,3]-Wittig Rearrangement', 'notes': 'Overview notes possible competition in benzylic ammonium systems.', 'confidence': 0.93},
    {'page_label': 'p26', 'entity_text': '(+)-Kainic acid', 'canonical_name': '(+)-Kainic acid', 'entity_type': 'target_molecule', 'family_name': 'Aza-[2,3]-Wittig Rearrangement', 'notes': 'Key stereochemical installation example.', 'confidence': 0.91},
    {'page_label': 'p27', 'entity_text': 'Unsaturated piperidines', 'canonical_name': 'Unsaturated piperidines', 'entity_type': 'product_class', 'family_name': 'Aza-[2,3]-Wittig Rearrangement', 'notes': 'Generated from vinylaziridine rearrangement.', 'confidence': 0.89},
    {'page_label': 'p27', 'entity_text': 'N-Alkyl-C-allyl glycine ester', 'canonical_name': 'N-Alkyl-C-allyl glycine ester', 'entity_type': 'product_class', 'family_name': 'Aza-[2,3]-Wittig Rearrangement', 'notes': 'Tertiary amine aza-[2,3]-Wittig product class.', 'confidence': 0.88},
    {'page_label': 'p28', 'entity_text': 'Criegee intermediate', 'canonical_name': 'Criegee intermediate', 'entity_type': 'mechanistic_term', 'family_name': 'Baeyer-Villiger Oxidation/Rearrangement', 'notes': 'Named mechanistic intermediate on the overview page.', 'confidence': 0.94},
    {'page_label': 'p28', 'entity_text': 'Lactone', 'canonical_name': 'Lactone', 'entity_type': 'product_class', 'family_name': 'Baeyer-Villiger Oxidation/Rearrangement', 'notes': 'Canonical cyclic-ketone oxidation product.', 'confidence': 0.9},
    {'page_label': 'p29', 'entity_text': 'C1-Methyl glucitol derivative', 'canonical_name': 'C1-Methyl glucitol derivative', 'entity_type': 'target_molecule', 'family_name': 'Baeyer-Villiger Oxidation/Rearrangement', 'notes': 'Cycloaddition/Baeyer-Villiger sequence example.', 'confidence': 0.88},
    {'page_label': 'p29', 'entity_text': 'Functionalized CD ring of Taxol', 'canonical_name': 'Functionalized CD ring of Taxol', 'entity_type': 'target_fragment', 'family_name': 'Baeyer-Villiger Oxidation/Rearrangement', 'notes': 'Taxol ring-fragment synthesis example.', 'confidence': 0.9},
    {'page_label': 'p29', 'entity_text': 'Cage-annulated ether', 'canonical_name': 'Cage-annulated ether', 'entity_type': 'product_class', 'family_name': 'Baeyer-Villiger Oxidation/Rearrangement', 'notes': 'Cage-ketone oxygen-insertion chemistry example.', 'confidence': 0.88},
    {'page_label': 'p29', 'entity_text': '(+)-Farnesiferol C', 'canonical_name': '(+)-Farnesiferol C', 'entity_type': 'target_molecule', 'family_name': 'Baeyer-Villiger Oxidation/Rearrangement', 'notes': 'Peroxytrifluoroacetic-acid-mediated rearrangement used in total synthesis.', 'confidence': 0.9},
    {'page_label': 'p30', 'entity_text': 'Chromones', 'canonical_name': 'Chromones', 'entity_type': 'product_class', 'family_name': 'Baker-Venkataraman Rearrangement', 'notes': 'Reaction highlighted as entry to chromone scaffolds.', 'confidence': 0.9},
    {'page_label': 'p30', 'entity_text': 'Coumarins', 'canonical_name': 'Coumarins', 'entity_type': 'product_class', 'family_name': 'Baker-Venkataraman Rearrangement', 'notes': 'Reaction highlighted as entry to coumarin scaffolds.', 'confidence': 0.9},
    {'page_label': 'p30', 'entity_text': 'Aklanonic acid', 'canonical_name': 'Aklanonic acid', 'entity_type': 'target_molecule', 'family_name': 'Baker-Venkataraman Rearrangement', 'notes': 'Anthraquinone-based application featured on page.', 'confidence': 0.9},
    {'page_label': 'p31', 'entity_text': '4-Hydroxycoumarin', 'canonical_name': '4-Hydroxycoumarin', 'entity_type': 'target_molecule', 'family_name': 'Baker-Venkataraman Rearrangement', 'notes': 'Carbamoyl Baker-Venkataraman product example.', 'confidence': 0.91},
    {'page_label': 'p31', 'entity_text': 'Stigmatellin A', 'canonical_name': 'Stigmatellin A', 'entity_type': 'target_molecule', 'family_name': 'Baker-Venkataraman Rearrangement', 'notes': 'Chromone-system construction example.', 'confidence': 0.91},
    {'page_label': 'p31', 'entity_text': 'Benz[b]indeno[2,1-e]pyran-10,11-dione', 'canonical_name': 'Benz[b]indeno[2,1-e]pyran-10,11-dione', 'entity_type': 'target_molecule', 'family_name': 'Baker-Venkataraman Rearrangement', 'notes': 'Domino annulation example after initial acyl transfer.', 'confidence': 0.88},
]


def _insert_page_knowledge(con: sqlite3.Connection) -> int:
    inserted = 0
    for row in PAGE_KNOWLEDGE_SEEDS:
        con.execute(
            """
            INSERT INTO manual_page_knowledge (
              source_label, page_label, page_no, title, section_name, page_kind,
              summary, family_names, reference_family_name, notes, image_filename, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(source_label, page_label) DO UPDATE SET
              page_no=excluded.page_no,
              title=excluded.title,
              section_name=excluded.section_name,
              page_kind=excluded.page_kind,
              summary=excluded.summary,
              family_names=excluded.family_names,
              reference_family_name=excluded.reference_family_name,
              notes=excluded.notes,
              image_filename=excluded.image_filename,
              updated_at=datetime('now')
            """,
            (
                row['source_label'], row['page_label'], row.get('page_no'), row.get('title'), row.get('section_name'),
                row['page_kind'], row.get('summary'), row.get('family_names'), row.get('reference_family_name'),
                row.get('notes'), row.get('image_filename')
            )
        )
        inserted += 1
    return inserted


def _seed_abbreviations(con: sqlite3.Connection) -> int:
    if not _table_exists(con, 'abbreviation_aliases'):
        return 0
    changes = 0
    for row in CURATED_BATCH7_ABBREVIATIONS:
        alias_norm = normalize_key(row['alias'])
        canon_norm = normalize_key(row['canonical_name']) if row.get('canonical_name') else None
        con.execute(
            """
            INSERT INTO abbreviation_aliases (
              alias, alias_norm, canonical_name, canonical_name_norm,
              entity_type, smiles, molblock, notes, source_label, source_page, confidence, updated_at
            ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, 'front_matter_seed_batch7', ?, 0.94, datetime('now'))
            ON CONFLICT(alias_norm, canonical_name_norm, entity_type) DO UPDATE SET
              canonical_name=COALESCE(excluded.canonical_name, abbreviation_aliases.canonical_name),
              canonical_name_norm=COALESCE(excluded.canonical_name_norm, abbreviation_aliases.canonical_name_norm),
              notes=COALESCE(abbreviation_aliases.notes, excluded.notes),
              source_label='front_matter_seed_batch7',
              source_page=COALESCE(abbreviation_aliases.source_page, excluded.source_page),
              confidence=MAX(abbreviation_aliases.confidence, excluded.confidence),
              updated_at=datetime('now')
            """,
            (row['alias'], alias_norm, row.get('canonical_name'), canon_norm, row.get('entity_type','chemical_term'), row.get('notes'), row.get('source_page'))
        )
        changes += 1
    return changes


def _seed_family_patterns(con: sqlite3.Connection) -> int:
    if not _table_exists(con, 'reaction_family_patterns'):
        return 0
    changes = 0
    for row in FAMILY_PATTERN_SEEDS:
        norm = normalize_key(row['family_name'])
        con.execute(
            """
            INSERT INTO reaction_family_patterns (
              family_name, family_name_norm, family_class, transformation_type, mechanism_type,
              reactant_pattern_text, product_pattern_text, key_reagents_clue,
              common_solvents, common_conditions, synonym_names, description_short,
              evidence_extract_count, overview_count, application_count, mechanism_count,
              latest_source_zip, latest_updated_at, seeded_from, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, 'named_reactions_frontmatter_batch7', datetime('now'), ?, datetime('now'))
            ON CONFLICT(family_name_norm) DO UPDATE SET
              family_name=excluded.family_name,
              family_class=excluded.family_class,
              transformation_type=excluded.transformation_type,
              mechanism_type=COALESCE(excluded.mechanism_type, reaction_family_patterns.mechanism_type),
              reactant_pattern_text=COALESCE(excluded.reactant_pattern_text, reaction_family_patterns.reactant_pattern_text),
              product_pattern_text=COALESCE(excluded.product_pattern_text, reaction_family_patterns.product_pattern_text),
              key_reagents_clue=COALESCE(excluded.key_reagents_clue, reaction_family_patterns.key_reagents_clue),
              common_solvents=COALESCE(excluded.common_solvents, reaction_family_patterns.common_solvents),
              common_conditions=COALESCE(excluded.common_conditions, reaction_family_patterns.common_conditions),
              synonym_names=CASE
                  WHEN excluded.synonym_names IS NULL OR trim(excluded.synonym_names)='' THEN reaction_family_patterns.synonym_names
                  WHEN reaction_family_patterns.synonym_names IS NULL OR trim(reaction_family_patterns.synonym_names)='' THEN excluded.synonym_names
                  WHEN instr(lower(reaction_family_patterns.synonym_names), lower(excluded.synonym_names)) > 0 THEN reaction_family_patterns.synonym_names
                  ELSE reaction_family_patterns.synonym_names || '|' || excluded.synonym_names
              END,
              description_short=COALESCE(excluded.description_short, reaction_family_patterns.description_short),
              overview_count=MAX(reaction_family_patterns.overview_count, excluded.overview_count),
              application_count=MAX(reaction_family_patterns.application_count, excluded.application_count),
              mechanism_count=MAX(reaction_family_patterns.mechanism_count, excluded.mechanism_count),
              latest_source_zip='named_reactions_frontmatter_batch7',
              latest_updated_at=datetime('now'),
              seeded_from='frontmatter_batch7_named_reaction_pages',
              updated_at=datetime('now')
            """,
            (row['family_name'], norm, row['family_class'], row['transformation_type'], row['mechanism_type'], row['reactant_pattern_text'], row['product_pattern_text'], row['key_reagents_clue'], row['common_solvents'], row['common_conditions'], row['synonym_names'], row['description_short'], row['overview_count'], row['application_count'], row['mechanism_count'], row['seeded_from'])
        )
        changes += 1
    return changes


def _seed_page_entities(con: sqlite3.Connection) -> int:
    if not _table_exists(con, 'manual_page_entities') or not _table_exists(con, 'manual_page_knowledge'):
        return 0
    page_lookup = {row[0]: row[1] for row in con.execute("SELECT page_label, id FROM manual_page_knowledge WHERE source_label='named_reactions_frontmatter_batch7'").fetchall()}
    alias_lookup = {}
    if _table_exists(con, 'abbreviation_aliases'):
        alias_lookup = {row[0]: row[1] for row in con.execute('SELECT alias_norm, id FROM abbreviation_aliases').fetchall()}
    changes = 0
    for row in PAGE_ENTITY_SEEDS:
        page_id = page_lookup.get(row['page_label'])
        if not page_id:
            continue
        text_norm = normalize_key(row['entity_text'])
        alias_id = alias_lookup.get(text_norm) if row['entity_type']=='abbreviation' else None
        con.execute(
            """
            INSERT OR REPLACE INTO manual_page_entities (
              page_knowledge_id, entity_text, entity_text_norm, canonical_name,
              entity_type, alias_id, family_name, notes, confidence, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (page_id, row['entity_text'], text_norm, row.get('canonical_name'), row['entity_type'], alias_id, row.get('family_name'), row.get('notes'), row.get('confidence',0.9))
        )
        changes += 1
    return changes


def get_frontmatter_batch7_counts(db_path: str | Path, con: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
    close_after = False
    if con is None:
        con = sqlite3.connect(str(db_path))
        close_after = True
    try:
        def q(sql: str) -> int:
            return int(con.execute(sql).fetchone()[0])
        return {
            'manual_page_knowledge': q('SELECT COUNT(*) FROM manual_page_knowledge') if _table_exists(con,'manual_page_knowledge') else 0,
            'manual_page_entities': q('SELECT COUNT(*) FROM manual_page_entities') if _table_exists(con,'manual_page_entities') else 0,
            'frontmatter_batch7_pages': q("SELECT COUNT(*) FROM manual_page_knowledge WHERE source_label='named_reactions_frontmatter_batch7'") if _table_exists(con,'manual_page_knowledge') else 0,
            'frontmatter_batch7_aliases': q("SELECT COUNT(*) FROM abbreviation_aliases WHERE source_label='front_matter_seed_batch7'") if _table_exists(con,'abbreviation_aliases') else 0,
            'frontmatter_batch7_families': q("SELECT COUNT(*) FROM reaction_family_patterns WHERE latest_source_zip='named_reactions_frontmatter_batch7'") if _table_exists(con,'reaction_family_patterns') else 0,
        }
    finally:
        if close_after:
            con.close()


def apply_frontmatter_batch7(db_path: str | Path) -> Dict[str, Any]:
    db_path = Path(db_path)
    ensure_frontmatter_schema(db_path)
    con = sqlite3.connect(str(db_path))
    try:
        con.execute('PRAGMA foreign_keys=ON')
        page_rows = _insert_page_knowledge(con)
        alias_rows = _seed_abbreviations(con)
        family_rows = _seed_family_patterns(con)
        entity_rows = _seed_page_entities(con)
        if _table_exists(con, 'labint_schema_meta'):
            con.execute("""
                INSERT INTO labint_schema_meta(key, value) VALUES('frontmatter_batch7_last_applied_at', datetime('now'))
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')
            """)
        con.commit()
        out = get_frontmatter_batch7_counts(db_path, con)
        out.update({
            'page_rows_seeded': page_rows,
            'alias_rows_seeded': alias_rows,
            'family_rows_seeded': family_rows,
            'entity_rows_seeded': entity_rows,
            'frontmatter_batch7_version': FRONTMATTER_BATCH7_VERSION,
        })
        return out
    finally:
        con.close()


if __name__ == '__main__':
    base = Path(__file__).resolve().parent
    for name in ['labint.db', 'labint_round9_bridge_work.db']:
        p = base / name
        if p.exists():
            print(name, apply_frontmatter_batch7(p))
