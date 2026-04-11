from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from app.labint_frontmatter import ensure_frontmatter_schema, normalize_key, _table_exists

FRONTMATTER_BATCH8_VERSION = 'labint_frontmatter_batch8_v1_20260411'

PAGE_KNOWLEDGE_SEEDS = [{'source_label': 'named_reactions_frontmatter_batch8',
  'page_label': 'p32',
  'page_no': 32,
  'title': "Baldwin's Rules / Guidelines for Ring-Closing Reactions – Importance",
  'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
  'page_kind': 'canonical_overview',
  'summary': "Overview page for Baldwin's rules/guidelines for ring-closing reactions, summarizing favored and "
             "disfavored exo/endo ring closures across dig, trig, and tet manifolds. The page frames Baldwin's rules "
             'as stereoelectronic heuristics for intramolecular cyclization feasibility rather than a single reaction '
             'transformation.',
  'family_names': "Baldwin's Rules / Guidelines for Ring-Closing Reactions",
  'reference_family_name': "Baldwin's Rules / Guidelines for Ring-Closing Reactions",
  'notes': 'Contains the key ring-size table and favored/disfavored closure map for exo-dig, exo-trig, exo-tet, '
           'endo-dig, endo-trig, and endo-tet processes.',
  'image_filename': 'named reactions_84.jpg'},
 {'source_label': 'named_reactions_frontmatter_batch8',
  'page_label': 'p33',
  'page_no': 33,
  'title': "Baldwin's Rules / Guidelines for Ring-Closing Reactions – Synthetic Applications",
  'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
  'page_kind': 'application_example',
  'summary': "Application page for Baldwin's rules showing how the guideline predicts successful 5-exo-trig acyl "
             'radical cyclization, 7-exo-tet cyclization, 5-endo-dig cyclization, and 5-exo-tet ether-forming closures '
             'in complex natural-product synthesis.',
  'family_names': "Baldwin's Rules / Guidelines for Ring-Closing Reactions",
  'reference_family_name': "Baldwin's Rules / Guidelines for Ring-Closing Reactions",
  'notes': 'Examples include roseophilin, balanol, (+)-preussin, and lactone-ether formation from epoxy alcohol '
           'substrates.',
  'image_filename': 'named reactions_85.jpg'},
 {'source_label': 'named_reactions_frontmatter_batch8',
  'page_label': 'p34',
  'page_no': 34,
  'title': 'Balz-Schiemann Reaction (Schiemann Reaction) – Importance and Mechanism',
  'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
  'page_kind': 'canonical_overview',
  'summary': 'Overview page for the Balz-Schiemann reaction describing diazotization of aromatic amines to '
             'aryldiazonium tetrafluoroborates and thermal or photochemical decomposition to aryl fluorides. The '
             'mechanism highlights nitrosonium formation, diazonium-salt generation, and fluoride transfer from '
             'tetrafluoroborate.',
  'family_names': 'Balz-Schiemann Reaction',
  'reference_family_name': 'Balz-Schiemann Reaction',
  'notes': 'Synthetic application on the page introduces fluorine into a steroidal aromatic A ring; mechanistic '
           'emphasis is on aryl diazonium tetrafluoroborates as isolable fluorination precursors.',
  'image_filename': 'named reactions_86.jpg'},
 {'source_label': 'named_reactions_frontmatter_batch8',
  'page_label': 'p35',
  'page_no': 35,
  'title': 'Balz-Schiemann Reaction (Schiemann Reaction) – Synthetic Applications',
  'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
  'page_kind': 'application_example',
  'summary': 'Application page for the Balz-Schiemann reaction featuring synthesis of 5-fluoro-D/L-dopa, '
             'difluorobenzophenanthrenes, difluorodibenzocycloalkenimines, and 4-fluoro-1H-pyrrolo[2,3-b]pyridine.',
  'family_names': 'Balz-Schiemann Reaction',
  'reference_family_name': 'Balz-Schiemann Reaction',
  'notes': 'Examples stress late-stage aryl fluorination from aniline precursors after diazotization under HBF4/NaNO2 '
           'conditions.',
  'image_filename': 'named reactions_87.jpg'},
 {'source_label': 'named_reactions_frontmatter_batch8',
  'page_label': 'p36',
  'page_no': 36,
  'title': 'Bamford-Stevens-Shapiro Olefination – Importance and Mechanism',
  'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
  'page_kind': 'canonical_overview',
  'summary': 'Overview page for Bamford-Stevens/Shapiro olefination describing base-promoted decomposition of '
             'arylsulfonyl hydrazones to alkenes. The page contrasts carbene/carbocation pathways of Bamford-Stevens '
             'conditions with the alkenyllithium-forming Shapiro reaction under strong organolithium conditions.',
  'family_names': 'Bamford-Stevens-Shapiro Olefination',
  'reference_family_name': 'Bamford-Stevens-Shapiro Olefination',
  'notes': 'Mechanism panel covers diazoalkane formation, carbene pathways, [1,2]-hydrogen shifts, and alkenyllithium '
           'formation/protonation under Shapiro conditions.',
  'image_filename': 'named reactions_88.jpg'},
 {'source_label': 'named_reactions_frontmatter_batch8',
  'page_label': 'p37',
  'page_no': 37,
  'title': 'Bamford-Stevens-Shapiro Olefination – Synthetic Applications',
  'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
  'page_kind': 'application_example',
  'summary': 'Application page for Bamford-Stevens/Shapiro olefination including total syntheses of (-)-phytocassane '
             'D, exo-glycals from glycosyl cyanides, chiral indenones such as verbindene, and (-)-isoclavukerin.',
  'family_names': 'Bamford-Stevens-Shapiro Olefination',
  'reference_family_name': 'Bamford-Stevens-Shapiro Olefination',
  'notes': 'Applications illustrate both direct olefination and use of alkenyllithium or carbene-like intermediates in '
           'complex terpenoid and carbohydrate settings.',
  'image_filename': 'named reactions_89.jpg'},
 {'source_label': 'named_reactions_frontmatter_batch8',
  'page_label': 'p38',
  'page_no': 38,
  'title': 'Barbier Coupling Reaction – Importance and Mechanism',
  'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
  'page_kind': 'canonical_overview',
  'summary': 'Overview page for the Barbier coupling reaction describing in situ generation of organometallic reagents '
             'in the presence of carbonyl compounds to furnish alcohols after workup. The page notes mechanistic '
             'overlap with Grignard chemistry and discusses both concerted and radical stepwise pathways.',
  'family_names': 'Barbier Coupling Reaction',
  'reference_family_name': 'Barbier Coupling Reaction',
  'notes': 'Page emphasizes operational simplicity, aqueous compatibility for some metals, and immediate trapping of '
           'unstable organometallic species by aldehydes or ketones.',
  'image_filename': 'named reactions_90.jpg'},
 {'source_label': 'named_reactions_frontmatter_batch8',
  'page_label': 'p39',
  'page_no': 39,
  'title': 'Barbier Coupling Reaction – Synthetic Applications',
  'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
  'page_kind': 'application_example',
  'summary': 'Application page for the Barbier coupling reaction highlighting stereoselective installation of allylic '
             'fragments in the syntheses of saponaceolide B, talpinine, talcarpine, stypodiol, epistypodiol, and '
             'styptotriol.',
  'family_names': 'Barbier Coupling Reaction',
  'reference_family_name': 'Barbier Coupling Reaction',
  'notes': 'Applications underscore in situ allylmetal generation, suppression of allylic rearrangement, and merger '
           'with oxy-Cope or cyclization logic in terpene and alkaloid synthesis.',
  'image_filename': 'named reactions_91.jpg'},
 {'source_label': 'named_reactions_frontmatter_batch8',
  'page_label': 'p40',
  'page_no': 40,
  'title': 'Bartoli Indole Synthesis – Importance and Mechanism',
  'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
  'page_kind': 'canonical_overview',
  'summary': 'Overview page for the Bartoli indole synthesis describing conversion of substituted nitroarenes or '
             'nitrosoarenes to indoles with excess vinyl or substituted alkenyl Grignard reagents at low temperature '
             'followed by acidic workup. The mechanism involves nitro-group addition, nitrosoarene formation, '
             '[3,3]-sigmatropic rearrangement, and intramolecular attack to the indole core.',
  'family_names': 'Bartoli Indole Synthesis',
  'reference_family_name': 'Bartoli Indole Synthesis',
  'notes': 'The page stresses the importance of ortho substitution on the nitroarene and shows both substrate scope '
           'and a multistep mechanistic scheme.',
  'image_filename': 'named reactions_92.jpg'},
 {'source_label': 'named_reactions_frontmatter_batch8',
  'page_label': 'p41',
  'page_no': 41,
  'title': 'Bartoli Indole Synthesis – Synthetic Applications',
  'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
  'page_kind': 'application_example',
  'summary': 'Application page for the Bartoli indole synthesis featuring access to azaindoles, hippadine, a glycogen '
             'synthase kinase-3 inhibitor scaffold, and brominated pyrrolyl dehydroabietate derivatives.',
  'family_names': 'Bartoli Indole Synthesis',
  'reference_family_name': 'Bartoli Indole Synthesis',
  'notes': 'Applications show heteroaryl nitroarene and nitroabietane substrates and reinforce the value of Bartoli '
           'chemistry for rapid indole installation in medicinal and natural-product settings.',
  'image_filename': 'named reactions_93.jpg'}]

CURATED_BATCH8_ABBREVIATIONS = [{'alias': 'AIBN', 'canonical_name': 'azobisisobutyronitrile', 'entity_type': 'radical_initiator', 'source_page': 33},
 {'alias': 'Bu3SnH', 'canonical_name': 'tributyltin hydride', 'entity_type': 'reagent', 'source_page': 33},
 {'alias': 'CSA', 'canonical_name': 'camphorsulfonic acid', 'entity_type': 'acid', 'source_page': 33},
 {'alias': 'Et2O', 'canonical_name': 'diethyl ether', 'entity_type': 'solvent', 'source_page': 35},
 {'alias': 'HBF4', 'canonical_name': 'tetrafluoroboric acid', 'entity_type': 'acid', 'source_page': 34},
 {'alias': 'PPTS', 'canonical_name': 'pyridinium p-toluenesulfonate', 'entity_type': 'acid', 'source_page': 37},
 {'alias': 'TsNHNH2', 'canonical_name': 'p-toluenesulfonyl hydrazide', 'entity_type': 'reagent', 'source_page': 36},
 {'alias': 'LDA', 'canonical_name': 'lithium diisopropylamide', 'entity_type': 'base', 'source_page': 37},
 {'alias': 'NaH2PO2', 'canonical_name': 'sodium hypophosphite', 'entity_type': 'reducing_agent', 'source_page': 37},
 {'alias': 'Sn', 'canonical_name': 'tin', 'entity_type': 'metal', 'source_page': 38},
 {'alias': 'In', 'canonical_name': 'indium', 'entity_type': 'metal', 'source_page': 38},
 {'alias': 'Sm', 'canonical_name': 'samarium', 'entity_type': 'metal', 'source_page': 38},
 {'alias': 'Bal2', 'canonical_name': 'barium iodide', 'entity_type': 'salt', 'source_page': 39},
 {'alias': 'TsOH', 'canonical_name': 'p-toluenesulfonic acid', 'entity_type': 'acid', 'source_page': 41},
 {'alias': 'GSK3',
  'canonical_name': 'glycogen synthase kinase 3',
  'entity_type': 'biological_target',
  'source_page': 41}]

FAMILY_PATTERN_SEEDS = [{'family_name': "Baldwin's Rules / Guidelines for Ring-Closing Reactions",
  'family_class': 'cyclization guideline / stereoelectronic heuristic',
  'transformation_type': 'predictive framework for exo/endo dig/trig/tet ring-closing reactions',
  'mechanism_type': 'stereoelectronic feasibility guideline',
  'reactant_pattern_text': 'intramolecular nucleophile/radical/organometallic tether approaching an alkene, alkyne, or '
                           'heteroatom-bearing electrophile during ring closure',
  'product_pattern_text': 'ring-closed product whose feasibility is categorized as favored or disfavored by exo/endo '
                          'and dig/trig/tet topology',
  'key_reagents_clue': 'cyclization|5-exo-trig|7-exo-tet|5-endo-dig|5-exo-tet|AIBN|Bu3SnH|CSA',
  'common_solvents': 'benzene|DCM|THF|MeOH',
  'common_conditions': 'intramolecular ring closure interpreted through favored/disfavored Baldwin trajectories rather '
                       'than a single reagent set',
  'synonym_names': "Baldwin's rules|Baldwin ring-closure guidelines|ring-closing rules",
  'description_short': "Baldwin's rules are stereoelectronic guidelines that predict whether exo/endo dig, trig, and "
                       'tet intramolecular ring closures are favored in synthesis planning.',
  'overview_count': 1,
  'application_count': 1,
  'mechanism_count': 0,
  'seeded_from': 'frontmatter_batch8_named_reaction_pages'},
 {'family_name': 'Balz-Schiemann Reaction',
  'family_class': 'aryl fluorination / diazonium decomposition',
  'transformation_type': 'aryl amine to aryl fluoride via aryldiazonium tetrafluoroborate',
  'mechanism_type': 'diazotization followed by thermal or photochemical decomposition',
  'reactant_pattern_text': 'electron-rich or electron-poor aniline / aryl amine precursor subjected to diazotization '
                           'in HBF4',
  'product_pattern_text': 'aryl fluoride produced after loss of nitrogen from the aryldiazonium tetrafluoroborate salt',
  'key_reagents_clue': 'NaNO2|HBF4|BF4-|heat|xylene|aryl diazonium tetrafluoroborate',
  'common_solvents': 'xylene|Et2O|pyridine-HF',
  'common_conditions': 'diazotization at low temperature followed by thermal decomposition or photolysis of isolated '
                       'diazonium tetrafluoroborates',
  'synonym_names': 'Schiemann reaction|Balz-Schiemann fluorination',
  'description_short': 'The Balz-Schiemann reaction converts aromatic amines into aryl fluorides by way of isolable '
                       'aryldiazonium tetrafluoroborate salts.',
  'overview_count': 1,
  'application_count': 1,
  'mechanism_count': 1,
  'seeded_from': 'frontmatter_batch8_named_reaction_pages'},
 {'family_name': 'Bamford-Stevens-Shapiro Olefination',
  'family_class': 'olefination / tosylhydrazone decomposition',
  'transformation_type': 'aldehyde or ketone tosylhydrazone to alkene under base-promoted Bamford-Stevens or Shapiro '
                         'conditions',
  'mechanism_type': 'diazo/carbenoid or alkenyllithium-mediated elimination',
  'reactant_pattern_text': 'tosylhydrazone or trisylhydrazone derived from aldehydes, ketones, or cyclic carbonyl '
                           'compounds',
  'product_pattern_text': 'substituted alkene, diene, or alkenyllithium-derived trapping product after nitrogen '
                          'extrusion',
  'key_reagents_clue': 'TsNHNH2|BuLi|LDA|THF|alkenyllithium|diazoalkane',
  'common_solvents': 'THF|toluene|diglyme|1,4-dioxane',
  'common_conditions': 'strong base, often organolithium; Shapiro variant generates alkenyllithium intermediates prior '
                       'to protonation or electrophile trapping',
  'synonym_names': 'Bamford-Stevens reaction|Shapiro reaction|tosylhydrazone olefination',
  'description_short': 'Bamford-Stevens/Shapiro olefination transforms tosylhydrazones into alkenes, either through '
                       'diazo/carbene chemistry or through alkenyllithium intermediates under organolithium '
                       'conditions.',
  'overview_count': 1,
  'application_count': 1,
  'mechanism_count': 1,
  'seeded_from': 'frontmatter_batch8_named_reaction_pages'},
 {'family_name': 'Barbier Coupling Reaction',
  'family_class': 'carbonyl addition / organometallic coupling',
  'transformation_type': 'in situ organometallic generation from alkyl/allyl halides with direct addition to carbonyl '
                         'compounds',
  'mechanism_type': 'concerted or radical stepwise carbonyl addition',
  'reactant_pattern_text': 'aldehyde or ketone combined with alkyl, allyl, vinyl, or benzyl halide and a reactive '
                           'metal in one pot',
  'product_pattern_text': 'alcohol product after immediate carbonyl trapping and workup',
  'key_reagents_clue': 'Mg|Zn|In|Sm|Sn|allyl halide|carbonyl compound|work-up',
  'common_solvents': 'water|THF|Et2O|MeOH',
  'common_conditions': 'organometallic reagent formed in situ in the presence of the carbonyl electrophile, often '
                       'suppressing decomposition or rearrangement of unstable organometallics',
  'synonym_names': 'Barbier reaction|in situ Barbier coupling',
  'description_short': 'The Barbier reaction generates an organometallic reagent in the presence of a carbonyl '
                       'compound so that addition occurs immediately without prior isolation of the Grignard-like '
                       'species.',
  'overview_count': 1,
  'application_count': 1,
  'mechanism_count': 1,
  'seeded_from': 'frontmatter_batch8_named_reaction_pages'},
 {'family_name': 'Bartoli Indole Synthesis',
  'family_class': 'heterocycle synthesis / indole annulation',
  'transformation_type': 'substituted nitroarene or nitrosoarene to indole using excess vinyl or alkenyl Grignard '
                         'reagent',
  'mechanism_type': 'nitro-group addition followed by rearrangement and intramolecular cyclization',
  'reactant_pattern_text': 'ortho-substituted nitroarene or nitrosoarene treated with vinylmagnesium or substituted '
                           'alkenylmagnesium reagents at low temperature',
  'product_pattern_text': 'indole or azaindole bearing substituents inherited from the nitroarene and Grignard '
                          'coupling partner',
  'key_reagents_clue': 'vinylmagnesium bromide|alkenyl Grignard|THF|-40 to -78 C|acidic work-up|nitroarene',
  'common_solvents': 'THF|Et2O|Bu2O',
  'common_conditions': 'low-temperature addition of 2-3 equivalents of Grignard reagent to nitroarenes, followed by '
                       'acidic workup to reveal the indole nucleus',
  'synonym_names': 'Bartoli reaction|Bartoli indole annulation',
  'description_short': 'The Bartoli indole synthesis rapidly converts ortho-substituted nitroarenes into indoles or '
                       'azaindoles using excess vinyl or alkenyl Grignard reagents.',
  'overview_count': 1,
  'application_count': 1,
  'mechanism_count': 1,
  'seeded_from': 'frontmatter_batch8_named_reaction_pages'}]

PAGE_ENTITY_SEEDS = [{'page_label': 'p32',
  'entity_text': '5-exo-trig',
  'canonical_name': '5-exo-trig cyclization',
  'entity_type': 'mechanistic_term',
  'family_name': "Baldwin's Rules / Guidelines for Ring-Closing Reactions",
  'notes': 'Favored ring-closing mode explicitly listed in Baldwin summary table.',
  'confidence': 0.94},
 {'page_label': 'p32',
  'entity_text': '5-endo-trig',
  'canonical_name': '5-endo-trig cyclization',
  'entity_type': 'mechanistic_term',
  'family_name': "Baldwin's Rules / Guidelines for Ring-Closing Reactions",
  'notes': 'Disfavored ring-closing mode highlighted in Baldwin rules chart.',
  'confidence': 0.93},
 {'page_label': 'p32',
  'entity_text': '7-exo-dig',
  'canonical_name': '7-exo-dig cyclization',
  'entity_type': 'mechanistic_term',
  'family_name': "Baldwin's Rules / Guidelines for Ring-Closing Reactions",
  'notes': 'Favored exo-dig trajectory shown in the ring-closure map.',
  'confidence': 0.92},
 {'page_label': 'p33',
  'entity_text': 'ent-(-)-Roseophilin',
  'canonical_name': 'ent-(-)-Roseophilin',
  'entity_type': 'target_molecule',
  'family_name': "Baldwin's Rules / Guidelines for Ring-Closing Reactions",
  'notes': '5-exo-trig acyl radical cyclization example.',
  'confidence': 0.91},
 {'page_label': 'p33',
  'entity_text': 'Balanol',
  'canonical_name': 'Balanol',
  'entity_type': 'target_molecule',
  'family_name': "Baldwin's Rules / Guidelines for Ring-Closing Reactions",
  'notes': '7-exo-tet cyclization example.',
  'confidence': 0.91},
 {'page_label': 'p33',
  'entity_text': '(+)-Preussin',
  'canonical_name': '(+)-Preussin',
  'entity_type': 'target_molecule',
  'family_name': "Baldwin's Rules / Guidelines for Ring-Closing Reactions",
  'notes': '5-endo-dig cyclization example.',
  'confidence': 0.91},
 {'page_label': 'p34',
  'entity_text': 'Aryldiazonium tetrafluoroborate',
  'canonical_name': 'Aryldiazonium tetrafluoroborate',
  'entity_type': 'intermediate',
  'family_name': 'Balz-Schiemann Reaction',
  'notes': 'Key isolable intermediate in Balz-Schiemann fluorination.',
  'confidence': 0.95},
 {'page_label': 'p34',
  'entity_text': 'Nitrosonium ion',
  'canonical_name': 'Nitrosonium ion',
  'entity_type': 'mechanistic_term',
  'family_name': 'Balz-Schiemann Reaction',
  'notes': 'Mechanism explicitly labels nitrosonium formation.',
  'confidence': 0.93},
 {'page_label': 'p34',
  'entity_text': 'Aryl fluoride',
  'canonical_name': 'Aryl fluoride',
  'entity_type': 'product_class',
  'family_name': 'Balz-Schiemann Reaction',
  'notes': 'Canonical product class of the reaction.',
  'confidence': 0.9},
 {'page_label': 'p35',
  'entity_text': '5-Fluoro-D/L-dopa hydrobromide',
  'canonical_name': '5-Fluoro-D/L-dopa hydrobromide',
  'entity_type': 'target_molecule',
  'family_name': 'Balz-Schiemann Reaction',
  'notes': 'Medicinal amino-acid fluorination example.',
  'confidence': 0.9},
 {'page_label': 'p35',
  'entity_text': '5,8-Difluorobenzo[c]phenanthrene',
  'canonical_name': '5,8-Difluorobenzo[c]phenanthrene',
  'entity_type': 'target_molecule',
  'family_name': 'Balz-Schiemann Reaction',
  'notes': 'Polycyclic aromatic fluorination example.',
  'confidence': 0.89},
 {'page_label': 'p35',
  'entity_text': '4-Fluoro-1H-pyrrolo[2,3-b]pyridine',
  'canonical_name': '4-Fluoro-1H-pyrrolo[2,3-b]pyridine',
  'entity_type': 'target_molecule',
  'family_name': 'Balz-Schiemann Reaction',
  'notes': 'Azaindole fluorination example.',
  'confidence': 0.9},
 {'page_label': 'p36',
  'entity_text': 'Tosylhydrazone',
  'canonical_name': 'Tosylhydrazone',
  'entity_type': 'intermediate',
  'family_name': 'Bamford-Stevens-Shapiro Olefination',
  'notes': 'Starting derivative used for Bamford-Stevens/Shapiro chemistry.',
  'confidence': 0.95},
 {'page_label': 'p36',
  'entity_text': 'Diazoalkane',
  'canonical_name': 'Diazoalkane',
  'entity_type': 'intermediate',
  'family_name': 'Bamford-Stevens-Shapiro Olefination',
  'notes': 'Mechanistic intermediate on Bamford-Stevens pathway.',
  'confidence': 0.93},
 {'page_label': 'p36',
  'entity_text': 'Alkenyllithium',
  'canonical_name': 'Alkenyllithium',
  'entity_type': 'intermediate',
  'family_name': 'Bamford-Stevens-Shapiro Olefination',
  'notes': 'Characteristic Shapiro-reaction intermediate.',
  'confidence': 0.94},
 {'page_label': 'p37',
  'entity_text': '(-)-Phytocassane D',
  'canonical_name': '(-)-Phytocassane D',
  'entity_type': 'target_molecule',
  'family_name': 'Bamford-Stevens-Shapiro Olefination',
  'notes': 'Tricyclic terpenoid synthesis example.',
  'confidence': 0.9},
 {'page_label': 'p37',
  'entity_text': 'exo-Glycal',
  'canonical_name': 'exo-Glycal',
  'entity_type': 'product_class',
  'family_name': 'Bamford-Stevens-Shapiro Olefination',
  'notes': 'Carbohydrate olefination product class from glycosyl cyanides.',
  'confidence': 0.89},
 {'page_label': 'p37',
  'entity_text': '(-)-Isoclavukerin',
  'canonical_name': '(-)-Isoclavukerin',
  'entity_type': 'target_molecule',
  'family_name': 'Bamford-Stevens-Shapiro Olefination',
  'notes': 'Bicyclic trisylhydrazone olefination example.',
  'confidence': 0.9},
 {'page_label': 'p38',
  'entity_text': 'Grignard reagent',
  'canonical_name': 'Grignard reagent',
  'entity_type': 'related_reaction',
  'family_name': 'Barbier Coupling Reaction',
  'notes': 'Overview explicitly compares Barbier to Grignard chemistry.',
  'confidence': 0.92},
 {'page_label': 'p38',
  'entity_text': 'SET',
  'canonical_name': 'Single electron transfer',
  'entity_type': 'mechanistic_term',
  'family_name': 'Barbier Coupling Reaction',
  'notes': 'Mechanistic chart presents SET pathway for organometallic generation.',
  'confidence': 0.92},
 {'page_label': 'p38',
  'entity_text': '1°, 2°, or 3° alcohols',
  'canonical_name': 'Alcohols',
  'entity_type': 'product_class',
  'family_name': 'Barbier Coupling Reaction',
  'notes': 'Canonical outcome after carbonyl addition and workup.',
  'confidence': 0.9},
 {'page_label': 'p39',
  'entity_text': 'Talpinine',
  'canonical_name': 'Talpinine',
  'entity_type': 'target_molecule',
  'family_name': 'Barbier Coupling Reaction',
  'notes': 'Anionic oxy-Cope sequence following Barbier addition.',
  'confidence': 0.9},
 {'page_label': 'p39',
  'entity_text': 'Stypodiol',
  'canonical_name': 'Stypodiol',
  'entity_type': 'target_molecule',
  'family_name': 'Barbier Coupling Reaction',
  'notes': 'Sonochemical Barbier reaction example in diterpene synthesis.',
  'confidence': 0.9},
 {'page_label': 'p39',
  'entity_text': 'Epistypodiol',
  'canonical_name': 'Epistypodiol',
  'entity_type': 'target_molecule',
  'family_name': 'Barbier Coupling Reaction',
  'notes': 'Epimeric diterpene product from Barbier/cyclization sequence.',
  'confidence': 0.89},
 {'page_label': 'p40',
  'entity_text': 'Nitrosoarene',
  'canonical_name': 'Nitrosoarene',
  'entity_type': 'intermediate',
  'family_name': 'Bartoli Indole Synthesis',
  'notes': 'Mechanistic intermediate after initial attack on the nitro group.',
  'confidence': 0.93},
 {'page_label': 'p40',
  'entity_text': '[3,3]-Sigmatropic rearrangement',
  'canonical_name': '[3,3]-Sigmatropic rearrangement',
  'entity_type': 'mechanistic_term',
  'family_name': 'Bartoli Indole Synthesis',
  'notes': 'Mechanism explicitly invokes a [3,3]-rearrangement.',
  'confidence': 0.92},
 {'page_label': 'p40',
  'entity_text': 'Substituted indole',
  'canonical_name': 'Substituted indole',
  'entity_type': 'product_class',
  'family_name': 'Bartoli Indole Synthesis',
  'notes': 'Canonical product class from the Bartoli reaction.',
  'confidence': 0.9},
 {'page_label': 'p41',
  'entity_text': '4-Bromo-7-chloro-6-azaindole',
  'canonical_name': '4-Bromo-7-chloro-6-azaindole',
  'entity_type': 'target_molecule',
  'family_name': 'Bartoli Indole Synthesis',
  'notes': 'Heteroaryl nitroarene application example.',
  'confidence': 0.89},
 {'page_label': 'p41',
  'entity_text': 'Hippadine',
  'canonical_name': 'Hippadine',
  'entity_type': 'target_molecule',
  'family_name': 'Bartoli Indole Synthesis',
  'notes': 'Pyrrolophenanthridone alkaloid synthesis example.',
  'confidence': 0.9},
 {'page_label': 'p41',
  'entity_text': 'Potent inhibitor of glycogen synthase kinase-3',
  'canonical_name': 'GSK3 inhibitor scaffold',
  'entity_type': 'target_molecule',
  'family_name': 'Bartoli Indole Synthesis',
  'notes': 'Medicinal-chemistry application of Bartoli indole synthesis.',
  'confidence': 0.88}]


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
    for row in CURATED_BATCH8_ABBREVIATIONS:
        alias_norm = normalize_key(row['alias'])
        canon_norm = normalize_key(row['canonical_name']) if row.get('canonical_name') else None
        con.execute(
            """
            INSERT INTO abbreviation_aliases (
              alias, alias_norm, canonical_name, canonical_name_norm,
              entity_type, smiles, molblock, notes, source_label, source_page, confidence, updated_at
            ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, 'front_matter_seed_batch8', ?, 0.94, datetime('now'))
            ON CONFLICT(alias_norm, canonical_name_norm, entity_type) DO UPDATE SET
              canonical_name=COALESCE(excluded.canonical_name, abbreviation_aliases.canonical_name),
              canonical_name_norm=COALESCE(excluded.canonical_name_norm, abbreviation_aliases.canonical_name_norm),
              notes=COALESCE(abbreviation_aliases.notes, excluded.notes),
              source_label='front_matter_seed_batch8',
              source_page=COALESCE(abbreviation_aliases.source_page, excluded.source_page),
              confidence=MAX(abbreviation_aliases.confidence, excluded.confidence),
              updated_at=datetime('now')
            """,
            (row['alias'], alias_norm, row.get('canonical_name'), canon_norm, row.get('entity_type', 'chemical_term'),
             row.get('notes'), row.get('source_page'))
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, 'named_reactions_frontmatter_batch8', datetime('now'), ?, datetime('now'))
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
              synonym_names=COALESCE(excluded.synonym_names, reaction_family_patterns.synonym_names),
              description_short=COALESCE(excluded.description_short, reaction_family_patterns.description_short),
              overview_count=MAX(reaction_family_patterns.overview_count, excluded.overview_count),
              application_count=MAX(reaction_family_patterns.application_count, excluded.application_count),
              mechanism_count=MAX(reaction_family_patterns.mechanism_count, excluded.mechanism_count),
              latest_source_zip='named_reactions_frontmatter_batch8',
              latest_updated_at=datetime('now'),
              seeded_from=excluded.seeded_from,
              updated_at=datetime('now')
            """,
            (
                row['family_name'], norm, row.get('family_class'), row.get('transformation_type'),
                row.get('mechanism_type'), row.get('reactant_pattern_text'), row.get('product_pattern_text'),
                row.get('key_reagents_clue'), row.get('common_solvents'), row.get('common_conditions'),
                row.get('synonym_names'), row.get('description_short'), row.get('overview_count', 0),
                row.get('application_count', 0), row.get('mechanism_count', 0), row.get('seeded_from')
            )
        )
        changes += 1
    return changes


def _seed_page_entities(con: sqlite3.Connection) -> int:
    if not _table_exists(con, 'manual_page_entities') or not _table_exists(con, 'manual_page_knowledge'):
        return 0
    page_lookup = {row[0]: row[1] for row in con.execute("SELECT page_label, id FROM manual_page_knowledge WHERE source_label='named_reactions_frontmatter_batch8'").fetchall()}
    alias_lookup = {}
    if _table_exists(con, 'abbreviation_aliases'):
        alias_lookup = {row[0]: row[1] for row in con.execute('SELECT alias_norm, id FROM abbreviation_aliases').fetchall()}
    changes = 0
    for row in PAGE_ENTITY_SEEDS:
        page_id = page_lookup.get(row['page_label'])
        if not page_id:
            continue
        text_norm = normalize_key(row['entity_text'])
        alias_id = alias_lookup.get(text_norm) if row['entity_type'] == 'abbreviation' else None
        con.execute(
            """
            INSERT OR REPLACE INTO manual_page_entities (
              page_knowledge_id, entity_text, entity_text_norm, canonical_name,
              entity_type, alias_id, family_name, notes, confidence, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                page_id, row['entity_text'], text_norm, row.get('canonical_name'),
                row['entity_type'], alias_id, row.get('family_name'),
                row.get('notes'), row.get('confidence', 0.9)
            )
        )
        changes += 1
    return changes


def get_frontmatter_batch8_counts(db_path: str | Path, con: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
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
            'frontmatter_batch8_pages': q("SELECT COUNT(*) FROM manual_page_knowledge WHERE source_label='named_reactions_frontmatter_batch8'") if _table_exists(con,'manual_page_knowledge') else 0,
            'frontmatter_batch8_aliases': q("SELECT COUNT(*) FROM abbreviation_aliases WHERE source_label='front_matter_seed_batch8'") if _table_exists(con,'abbreviation_aliases') else 0,
            'frontmatter_batch8_families': q("SELECT COUNT(*) FROM reaction_family_patterns WHERE latest_source_zip='named_reactions_frontmatter_batch8'") if _table_exists(con,'reaction_family_patterns') else 0,
        }
    finally:
        if close_after:
            con.close()


def apply_frontmatter_batch8(db_path: str | Path) -> Dict[str, Any]:
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
                INSERT INTO labint_schema_meta(key, value) VALUES('frontmatter_batch8_last_applied_at', datetime('now'))
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')
            """)
        con.commit()
        out = get_frontmatter_batch8_counts(db_path, con)
        out.update({
            'page_rows_seeded': page_rows,
            'alias_rows_seeded': alias_rows,
            'family_rows_seeded': family_rows,
            'entity_rows_seeded': entity_rows,
            'frontmatter_batch8_version': FRONTMATTER_BATCH8_VERSION,
        })
        return out
    finally:
        con.close()


if __name__ == '__main__':
    base = Path(__file__).resolve().parent
    for name in ['labint.db', 'labint_round9_bridge_work.db']:
        p = base / name
        if p.exists():
            print(name, apply_frontmatter_batch8(p))
