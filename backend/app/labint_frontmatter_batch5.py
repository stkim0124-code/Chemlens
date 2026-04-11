
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from app.labint_frontmatter import ensure_frontmatter_schema, normalize_key, _table_exists

FRONTMATTER_BATCH5_VERSION = 'labint_frontmatter_batch5_v1_20260411'

PAGE_KNOWLEDGE_SEEDS = [
    {
        'source_label': 'named_reactions_frontmatter_batch5',
        'page_label': 'p11',
        'page_no': 11,
        'title': 'Alkene (Olefin) Metathesis – Synthetic Applications',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'application_example',
        'summary': 'Synthetic application page for alkene metathesis highlighting RCM/CM-based total synthesis examples with Schrock catalyst, first-generation Grubbs catalyst, and modified Grubbs catalyst.',
        'family_names': 'Alkene (Olefin) Metathesis',
        'reference_family_name': 'Alkene (Olefin) Metathesis',
        'notes': 'Examples include (-)-Cylindrocyclophane F, (-)-Griseoviridin, and (+)-Pleuralureatin via ring-closing or cross metathesis.',
        'image_filename': 'named reactions_54.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch5',
        'page_label': 'p2',
        'page_no': 2,
        'title': 'Acetoacetic Ester Synthesis – Importance and Mechanism',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'canonical_overview',
        'summary': 'Overview page defining acetoacetic ester synthesis as C-alkylation of acetoacetic esters followed by hydrolysis/decarboxylation to ketones. Shows C2/C4 alkylation manifold and mechanism.',
        'family_names': 'Acetoacetic Ester Synthesis',
        'reference_family_name': 'Acetoacetic Ester Synthesis',
        'notes': 'Key bases listed include NaH, NaOR, LiHMDS, and NaHMDS; page explicitly relates the method to retro-Claisen and Krapcho-style variants.',
        'image_filename': 'named reactions_55.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch5',
        'page_label': 'p3',
        'page_no': 3,
        'title': 'Acetoacetic Ester Synthesis – Synthetic Applications',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'application_example',
        'summary': 'Application page for acetoacetic ester synthesis showing access to bicyclo[2.1.1]hexane, 2-hydroxy-3-acetylfuran derivatives, tetrahydrofuran/tetrahydropyran products, and aza-bicyclic alkaloid cores.',
        'family_names': 'Acetoacetic Ester Synthesis',
        'reference_family_name': 'Acetoacetic Ester Synthesis',
        'notes': 'Examples cite solanoeclepin A precursor synthesis, Feist-Bénary reaction access, and stemona alkaloid core construction.',
        'image_filename': 'named reactions_56.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch5',
        'page_label': 'p4',
        'page_no': 4,
        'title': 'Acyloin Condensation – Importance and Mechanism',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'canonical_overview',
        'summary': 'Overview page for acyloin condensation describing reductive dimerization of esters with sodium metal to form acyloins, including TMSCl-assisted variants and mechanistic alternatives.',
        'family_names': 'Acyloin Condensation',
        'reference_family_name': 'Acyloin Condensation',
        'notes': 'Mechanism panel shows radical and epoxide-intermediate proposals and conversion of esters to bis-siloxyalkenes then acyloins.',
        'image_filename': 'named reactions_57.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch5',
        'page_label': 'p5',
        'page_no': 5,
        'title': 'Acyloin Condensation – Synthetic Applications',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'application_example',
        'summary': 'Synthetic applications of acyloin condensation including ultrasound-promoted cyclizations to 3-, 4-, 5-, and 6-membered rings, tricyclic diterpene core synthesis, and bicyclic diketone formation.',
        'family_names': 'Acyloin Condensation',
        'reference_family_name': 'Acyloin Condensation',
        'notes': 'Products include anopterine-related tricyclo[3.2.1.0]decane substructure and Lewis-acid-promoted bicyclic diketones derived from cyclic acyloins.',
        'image_filename': 'named reactions_58.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch5',
        'page_label': 'p6',
        'page_no': 6,
        'title': 'Alder (Ene) Reaction (Hydro-Allyl Addition) – Importance and Mechanism',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'canonical_overview',
        'summary': 'Overview page for the ene reaction, a pericyclic addition of an alkene bearing an allylic hydrogen to an enophile, with mechanistic comparison to the Diels–Alder reaction.',
        'family_names': 'Alder (Ene) Reaction (Hydro-Allyl Addition)',
        'reference_family_name': 'Alder (Ene) Reaction (Hydro-Allyl Addition)',
        'notes': 'Page explicitly mentions carbonyl-ene, aza-ene, imino-ene, hetero-ene, and metallo-ene variants together with a concerted six-membered transition state.',
        'image_filename': 'named reactions_59.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch5',
        'page_label': 'p7',
        'page_no': 7,
        'title': 'Alder (Ene) Reaction (Hydro-Allyl Addition) – Synthetic Applications',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'application_example',
        'summary': 'Synthetic applications page for the ene reaction featuring aza-ene additions with MVK, metal-promoted ene cyclization en route to kainic acid, and tandem oxy-Cope/transannular ene chemistry for arteannuin M.',
        'family_names': 'Alder (Ene) Reaction (Hydro-Allyl Addition)',
        'reference_family_name': 'Alder (Ene) Reaction (Hydro-Allyl Addition)',
        'notes': 'Notable targets shown include imidazo[1,2,3-jk][1,8]naphthyridine, (-)-α-kainic acid, and (+)-arteannuin M.',
        'image_filename': 'named reactions_60.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch5',
        'page_label': 'p8',
        'page_no': 8,
        'title': 'Aldol Reaction – Importance and Mechanism',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'canonical_overview',
        'summary': 'Overview page for the aldol reaction showing addition of enols/enolates to aldehydes or ketones, β-hydroxy carbonyl formation, and dehydration to α,β-unsaturated carbonyl compounds.',
        'family_names': 'Aldol Reaction',
        'reference_family_name': 'Aldol Reaction',
        'notes': 'Page contrasts classical base- or acid-catalyzed aldol reactions with preformed enolates and discusses Zimmerman–Traxler control, Mukaiyama aldol, and direct catalytic asymmetric aldol variants.',
        'image_filename': 'named reactions_61.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch5',
        'page_label': 'p9',
        'page_no': 9,
        'title': 'Aldol Reaction – Synthetic Applications',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'application_example',
        'summary': 'Aldol application page covering stereocontrolled aldol steps in the synthesis of denticulatin A, rhizoxin D, and fostriecin.',
        'family_names': 'Aldol Reaction',
        'reference_family_name': 'Aldol Reaction',
        'notes': 'Examples highlight boron-enolate aldol chemistry, matched aldol coupling, double-diastereodifferentiating aldol reaction, and direct catalytic asymmetric aldol reactions.',
        'image_filename': 'named reactions_62.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch5',
        'page_label': 'p10',
        'page_no': 10,
        'title': 'Alkene (Olefin) Metathesis – Importance and Mechanism',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'canonical_overview',
        'summary': 'Overview page for alkene metathesis describing redistribution of carbon–carbon double bonds and the major subclasses RCM, CM, ROM, ROMP, and ADMET with a ruthenium-carbene mechanism.',
        'family_names': 'Alkene (Olefin) Metathesis',
        'reference_family_name': 'Alkene (Olefin) Metathesis',
        'notes': 'Mechanism panel shows metallacyclobutane pathways and highlights modern metathesis catalysts with Grubbs-type Ru carbenes.',
        'image_filename': 'named reactions_63.jpg',
    },
]

CURATED_BATCH5_ABBREVIATIONS = [
    {'alias': 'RCM', 'canonical_name': 'ring-closing metathesis', 'entity_type': 'reaction_term', 'source_page': 10},
    {'alias': 'CM', 'canonical_name': 'cross metathesis', 'entity_type': 'reaction_term', 'source_page': 10},
    {'alias': 'ROM', 'canonical_name': 'ring-opening metathesis', 'entity_type': 'reaction_term', 'source_page': 10},
    {'alias': 'ROMP', 'canonical_name': 'ring-opening metathesis polymerization', 'entity_type': 'reaction_term', 'source_page': 10},
    {'alias': 'ADMET', 'canonical_name': 'acyclic diene metathesis polymerization', 'entity_type': 'reaction_term', 'source_page': 10},
    {'alias': 'MVK', 'canonical_name': 'methyl vinyl ketone', 'entity_type': 'reagent', 'source_page': 7},
    {'alias': 'PPTS', 'canonical_name': 'pyridinium p-toluenesulfonate', 'entity_type': 'reagent', 'source_page': 11},
    {'alias': 'BBr3', 'canonical_name': 'boron tribromide', 'entity_type': 'reagent', 'source_page': 11},
    {'alias': 'TMSCl', 'canonical_name': 'trimethylsilyl chloride', 'entity_type': 'reagent', 'source_page': 4},
    {'alias': 'LiAlH4', 'canonical_name': 'lithium aluminium hydride', 'entity_type': 'reagent', 'source_page': 3},
    {'alias': 'DBU', 'canonical_name': '1,8-diazabicyclo[5.4.0]undec-7-ene', 'entity_type': 'base', 'source_page': 7},
    {'alias': 'KOtBu', 'canonical_name': 'potassium tert-butoxide', 'entity_type': 'base', 'source_page': 3},
    {'alias': 'LiClO4', 'canonical_name': 'lithium perchlorate', 'entity_type': 'additive', 'source_page': 3},
    {'alias': 'NaHMDS', 'canonical_name': 'sodium bis(trimethylsilyl)amide', 'entity_type': 'base', 'source_page': 2},
    {'alias': 'LiHMDS', 'canonical_name': 'lithium bis(trimethylsilyl)amide', 'entity_type': 'base', 'source_page': 2},
]

FAMILY_PATTERN_SEEDS = [
    {
        'family_name': 'Acetoacetic Ester Synthesis',
        'family_class': 'alkylation',
        'transformation_type': 'β-keto ester alkylation and decarboxylative ketone synthesis',
        'mechanism_type': 'enolate alkylation',
        'reactant_pattern_text': 'acetoacetic ester / β-keto ester enolate + alkyl halide or equivalent electrophile',
        'product_pattern_text': 'α-alkylated acetoacetic ester -> β-keto acid -> ketone after hydrolysis and decarboxylation',
        'key_reagents_clue': 'NaH|NaOR|LiHMDS|NaHMDS|alkyl halide|H3O+|heat',
        'common_solvents': 'THF|MeOH',
        'common_conditions': 'base-promoted deprotonation; alkylation; acidic hydrolysis; heat-induced decarboxylation',
        'synonym_names': None,
        'description_short': 'Classical acetoacetic ester synthesis uses C-alkylation of acetoacetic esters followed by hydrolysis and decarboxylation to furnish mono- or disubstituted ketones.',
        'overview_count': 1,
        'application_count': 1,
        'mechanism_count': 1,
        'seeded_from': 'frontmatter_batch5_named_reaction_pages',
    },
    {
        'family_name': 'Acyloin Condensation',
        'family_class': 'reductive coupling',
        'transformation_type': 'ester reductive dimerization to α-hydroxy ketones',
        'mechanism_type': 'reductive coupling',
        'reactant_pattern_text': 'aliphatic ester or diester + sodium metal',
        'product_pattern_text': 'acyloin (α-hydroxy ketone) or cyclic acyloin derivative',
        'key_reagents_clue': 'Na|TMSCl|xylene|H+|H2O',
        'common_solvents': 'xylene|toluene|PhMe',
        'common_conditions': 'molten or dispersed sodium, frequently with TMSCl; acidic workup',
        'synonym_names': None,
        'description_short': 'Acyloin condensation reductively couples esters to acyloins and is especially valuable for cyclic and intramolecular acyloin formation.',
        'overview_count': 1,
        'application_count': 1,
        'mechanism_count': 1,
        'seeded_from': 'frontmatter_batch5_named_reaction_pages',
    },
    {
        'family_name': 'Alder (Ene) Reaction (Hydro-Allyl Addition)',
        'family_class': 'pericyclic addition',
        'transformation_type': 'ene reaction / allylic transposition addition',
        'mechanism_type': 'pericyclic',
        'reactant_pattern_text': 'ene (alkene with allylic hydrogen) + enophile',
        'product_pattern_text': 'allylic transposition adduct / hydro-allylation product',
        'key_reagents_clue': 'enophile|Lewis acid|MVK|DBU',
        'common_solvents': 'CH3CN|H2O|PhMe|DCM',
        'common_conditions': 'thermal or Lewis-acid-promoted ene reaction; can appear as aza-ene or carbonyl-ene variants',
        'synonym_names': 'ene reaction|hydro-allyl addition',
        'description_short': 'The Alder ene reaction is a pericyclic addition of an allylic alkene to an enophile with concomitant allylic hydrogen transfer and double-bond migration.',
        'overview_count': 1,
        'application_count': 1,
        'mechanism_count': 1,
        'seeded_from': 'frontmatter_batch5_named_reaction_pages',
    },
    {
        'family_name': 'Aldol Reaction',
        'family_class': 'carbon-carbon bond formation',
        'transformation_type': 'enolate addition to carbonyl / β-hydroxy carbonyl formation',
        'mechanism_type': 'enolate addition',
        'reactant_pattern_text': 'aldehyde or ketone electrophile + enol/enolate or silyl enol ether nucleophile',
        'product_pattern_text': 'β-hydroxy carbonyl compound and often α,β-unsaturated carbonyl after dehydration',
        'key_reagents_clue': 'enolate|boron enolate|Ti|Si|Zn|acid|base|TiCl4|i-Pr2NEt',
        'common_solvents': 'THF|DCM|Et2O',
        'common_conditions': 'acid- or base-catalyzed aldol; preformed enolates; Mukaiyama and asymmetric aldol variants',
        'synonym_names': None,
        'description_short': 'The aldol reaction forges C–C bonds between enol/enolate donors and carbonyl electrophiles to produce β-hydroxy carbonyl compounds, often with high stereocontrol.',
        'overview_count': 1,
        'application_count': 1,
        'mechanism_count': 1,
        'seeded_from': 'frontmatter_batch5_named_reaction_pages',
    },
    {
        'family_name': 'Alkene (Olefin) Metathesis',
        'family_class': 'metathesis',
        'transformation_type': 'olefin exchange via metal carbene / alkene redistribution',
        'mechanism_type': 'organometallic',
        'reactant_pattern_text': 'alkene substrate(s) undergoing RCM, CM, ROM, ROMP, or ADMET under Ru/Mo carbene catalysis',
        'product_pattern_text': 'new olefin connectivity / ring-closed or cross-metathesis product with ethylene or alkene byproduct',
        'key_reagents_clue': 'Grubbs catalyst|Schrock catalyst|Ru carbene|RCM|CM|ROMP|ADMET|PCy3',
        'common_solvents': 'DCM|benzene|PhCH3',
        'common_conditions': 'transition-metal carbene catalysis, often dilute conditions for macrocyclizing RCM',
        'synonym_names': 'olefin metathesis',
        'description_short': 'Alkene metathesis redistributes C=C bonds through metal-carbene and metallacyclobutane intermediates and encompasses RCM, CM, ROM, ROMP, and ADMET manifolds.',
        'overview_count': 1,
        'application_count': 1,
        'mechanism_count': 1,
        'seeded_from': 'frontmatter_batch5_named_reaction_pages',
    },
]

PAGE_LABEL_BY_NO = {row['page_no']: row['page_label'] for row in PAGE_KNOWLEDGE_SEEDS}

PAGE_ENTITY_SEEDS = [
    {'page_label': 'p11', 'entity_text': 'Schrock catalyst', 'canonical_name': 'Schrock catalyst', 'entity_type': 'catalyst', 'family_name': 'Alkene (Olefin) Metathesis', 'notes': 'Catalyst A used in cylindrocyclophane case study.', 'confidence': 0.93},
    {'page_label': 'p11', 'entity_text': 'Grubbs catalyst', 'canonical_name': 'Grubbs catalyst', 'entity_type': 'catalyst', 'family_name': 'Alkene (Olefin) Metathesis', 'notes': 'Catalyst B shown on application page.', 'confidence': 0.93},
    {'page_label': 'p11', 'entity_text': 'Grubbs modified perhydroimidazoline catalyst', 'canonical_name': 'Grubbs modified perhydroimidazoline catalyst', 'entity_type': 'catalyst', 'family_name': 'Alkene (Olefin) Metathesis', 'notes': 'Catalyst C shown on application page.', 'confidence': 0.9},
    {'page_label': 'p11', 'entity_text': '(-)-Cylindrocyclophane F', 'canonical_name': '(-)-Cylindrocyclophane F', 'entity_type': 'target_molecule', 'family_name': 'Alkene (Olefin) Metathesis', 'notes': 'Macrocycle assembled through ring-closing / cross-metathesis dimerization.', 'confidence': 0.91},
    {'page_label': 'p11', 'entity_text': '(-)-Griseoviridin', 'canonical_name': '(-)-Griseoviridin', 'entity_type': 'target_molecule', 'family_name': 'Alkene (Olefin) Metathesis', 'notes': 'Macrocyclic target prepared using RCM.', 'confidence': 0.91},
    {'page_label': 'p11', 'entity_text': '(+)-Pleuralureatin', 'canonical_name': '(+)-Pleuralureatin', 'entity_type': 'target_molecule', 'family_name': 'Alkene (Olefin) Metathesis', 'notes': 'Oxocene core assembled by RCM.', 'confidence': 0.9},
    {'page_label': 'p2', 'entity_text': 'retro-Claisen reaction', 'canonical_name': 'retro-Claisen reaction', 'entity_type': 'related_reaction', 'family_name': 'Acetoacetic Ester Synthesis', 'notes': 'Mentioned as competing cleavage pathway under basic conditions.', 'confidence': 0.84},
    {'page_label': 'p2', 'entity_text': 'Krapcho decarboxylation', 'canonical_name': 'Krapcho decarboxylation', 'entity_type': 'related_reaction', 'family_name': 'Acetoacetic Ester Synthesis', 'notes': 'Mentioned as complementary decarboxylative method.', 'confidence': 0.84},
    {'page_label': 'p3', 'entity_text': 'solanoeclepin A', 'canonical_name': 'solanoeclepin A', 'entity_type': 'target_molecule', 'family_name': 'Acetoacetic Ester Synthesis', 'notes': 'Application example target.', 'confidence': 0.91},
    {'page_label': 'p3', 'entity_text': 'Feist-Bénary reaction', 'canonical_name': 'Feist-Bénary reaction', 'entity_type': 'related_reaction', 'family_name': 'Acetoacetic Ester Synthesis', 'notes': 'Page explicitly names Feist-Bénary access to furan derivatives.', 'confidence': 0.88},
    {'page_label': 'p3', 'entity_text': 'tetrahydrofuran derivative', 'canonical_name': 'tetrahydrofuran derivative', 'entity_type': 'product_class', 'family_name': 'Acetoacetic Ester Synthesis', 'notes': 'Diastereoselective product class shown.', 'confidence': 0.85},
    {'page_label': 'p3', 'entity_text': 'tetrahydropyran derivative', 'canonical_name': 'tetrahydropyran derivative', 'entity_type': 'product_class', 'family_name': 'Acetoacetic Ester Synthesis', 'notes': 'Minor diastereomeric product class shown.', 'confidence': 0.85},
    {'page_label': 'p4', 'entity_text': 'bis-siloxyalkene', 'canonical_name': 'bis-siloxyalkene', 'entity_type': 'intermediate_class', 'family_name': 'Acyloin Condensation', 'notes': 'Key silyl-protected intermediate shown on overview page.', 'confidence': 0.87},
    {'page_label': 'p5', 'entity_text': 'anopterine', 'canonical_name': 'anopterine', 'entity_type': 'target_molecule', 'family_name': 'Acyloin Condensation', 'notes': 'Natural product application example.', 'confidence': 0.9},
    {'page_label': 'p5', 'entity_text': 'bicyclic diketone', 'canonical_name': 'bicyclic diketone', 'entity_type': 'product_class', 'family_name': 'Acyloin Condensation', 'notes': 'Lewis-acid-promoted geminal acylation product.', 'confidence': 0.86},
    {'page_label': 'p6', 'entity_text': 'Diels-Alder reaction', 'canonical_name': 'Diels-Alder reaction', 'entity_type': 'related_reaction', 'family_name': 'Alder (Ene) Reaction (Hydro-Allyl Addition)', 'notes': 'Mechanistic comparison made directly on overview page.', 'confidence': 0.82},
    {'page_label': 'p7', 'entity_text': '(-)-α-Kainic acid', 'canonical_name': '(-)-α-Kainic acid', 'entity_type': 'target_molecule', 'family_name': 'Alder (Ene) Reaction (Hydro-Allyl Addition)', 'notes': 'Metal-promoted ene cyclization example.', 'confidence': 0.9},
    {'page_label': 'p7', 'entity_text': '(+)-Arteannuin M', 'canonical_name': '(+)-Arteannuin M', 'entity_type': 'target_molecule', 'family_name': 'Alder (Ene) Reaction (Hydro-Allyl Addition)', 'notes': 'Tandem oxy-Cope/transannular ene application example.', 'confidence': 0.89},
    {'page_label': 'p7', 'entity_text': 'imidazo[1,2,3-jk][1,8]naphthyridine', 'canonical_name': 'imidazo[1,2,3-jk][1,8]naphthyridine', 'entity_type': 'target_molecule', 'family_name': 'Alder (Ene) Reaction (Hydro-Allyl Addition)', 'notes': 'Aza-ene adduct target class.', 'confidence': 0.88},
    {'page_label': 'p8', 'entity_text': 'Mukaiyama aldol reaction', 'canonical_name': 'Mukaiyama aldol reaction', 'entity_type': 'related_reaction', 'family_name': 'Aldol Reaction', 'notes': 'Named variant explicitly mentioned on overview page.', 'confidence': 0.88},
    {'page_label': 'p8', 'entity_text': 'Evans aldol', 'canonical_name': 'Evans aldol', 'entity_type': 'related_reaction', 'family_name': 'Aldol Reaction', 'notes': 'Named variant explicitly mentioned on overview page.', 'confidence': 0.88},
    {'page_label': 'p9', 'entity_text': '(-)-Denticulatin A', 'canonical_name': '(-)-Denticulatin A', 'entity_type': 'target_molecule', 'family_name': 'Aldol Reaction', 'notes': 'Enantioselective total synthesis example.', 'confidence': 0.91},
    {'page_label': 'p9', 'entity_text': 'Rhizoxin D', 'canonical_name': 'Rhizoxin D', 'entity_type': 'target_molecule', 'family_name': 'Aldol Reaction', 'notes': 'Asymmetric aldol coupling example.', 'confidence': 0.91},
    {'page_label': 'p9', 'entity_text': 'Fostriecin', 'canonical_name': 'Fostriecin', 'entity_type': 'target_molecule', 'family_name': 'Aldol Reaction', 'notes': 'Direct catalytic asymmetric aldol application example.', 'confidence': 0.9},
    {'page_label': 'p10', 'entity_text': 'metallacyclobutane', 'canonical_name': 'metallacyclobutane', 'entity_type': 'mechanistic_intermediate', 'family_name': 'Alkene (Olefin) Metathesis', 'notes': 'Central mechanistic intermediate shown on overview page.', 'confidence': 0.9},
]
for row in CURATED_BATCH5_ABBREVIATIONS:
    page_label = PAGE_LABEL_BY_NO.get(row['source_page'])
    if page_label:
        PAGE_ENTITY_SEEDS.append({
            'page_label': page_label,
            'entity_text': row['alias'],
            'canonical_name': row['canonical_name'],
            'entity_type': 'abbreviation',
            'family_name': None,
            'notes': row.get('notes'),
            'confidence': 0.93,
        })
for fam in FAMILY_PATTERN_SEEDS:
    for label in ['p2','p3'] if fam['family_name']=='Acetoacetic Ester Synthesis' else \
                 ['p4','p5'] if fam['family_name']=='Acyloin Condensation' else \
                 ['p6','p7'] if fam['family_name']=='Alder (Ene) Reaction (Hydro-Allyl Addition)' else \
                 ['p8','p9'] if fam['family_name']=='Aldol Reaction' else ['p10','p11']:
        PAGE_ENTITY_SEEDS.append({
            'page_label': label,
            'entity_text': fam['family_name'],
            'canonical_name': fam['family_name'],
            'entity_type': 'reaction_family',
            'family_name': fam['family_name'],
            'notes': 'Named reaction page anchor.',
            'confidence': 0.96,
        })


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
            (row['source_label'], row['page_label'], row['page_no'], row['title'], row['section_name'], row['page_kind'], row['summary'], row['family_names'], row['reference_family_name'], row['notes'], row['image_filename'])
        )
        inserted += 1
    return inserted


def _seed_abbreviations(con: sqlite3.Connection) -> int:
    if not _table_exists(con, 'abbreviation_aliases'):
        return 0
    changes = 0
    for row in CURATED_BATCH5_ABBREVIATIONS:
        alias_norm = normalize_key(row['alias'])
        canon_norm = normalize_key(row['canonical_name']) if row.get('canonical_name') else None
        con.execute(
            """
            INSERT INTO abbreviation_aliases (
              alias, alias_norm, canonical_name, canonical_name_norm,
              entity_type, smiles, molblock, notes, source_label, source_page, confidence, updated_at
            ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, 'front_matter_seed_batch5', ?, 0.94, datetime('now'))
            ON CONFLICT(alias_norm, canonical_name_norm, entity_type) DO UPDATE SET
              canonical_name=COALESCE(excluded.canonical_name, abbreviation_aliases.canonical_name),
              canonical_name_norm=COALESCE(excluded.canonical_name_norm, abbreviation_aliases.canonical_name_norm),
              entity_type=COALESCE(abbreviation_aliases.entity_type, excluded.entity_type),
              notes=COALESCE(abbreviation_aliases.notes, excluded.notes),
              source_label='front_matter_seed_batch5',
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, 'named_reactions_frontmatter_batch5', datetime('now'), ?, datetime('now'))
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
              latest_source_zip='named_reactions_frontmatter_batch5',
              latest_updated_at=datetime('now'),
              seeded_from='frontmatter_batch5_named_reaction_pages',
              updated_at=datetime('now')
            """,
            (row['family_name'], norm, row['family_class'], row['transformation_type'], row['mechanism_type'], row['reactant_pattern_text'], row['product_pattern_text'], row['key_reagents_clue'], row['common_solvents'], row['common_conditions'], row['synonym_names'], row['description_short'], row['overview_count'], row['application_count'], row['mechanism_count'], row['seeded_from'])
        )
        changes += 1
    return changes


def _seed_page_entities(con: sqlite3.Connection) -> int:
    if not _table_exists(con, 'manual_page_entities') or not _table_exists(con, 'manual_page_knowledge'):
        return 0
    page_lookup = {row[0]: row[1] for row in con.execute("SELECT page_label, id FROM manual_page_knowledge WHERE source_label='named_reactions_frontmatter_batch5'").fetchall()}
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


def get_frontmatter_batch5_counts(db_path: str | Path, con: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
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
            'frontmatter_batch5_pages': q("SELECT COUNT(*) FROM manual_page_knowledge WHERE source_label='named_reactions_frontmatter_batch5'") if _table_exists(con,'manual_page_knowledge') else 0,
            'frontmatter_batch5_aliases': q("SELECT COUNT(*) FROM abbreviation_aliases WHERE source_label='front_matter_seed_batch5'") if _table_exists(con,'abbreviation_aliases') else 0,
            'frontmatter_batch5_families': q("SELECT COUNT(*) FROM reaction_family_patterns WHERE latest_source_zip='named_reactions_frontmatter_batch5'") if _table_exists(con,'reaction_family_patterns') else 0,
        }
    finally:
        if close_after:
            con.close()


def apply_frontmatter_batch5(db_path: str | Path) -> Dict[str, Any]:
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
                INSERT INTO labint_schema_meta(key, value) VALUES('frontmatter_batch5_last_applied_at', datetime('now'))
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')
            """)
        con.commit()
        out = get_frontmatter_batch5_counts(db_path, con)
        out.update({
            'page_rows_seeded': page_rows,
            'alias_rows_seeded': alias_rows,
            'family_rows_seeded': family_rows,
            'entity_rows_seeded': entity_rows,
            'frontmatter_batch5_version': FRONTMATTER_BATCH5_VERSION,
        })
        return out
    finally:
        con.close()

if __name__ == '__main__':
    base = Path(__file__).resolve().parent
    for name in ['labint.db', 'labint_round9_bridge_work.db']:
        p = base / name
        if p.exists():
            print(name, apply_frontmatter_batch5(p))
