from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from app.labint_frontmatter import ensure_frontmatter_schema, normalize_key, _table_exists

FRONTMATTER_BATCH6_VERSION = 'labint_frontmatter_batch6_v1_20260411'

PAGE_KNOWLEDGE_SEEDS = [
    {
        'source_label': 'named_reactions_frontmatter_batch6',
        'page_label': 'p12',
        'page_no': 12,
        'title': 'Alkyne Metathesis – Importance and Mechanism',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'canonical_overview',
        'summary': 'Overview page for alkyne metathesis describing catalytic redistribution of carbon-carbon triple bonds, alkyne cross-metathesis, and ring-closing alkyne metathesis (RCAM), together with tungsten-carbene mechanistic proposals.',
        'family_names': 'Alkyne Metathesis',
        'reference_family_name': 'Alkyne Metathesis',
        'notes': 'Explicitly contrasts Mortreux-type and Schrock-type catalysts and notes conversion of cycloalkynes to cis/trans alkenes after reduction or hydroboration/protonation.',
        'image_filename': 'named reactions_64.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch6',
        'page_label': 'p13',
        'page_no': 13,
        'title': 'Alkyne Metathesis – Synthetic Applications',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'application_example',
        'summary': 'Application page for alkyne metathesis showing RCAM-based macrocyclizations in azamacrolides, prostaglandin E2-1,15-lactone synthesis, and cyclophane natural products such as turriane.',
        'family_names': 'Alkyne Metathesis',
        'reference_family_name': 'Alkyne Metathesis',
        'notes': 'Applications emphasize RCAM followed by Lindlar reduction to access Z-alkenes in macrocycles.',
        'image_filename': 'named reactions_65.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch6',
        'page_label': 'p14',
        'page_no': 14,
        'title': 'Amadori Reaction / Rearrangement – Importance, Mechanism, and Initial Application',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'canonical_overview',
        'summary': 'Overview page for the Amadori rearrangement describing conversion of N-glycosides to 1-amino-1-deoxyketoses under acid or Lewis-acid conditions, with mechanistic ring opening, enol formation, and tautomerization. Includes an initial synthetic application to D-fructose analogs.',
        'family_names': 'Amadori Reaction / Rearrangement',
        'reference_family_name': 'Amadori Reaction / Rearrangement',
        'notes': 'Page links the reaction to the Maillard reaction and non-enzymatic browning chemistry.',
        'image_filename': 'named reactions_66.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch6',
        'page_label': 'p15',
        'page_no': 15,
        'title': 'Amadori Reaction / Rearrangement – Synthetic Applications',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'application_example',
        'summary': 'Application page for the Amadori rearrangement showing synthesis of piperidin-3-one derivatives and intramolecular carbohydrate-derived rearrangements to bicyclic ketoses.',
        'family_names': 'Amadori Reaction / Rearrangement',
        'reference_family_name': 'Amadori Reaction / Rearrangement',
        'notes': 'Features DNA topoisomerase II inhibitor intermediates and peptide-linked sugar derivatives rearranged under p-TsOH or pyridine-acetic acid conditions.',
        'image_filename': 'named reactions_67.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch6',
        'page_label': 'p16',
        'page_no': 16,
        'title': 'Arbuzov Reaction (Michaelis–Arbuzov Reaction) – Importance and Mechanism',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'canonical_overview',
        'summary': 'Overview page for the Arbuzov reaction showing transformation of alkyl halides and trialkyl phosphites into dialkyl phosphonates or related organophosphorus derivatives through phosphonium-salt formation and rearrangement.',
        'family_names': 'Arbuzov Reaction (Michaelis-Arbuzov Reaction)',
        'reference_family_name': 'Arbuzov Reaction (Michaelis-Arbuzov Reaction)',
        'notes': 'Page explicitly connects the chemistry to Horner-Wadsworth-Emmons olefination and phosphonate synthesis.',
        'image_filename': 'named reactions_68.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch6',
        'page_label': 'p17',
        'page_no': 17,
        'title': 'Arbuzov Reaction (Michaelis–Arbuzov Reaction) – Synthetic Applications',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'application_example',
        'summary': 'Application page for Arbuzov chemistry highlighting synthesis of a phosphonic-acid analog of diclofenac, glycosyltransferase inhibitor scaffolds, and NMDA-antagonist LY235959.',
        'family_names': 'Arbuzov Reaction (Michaelis-Arbuzov Reaction)',
        'reference_family_name': 'Arbuzov Reaction (Michaelis-Arbuzov Reaction)',
        'notes': 'Examples include non-SN2 ortho-quinoid involvement and phosphonate installation from bromoheterocycle and lactone-derived precursors.',
        'image_filename': 'named reactions_69.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch6',
        'page_label': 'p18',
        'page_no': 18,
        'title': 'Arndt–Eistert Homologation / Synthesis – Importance and Mechanism',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'canonical_overview',
        'summary': 'Overview page for Arndt-Eistert homologation describing one-carbon extension of carboxylic acids via acid chloride formation, diazoketone generation, Wolff rearrangement, and nucleophilic trapping of the homologated ketene.',
        'family_names': 'Arndt-Eistert Homologation / Synthesis',
        'reference_family_name': 'Arndt-Eistert Homologation / Synthesis',
        'notes': 'Mechanism panel highlights diazoketone conformers, ketene formation, and homologated acid or derivative formation.',
        'image_filename': 'named reactions_70.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch6',
        'page_label': 'p19',
        'page_no': 19,
        'title': 'Arndt–Eistert Homologation / Synthesis – Synthetic Applications',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'application_example',
        'summary': 'Application page for Arndt-Eistert homologation showing access to β-amino acids, homologated CP-molecule intermediates, R-(-)-homocitric acid-γ-lactone, and the bis-indole alkaloid dragmacidin D.',
        'family_names': 'Arndt-Eistert Homologation / Synthesis',
        'reference_family_name': 'Arndt-Eistert Homologation / Synthesis',
        'notes': 'Examples emphasize diazoketone handling, mixed anhydrides, Ag2O-mediated Wolff rearrangement, and α-bromoketone formation in late-stage synthesis.',
        'image_filename': 'named reactions_71.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch6',
        'page_label': 'p20',
        'page_no': 20,
        'title': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement) – Importance and Mechanism',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'canonical_overview',
        'summary': 'Overview page for the aza-Claisen rearrangement, the thermal [3,3]-sigmatropic rearrangement of N-allyl enamines, with comparison to neutral, charged, and keteneimine manifolds and a chairlike transition state.',
        'family_names': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)',
        'reference_family_name': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)',
        'notes': 'Also includes a synthetic application to (-)-isoiridomyrmecin from carbamide enolate rearrangement.',
        'image_filename': 'named reactions_72.jpg',
    },
    {
        'source_label': 'named_reactions_frontmatter_batch6',
        'page_label': 'p21',
        'page_no': 21,
        'title': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement) – Synthetic Applications',
        'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
        'page_kind': 'application_example',
        'summary': 'Application page for aza-Claisen rearrangement including asymmetric and zwitterionic variants in the synthesis of fluvirucine A1, antimycin A3b, and (+)-dihydrocanadensolide.',
        'family_names': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)',
        'reference_family_name': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)',
        'notes': 'Examples highlight amide-enolate-induced rearrangement, LiHMDS/NaHMDS-promoted variants, and zwitterionic aza-Claisen processes.',
        'image_filename': 'named reactions_73.jpg',
    },
]

CURATED_BATCH6_ABBREVIATIONS = [
    {'alias': 'RCAM', 'canonical_name': 'ring-closing alkyne metathesis', 'entity_type': 'reaction_term', 'source_page': 12},
    {'alias': 'Mo(CO)6', 'canonical_name': 'molybdenum hexacarbonyl', 'entity_type': 'catalyst', 'source_page': 12},
    {'alias': '(t-BuO)3W≡C-t-Bu', 'canonical_name': 'Schrock tungsten carbyne complex', 'entity_type': 'catalyst', 'source_page': 12},
    {'alias': 'Lindlar cat.', 'canonical_name': 'Lindlar catalyst', 'entity_type': 'catalyst', 'source_page': 13},
    {'alias': 'p-TsOH', 'canonical_name': 'p-toluenesulfonic acid', 'entity_type': 'acid', 'source_page': 15},
    {'alias': 'MsOH', 'canonical_name': 'methanesulfonic acid', 'entity_type': 'acid', 'source_page': 17},
    {'alias': 'P(OMe)3', 'canonical_name': 'trimethyl phosphite', 'entity_type': 'reagent', 'source_page': 17},
    {'alias': 'P(OEt)3', 'canonical_name': 'triethyl phosphite', 'entity_type': 'reagent', 'source_page': 17},
    {'alias': 'SOCl2', 'canonical_name': 'thionyl chloride', 'entity_type': 'reagent', 'source_page': 18},
    {'alias': 'CH2N2', 'canonical_name': 'diazomethane', 'entity_type': 'reagent', 'source_page': 18},
    {'alias': 'Ag2O', 'canonical_name': 'silver oxide', 'entity_type': 'reagent', 'source_page': 18},
    {'alias': 'LA', 'canonical_name': 'Lewis acid', 'entity_type': 'reaction_term', 'source_page': 20},
    {'alias': 'NaHMDS', 'canonical_name': 'sodium bis(trimethylsilyl)amide', 'entity_type': 'base', 'source_page': 21},
    {'alias': 'LiHMDS', 'canonical_name': 'lithium bis(trimethylsilyl)amide', 'entity_type': 'base', 'source_page': 21},
    {'alias': 'TMSOTf', 'canonical_name': 'trimethylsilyl trifluoromethanesulfonate', 'entity_type': 'reagent', 'source_page': 21},
    {'alias': 'TIPSOTf', 'canonical_name': 'triisopropylsilyl trifluoromethanesulfonate', 'entity_type': 'reagent', 'source_page': 21},
]

FAMILY_PATTERN_SEEDS = [
    {
        'family_name': 'Alkyne Metathesis',
        'family_class': 'metathesis',
        'transformation_type': 'alkyne exchange and ring-closing alkyne metathesis',
        'mechanism_type': 'organometallic',
        'reactant_pattern_text': 'internal alkyne substrate(s) or diyne precursor under metal carbyne catalysis',
        'product_pattern_text': 'redistributed alkyne product or cycloalkyne from RCAM, often followed by semireduction to Z-alkene',
        'key_reagents_clue': 'RCAM|Mo(CO)6|(t-BuO)3W≡C-t-Bu|Lindlar catalyst|t-BuOH|ArOH',
        'common_solvents': 'chlorobenzene|toluene|MeCN|hexane',
        'common_conditions': 'metal carbyne catalysis, often high dilution or elevated temperature; RCAM commonly followed by Lindlar reduction',
        'synonym_names': 'alkyne cross metathesis|ring-closing alkyne metathesis',
        'description_short': 'Alkyne metathesis redistributes C≡C bonds through metal-carbyne intermediates and enables RCAM-based macrocyclization with downstream stereoselective alkyne reduction.',
        'overview_count': 1,
        'application_count': 1,
        'mechanism_count': 1,
        'seeded_from': 'frontmatter_batch6_named_reaction_pages',
    },
    {
        'family_name': 'Amadori Reaction / Rearrangement',
        'family_class': 'rearrangement',
        'transformation_type': 'N-glycoside to 1-amino-1-deoxyketose rearrangement',
        'mechanism_type': 'acid-catalyzed isomerization',
        'reactant_pattern_text': 'N-glycoside / glycosylamine derived from aldose and amine',
        'product_pattern_text': '1-amino-1-deoxyketose or rearranged amino ketose derivative',
        'key_reagents_clue': 'Lewis acid|protic acid|p-TsOH|AcOH|p-toluidine',
        'common_solvents': 'AcOH|toluene|pyridine-acetic acid',
        'common_conditions': 'acid- or Lewis-acid-catalyzed rearrangement with ring opening and tautomerization',
        'synonym_names': 'Amadori rearrangement',
        'description_short': 'The Amadori rearrangement converts N-glycosides into 1-amino-1-deoxyketoses and is a central transformation in carbohydrate chemistry and Maillard-type processes.',
        'overview_count': 1,
        'application_count': 2,
        'mechanism_count': 1,
        'seeded_from': 'frontmatter_batch6_named_reaction_pages',
    },
    {
        'family_name': 'Arbuzov Reaction (Michaelis-Arbuzov Reaction)',
        'family_class': 'phosphorus chemistry',
        'transformation_type': 'alkyl halide to phosphonate via phosphite rearrangement',
        'mechanism_type': 'SN2 / phosphonium rearrangement',
        'reactant_pattern_text': 'trialkyl phosphite or phosphinite + alkyl halide',
        'product_pattern_text': 'dialkyl phosphonate, phosphinate, or phosphine oxide derivative',
        'key_reagents_clue': 'P(OMe)3|P(OEt)3|alkyl halide|phosphonium salt|heat',
        'common_solvents': 'DCM|toluene',
        'common_conditions': 'phosphite alkylation followed by rearrangement, often thermal',
        'synonym_names': 'Michaelis-Arbuzov reaction',
        'description_short': 'The Arbuzov reaction transforms alkyl halides and phosphites into phosphonates or related organophosphorus products by way of phosphonium intermediates and alkyl migration.',
        'overview_count': 1,
        'application_count': 1,
        'mechanism_count': 1,
        'seeded_from': 'frontmatter_batch6_named_reaction_pages',
    },
    {
        'family_name': 'Arndt-Eistert Homologation / Synthesis',
        'family_class': 'homologation',
        'transformation_type': 'one-carbon chain extension of carboxylic acids',
        'mechanism_type': 'diazoketone / Wolff rearrangement',
        'reactant_pattern_text': 'carboxylic acid -> acid chloride -> α-diazoketone',
        'product_pattern_text': 'homologated acid, ester, amide, or β-amino acid via ketene capture',
        'key_reagents_clue': 'SOCl2|CH2N2|Ag2O|Wolff rearrangement|nucleophile',
        'common_solvents': 'ether|THF|H2O|ROH',
        'common_conditions': 'acid chloride formation, diazoketone generation, then thermal/photolytic/metal-assisted Wolff rearrangement',
        'synonym_names': 'Arndt-Eistert synthesis|Arndt-Eistert homologation',
        'description_short': 'Arndt-Eistert homologation extends carboxylic acids by one carbon through diazoketone formation and Wolff rearrangement to a ketene, which is then trapped by nucleophiles.',
        'overview_count': 1,
        'application_count': 1,
        'mechanism_count': 1,
        'seeded_from': 'frontmatter_batch6_named_reaction_pages',
    },
    {
        'family_name': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)',
        'family_class': 'sigmatropic rearrangement',
        'transformation_type': 'thermal [3,3]-sigmatropic rearrangement of N-allyl enamines',
        'mechanism_type': 'pericyclic',
        'reactant_pattern_text': 'N-allyl enamine / aza-1,5-hexadiene or amide-enolate precursor',
        'product_pattern_text': 'rearranged aza-1,5-hexadiene or aza-1,2,5-hexatriene-derived product',
        'key_reagents_clue': 'LiHMDS|NaHMDS|heat|Lewis acid|TMSOTf|TIPSOTf',
        'common_solvents': 'toluene|THF|DCM|CHCl3',
        'common_conditions': 'thermal or base-induced rearrangement through chairlike transition state; neutral, charged, and keteneimine manifolds',
        'synonym_names': '3-aza-Cope rearrangement|aza-Claisen rearrangement',
        'description_short': 'The aza-Claisen rearrangement is the [3,3]-sigmatropic rearrangement of N-allyl enamines and related aza-hexadiene systems, often used for stereocontrolled alkaloid synthesis.',
        'overview_count': 1,
        'application_count': 1,
        'mechanism_count': 1,
        'seeded_from': 'frontmatter_batch6_named_reaction_pages',
    },
]

PAGE_ENTITY_SEEDS = [
    {'page_label': 'p13', 'entity_text': 'Azamacrolide', 'canonical_name': 'Azamacrolide', 'entity_type': 'target_molecule', 'family_name': 'Alkyne Metathesis', 'notes': 'RCAM/Lindlar sequence example.', 'confidence': 0.9},
    {'page_label': 'p13', 'entity_text': 'PGE2-1,15-lactone', 'canonical_name': 'PGE2-1,15-lactone', 'entity_type': 'target_molecule', 'family_name': 'Alkyne Metathesis', 'notes': 'RCAM application in prostaglandin derivative synthesis.', 'confidence': 0.9},
    {'page_label': 'p13', 'entity_text': 'Turriane', 'canonical_name': 'Turriane', 'entity_type': 'target_molecule', 'family_name': 'Alkyne Metathesis', 'notes': 'Cyclophane natural-product example from RCAM.', 'confidence': 0.89},
    {'page_label': 'p14', 'entity_text': 'Maillard reaction', 'canonical_name': 'Maillard reaction', 'entity_type': 'related_reaction', 'family_name': 'Amadori Reaction / Rearrangement', 'notes': 'Explicitly referenced as broader carbohydrate rearrangement context.', 'confidence': 0.85},
    {'page_label': 'p14', 'entity_text': 'D-Fructose analogs', 'canonical_name': 'D-Fructose analogs', 'entity_type': 'target_family', 'family_name': 'Amadori Reaction / Rearrangement', 'notes': 'Initial application shown on p14.', 'confidence': 0.84},
    {'page_label': 'p15', 'entity_text': 'Piperidin-3-one derivatives', 'canonical_name': 'Piperidin-3-one derivatives', 'entity_type': 'target_family', 'family_name': 'Amadori Reaction / Rearrangement', 'notes': 'Topoisomerase II inhibitor program intermediates.', 'confidence': 0.86},
    {'page_label': 'p15', 'entity_text': 'Bicyclic ketoses', 'canonical_name': 'Bicyclic ketoses', 'entity_type': 'target_family', 'family_name': 'Amadori Reaction / Rearrangement', 'notes': 'Intramolecular Amadori rearrangement products from sugar derivatives.', 'confidence': 0.87},
    {'page_label': 'p16', 'entity_text': 'Horner-Wadsworth-Emmons olefination', 'canonical_name': 'Horner-Wadsworth-Emmons Olefination', 'entity_type': 'related_reaction', 'family_name': 'Arbuzov Reaction (Michaelis-Arbuzov Reaction)', 'notes': 'Overview page explicitly connects Arbuzov phosphonates to HWE chemistry.', 'confidence': 0.88},
    {'page_label': 'p17', 'entity_text': 'Diclofenac phosphonic acid analog', 'canonical_name': 'Diclofenac phosphonic acid analog', 'entity_type': 'target_molecule', 'family_name': 'Arbuzov Reaction (Michaelis-Arbuzov Reaction)', 'notes': 'Non-SN2 ortho-quinoid pathway example.', 'confidence': 0.89},
    {'page_label': 'p17', 'entity_text': 'LY235959', 'canonical_name': 'LY235959', 'entity_type': 'target_molecule', 'family_name': 'Arbuzov Reaction (Michaelis-Arbuzov Reaction)', 'notes': 'NMDA receptor antagonist assembled using high-yielding Arbuzov step.', 'confidence': 0.9},
    {'page_label': 'p18', 'entity_text': 'Wolff rearrangement', 'canonical_name': 'Wolff rearrangement', 'entity_type': 'related_reaction', 'family_name': 'Arndt-Eistert Homologation / Synthesis', 'notes': 'Central mechanistic step in overview page.', 'confidence': 0.9},
    {'page_label': 'p19', 'entity_text': 'β-Amino acid', 'canonical_name': 'β-Amino acid', 'entity_type': 'product_class', 'family_name': 'Arndt-Eistert Homologation / Synthesis', 'notes': 'Homologation applied to α-amino acid derivatives.', 'confidence': 0.88},
    {'page_label': 'p19', 'entity_text': 'CP-225,917', 'canonical_name': 'CP-225,917', 'entity_type': 'target_molecule', 'family_name': 'Arndt-Eistert Homologation / Synthesis', 'notes': 'Late-stage homologation example.', 'confidence': 0.88},
    {'page_label': 'p19', 'entity_text': '(R)-(-)-Homocitric acid-γ-lactone', 'canonical_name': '(R)-(-)-Homocitric acid-γ-lactone', 'entity_type': 'target_molecule', 'family_name': 'Arndt-Eistert Homologation / Synthesis', 'notes': 'Multigram synthesis using Arndt-Eistert homologation.', 'confidence': 0.89},
    {'page_label': 'p19', 'entity_text': '(+)-Dragmacidin D', 'canonical_name': '(+)-Dragmacidin D', 'entity_type': 'target_molecule', 'family_name': 'Arndt-Eistert Homologation / Synthesis', 'notes': 'Bis-indole alkaloid prepared using homologation in endgame.', 'confidence': 0.9},
    {'page_label': 'p20', 'entity_text': 'Claisen rearrangement', 'canonical_name': 'Claisen Rearrangement', 'entity_type': 'related_reaction', 'family_name': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)', 'notes': 'Overview compares aza-Claisen directly to Claisen.', 'confidence': 0.9},
    {'page_label': 'p20', 'entity_text': '(-)-Isoiridomyrmecin', 'canonical_name': '(-)-Isoiridomyrmecin', 'entity_type': 'target_molecule', 'family_name': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)', 'notes': 'Synthetic application shown at bottom of p20.', 'confidence': 0.88},
    {'page_label': 'p21', 'entity_text': 'Fluvirucine A1', 'canonical_name': 'Fluvirucine A1', 'entity_type': 'target_molecule', 'family_name': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)', 'notes': 'Amide-enolate-induced aza-Claisen example.', 'confidence': 0.9},
    {'page_label': 'p21', 'entity_text': 'Antimycin A3b', 'canonical_name': 'Antimycin A3b', 'entity_type': 'target_molecule', 'family_name': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)', 'notes': 'Asymmetric aza-Claisen rearrangement route.', 'confidence': 0.9},
    {'page_label': 'p21', 'entity_text': '(+)-Dihydrocanadensolide', 'canonical_name': '(+)-Dihydrocanadensolide', 'entity_type': 'target_molecule', 'family_name': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)', 'notes': 'Diastereoselective zwitterionic aza-Claisen example.', 'confidence': 0.89},
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
    for row in CURATED_BATCH6_ABBREVIATIONS:
        alias_norm = normalize_key(row['alias'])
        canon_norm = normalize_key(row['canonical_name']) if row.get('canonical_name') else None
        con.execute(
            """
            INSERT INTO abbreviation_aliases (
              alias, alias_norm, canonical_name, canonical_name_norm,
              entity_type, smiles, molblock, notes, source_label, source_page, confidence, updated_at
            ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, 'front_matter_seed_batch6', ?, 0.94, datetime('now'))
            ON CONFLICT(alias_norm, canonical_name_norm, entity_type) DO UPDATE SET
              canonical_name=COALESCE(excluded.canonical_name, abbreviation_aliases.canonical_name),
              canonical_name_norm=COALESCE(excluded.canonical_name_norm, abbreviation_aliases.canonical_name_norm),
              notes=COALESCE(abbreviation_aliases.notes, excluded.notes),
              source_label='front_matter_seed_batch6',
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, 'named_reactions_frontmatter_batch6', datetime('now'), ?, datetime('now'))
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
              latest_source_zip='named_reactions_frontmatter_batch6',
              latest_updated_at=datetime('now'),
              seeded_from='frontmatter_batch6_named_reaction_pages',
              updated_at=datetime('now')
            """,
            (row['family_name'], norm, row['family_class'], row['transformation_type'], row['mechanism_type'], row['reactant_pattern_text'], row['product_pattern_text'], row['key_reagents_clue'], row['common_solvents'], row['common_conditions'], row['synonym_names'], row['description_short'], row['overview_count'], row['application_count'], row['mechanism_count'], row['seeded_from'])
        )
        changes += 1
    return changes


def _seed_page_entities(con: sqlite3.Connection) -> int:
    if not _table_exists(con, 'manual_page_entities') or not _table_exists(con, 'manual_page_knowledge'):
        return 0
    page_lookup = {row[0]: row[1] for row in con.execute("SELECT page_label, id FROM manual_page_knowledge WHERE source_label='named_reactions_frontmatter_batch6'").fetchall()}
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


def get_frontmatter_batch6_counts(db_path: str | Path, con: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
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
            'frontmatter_batch6_pages': q("SELECT COUNT(*) FROM manual_page_knowledge WHERE source_label='named_reactions_frontmatter_batch6'") if _table_exists(con,'manual_page_knowledge') else 0,
            'frontmatter_batch6_aliases': q("SELECT COUNT(*) FROM abbreviation_aliases WHERE source_label='front_matter_seed_batch6'") if _table_exists(con,'abbreviation_aliases') else 0,
            'frontmatter_batch6_families': q("SELECT COUNT(*) FROM reaction_family_patterns WHERE latest_source_zip='named_reactions_frontmatter_batch6'") if _table_exists(con,'reaction_family_patterns') else 0,
        }
    finally:
        if close_after:
            con.close()


def apply_frontmatter_batch6(db_path: str | Path) -> Dict[str, Any]:
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
                INSERT INTO labint_schema_meta(key, value) VALUES('frontmatter_batch6_last_applied_at', datetime('now'))
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')
            """)
        con.commit()
        out = get_frontmatter_batch6_counts(db_path, con)
        out.update({
            'page_rows_seeded': page_rows,
            'alias_rows_seeded': alias_rows,
            'family_rows_seeded': family_rows,
            'entity_rows_seeded': entity_rows,
            'frontmatter_batch6_version': FRONTMATTER_BATCH6_VERSION,
        })
        return out
    finally:
        con.close()

if __name__ == '__main__':
    base = Path(__file__).resolve().parent
    for name in ['labint.db', 'labint_round9_bridge_work.db']:
        p = base / name
        if p.exists():
            print(name, apply_frontmatter_batch6(p))
