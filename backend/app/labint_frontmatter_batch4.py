from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from app.labint_frontmatter import ensure_frontmatter_schema, normalize_key, _table_exists

FRONTMATTER_BATCH4_VERSION = 'labint_frontmatter_batch4_v1_20260411'

PAGE_KNOWLEDGE_SEEDS = [{'family_names': None,
  'image_filename': 'named reactions_44.jpg',
  'notes': 'Alias-normalization seed page.',
  'page_kind': 'abbreviation_glossary',
  'page_label': 'xliv',
  'page_no': 44,
  'reference_family_name': None,
  'section_name': 'V. List of Abbreviations',
  'source_label': 'named_reactions_frontmatter_batch4',
  'summary': 'Abbreviation glossary page covering TPS to Z(Cbz), including trityl protecting groups, TTN, UHP, '
             'Vitride/Red-Al, and reaction-time shorthand.',
  'title': 'V. List of Abbreviations (TPS to Z/Cbz)'},
 {'family_names': None,
  'image_filename': 'named reactions_53.jpg',
  'notes': 'Section divider / navigation page.',
  'page_kind': 'section_cover',
  'page_label': 'vii_cover',
  'page_no': 53,
  'reference_family_name': None,
  'section_name': 'VII. Named Organic Reactions in Alphabetical Order',
  'source_label': 'named_reactions_frontmatter_batch4',
  'summary': 'Section cover introducing the alphabetical named reaction compendium that follows.',
  'title': 'VII. Named Organic Reactions in Alphabetical Order'},
 {'family_names': 'Acetoacetic Ester Synthesis|Acyloin Condensation|Alder (Ene) Reaction (Hydro-Allyl Addition)|Aldol '
                  'Reaction|Alkene (Olefin) Metathesis|Alkyne Metathesis|Amadori Reaction/Rearrangement|Arbuzov '
                  'Reaction (Michaelis-Arbuzov Reaction)|Arndt-Eistert Homologation/Synthesis|Aza-Claisen '
                  'Rearrangement (3-Aza-Cope Rearrangement)|Aza-Cope Rearrangement|Aza-Wittig Reaction|Aza[2,3]-Wittig '
                  "Rearrangement|Baeyer-Villiger Oxidation/Rearrangement|Baker-Venkataraman Rearrangement|Baldwin's "
                  'Rules/Guidelines for Ring-Closing Reactions|Balz-Schiemann Reaction (Schiemann '
                  'Reaction)|Bamford-Stevens-Shapiro Olefination|Barbier Coupling Reaction|Bartoli Indole '
                  'Synthesis|Barton Nitrite Ester Reaction|Barton Radical Decarboxylation Reaction|Barton-McCombie '
                  'Radical Deoxygenation Reaction|Baylis-Hillman Reaction|Beckmann Rearrangement|Benzilic Acid '
                  'Rearrangement|Benzoin and Retro-Benzoin Condensation|Bergman Cycloaromatization Reaction|Biginelli '
                  'Reaction|Birch Reduction|Bischler-Napieralski Isoquinoline Synthesis',
  'image_filename': 'named reactions_45.jpg',
  'notes': 'High-value family-seed index page.',
  'page_kind': 'named_reaction_index',
  'page_label': 'xlv',
  'page_no': 45,
  'reference_family_name': None,
  'section_name': 'VI. List of Named Organic Reactions',
  'source_label': 'named_reactions_frontmatter_batch4',
  'summary': 'Alphabetical named reaction index page listing 31 reactions from Acetoacetic Ester Synthesis to '
             'Bischler-Napieralski Isoquinoline Synthesis.',
  'title': 'VI. List of Named Organic Reactions (Acetoacetic Ester Synthesis to Bischler-Napieralski Isoquinoline '
           'Synthesis)'},
 {'family_names': 'Brook Rearrangement|Brown Hydroboration Reaction|Buchner Method of Ring Expansion (Buchner '
                  'Reaction)|Buchwald-Hartwig Cross-Coupling|Burgess Dehydration Reaction|Cannizzaro Reaction|Carroll '
                  'Rearrangement (Kimel-Cope Rearrangement)|Castro-Stephens Coupling|Chichibabin Amination Reaction '
                  '(Chichibabin Reaction)|Chugaev Elimination Reaction (Xanthate Ester Pyrolysis)|Ciamician-Dennstedt '
                  'Rearrangement|Claisen Condensation/Claisen Reaction|Claisen Rearrangement|Claisen-Ireland '
                  'Rearrangement|Clemmensen Reduction|Combes Quinoline Synthesis|Cope Elimination / Cope Reaction|Cope '
                  'Rearrangement|Corey-Bakshi-Shibata Reduction (CBS Reduction)|Corey-Chaykovsky Epoxidation and '
                  'Cyclopropanation|Corey-Fuchs Alkyne Synthesis|Corey-Kim Oxidation|Corey-Nicolaou '
                  'Macrolactonization|Corey-Winter Olefination|Cornforth Rearrangement|Criegee Oxidation|Curtius '
                  'Rearrangement|Dakin Oxidation|Dakin-West Reaction|Danheiser Benzannulation|Danheiser Cyclopentene '
                  "Annulation|Danishefsky's Diene Cycloaddition|Darzens Glycidic Ester Condensation|Davis' Oxaziridine "
                  'Oxidations',
  'image_filename': 'named reactions_46.jpg',
  'notes': 'High-value family-seed index page.',
  'page_kind': 'named_reaction_index',
  'page_label': 'xlvi',
  'page_no': 46,
  'reference_family_name': None,
  'section_name': 'VI. List of Named Organic Reactions',
  'source_label': 'named_reactions_frontmatter_batch4',
  'summary': "Alphabetical named reaction index page listing 34 reactions from Brook Rearrangement to Davis' "
             'Oxaziridine Oxidations.',
  'title': "VI. List of Named Organic Reactions (Brook Rearrangement to Davis' Oxaziridine Oxidations)"},
 {'family_names': 'De Mayo Cycloaddition (Enone-Alkene [2+2] Photocycloaddition)|Demjanov Rearrangement and '
                  'Tiffeneau-Demjanov Rearrangement|Dess-Martin Oxidation|Dieckmann Condensation|Diels-Alder '
                  'Cycloaddition|Dienone-Phenol Rearrangement|Dimroth Rearrangement|Doering-LaFlamme Allene '
                  'Synthesis|Dötz Benzannulation Reaction|Enders SAMP/RAMP Hydrazone Alkylation|Enyne '
                  'Metathesis|Eschenmoser Methenylation|Eschenmoser-Claisen Rearrangement|Eschenmoser-Tanabe '
                  'Fragmentation|Eschweiler-Clarke Methylation (Reductive Alkylation)|Evans Aldol Reaction|Favorskii '
                  'and Homo-Favorskii Rearrangement|Feist-Bénary Furan Synthesis|Ferrier '
                  'Reaction/Rearrangement|Finkelstein Reaction|Fischer Indole Synthesis|Fleming-Tamao '
                  'Oxidation|Friedel-Crafts Acylation|Friedel-Crafts Alkylation|Fries-, Photo-Fries, and Anionic '
                  'Ortho-Fries Rearrangement|Gabriel Synthesis|Gattermann and Gattermann-Koch Formylation|Glaser '
                  'Coupling|Grignard Reaction|Grob Fragmentation|Hajos-Parrish Reaction|Hantzsch Dihydropyridine '
                  'Synthesis|Heck Reaction|Heine Reaction',
  'image_filename': 'named reactions_47.jpg',
  'notes': 'High-value family-seed index page.',
  'page_kind': 'named_reaction_index',
  'page_label': 'xlvii',
  'page_no': 47,
  'reference_family_name': None,
  'section_name': 'VI. List of Named Organic Reactions',
  'source_label': 'named_reactions_frontmatter_batch4',
  'summary': 'Alphabetical named reaction index page listing 34 reactions from De Mayo Cycloaddition (Enone-Alkene '
             '[2+2] Photocycloaddition) to Heine Reaction.',
  'title': 'VI. List of Named Organic Reactions (De Mayo Cycloaddition to Heine Reaction)'},
 {'family_names': 'Hell-Volhard-Zelinsky Reaction|Henry Reaction|Hetero Diels-Alder Cycloaddition (HDA)|Hofmann '
                  'Elimination|Hofmann-Löffler-Freytag Reaction (Remote Functionalization)|Hofmann '
                  'Rearrangement|Horner-Wadsworth-Emmons Olefination|Horner-Wadsworth-Emmons Olefination – '
                  'Still-Gennari Modification|Houben-Hoesch Reaction/Synthesis|Hunsdiecker Reaction|Jacobsen '
                  'Hydrolytic Kinetic Resolution|Jacobsen-Katsuki Epoxidation|Japp-Klingemann Reaction|Johnson-Claisen '
                  'Rearrangement|Jones Oxidation/Oxidation of Alcohols by Chromium Reagents|Julia-Lythgoe '
                  'Olefination|Kagan-Molander Samarium Diiodide-Mediated Coupling|Kahne Glycosidation|Keck Asymmetric '
                  'Allylation|Keck Macrolactonization|Keck Radical Allylation|Knoevenagel Condensation|Knorr Pyrrole '
                  'Synthesis|Koenigs-Knorr Glycosidation|Kolbe-Schmitt Reaction|Kornblum Oxidation|Krapcho '
                  'Dealkoxycarbonylation (Krapcho Reaction)|Kröhnke Pyridine Synthesis|Kulinkovich Reaction|Kumada '
                  'Cross-Coupling|Larock Indole Synthesis|Ley Oxidation|Lieben Haloform Reaction|Lossen Rearrangement',
  'image_filename': 'named reactions_48.jpg',
  'notes': 'High-value family-seed index page.',
  'page_kind': 'named_reaction_index',
  'page_label': 'xlviii',
  'page_no': 48,
  'reference_family_name': None,
  'section_name': 'VI. List of Named Organic Reactions',
  'source_label': 'named_reactions_frontmatter_batch4',
  'summary': 'Alphabetical named reaction index page listing 34 reactions from Hell-Volhard-Zelinsky Reaction to '
             'Lossen Rearrangement.',
  'title': 'VI. List of Named Organic Reactions (Hell-Volhard-Zelinsky Reaction to Lossen Rearrangement)'},
 {'family_names': 'Luche Reduction|Madelung Indole Synthesis|Malonic Ester Synthesis|Mannich Reaction|McMurry '
                  'Coupling|Meerwein Arylation|Meerwein-Ponndorf-Verley Reduction|Meisenheimer '
                  'Rearrangement|Meyer-Schuster and Rupe Rearrangement|Michael Addition Reaction|Midland Alpine Borane '
                  'Reduction|Minisci Reaction|Mislow-Evans Rearrangement|Mitsunobu Reaction|Miyaura Boration|Mukaiyama '
                  'Aldol Reaction|Myers Asymmetric Alkylation|Nagata Hydrocyanation|Nazarov Cyclization|Neber '
                  'Rearrangement|Nef Reaction|Negishi Cross-Coupling|Nenitzescu Indole Synthesis|Nicholas '
                  'Reaction|Noyori Asymmetric Hydrogenation|Nozaki-Hiyama-Kishi Reaction|Oppenauer Oxidation|Overman '
                  'Rearrangement|Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement|Paal-Knorr Furan '
                  'Synthesis|Paal-Knorr Pyrrole Synthesis|Passerini Multicomponent Reaction|Paternò-Büchi '
                  'Reaction|Pauson-Khand Reaction',
  'image_filename': 'named reactions_49.jpg',
  'notes': 'High-value family-seed index page.',
  'page_kind': 'named_reaction_index',
  'page_label': 'xlix',
  'page_no': 49,
  'reference_family_name': None,
  'section_name': 'VI. List of Named Organic Reactions',
  'source_label': 'named_reactions_frontmatter_batch4',
  'summary': 'Alphabetical named reaction index page listing 34 reactions from Luche Reduction to Pauson-Khand '
             'Reaction.',
  'title': 'VI. List of Named Organic Reactions (Luche Reduction to Pauson-Khand Reaction)'},
 {'family_names': 'Payne Rearrangement|Perkin Reaction|Petasis Boronic Acid-Mannich Reaction|Petasis-Ferrier '
                  'Rearrangement|Peterson Olefination|Pfitzner-Moffatt Oxidation|Pictet-Spengler '
                  'Tetrahydroisoquinoline Synthesis|Pinacol and Semipinacol Rearrangement|Pinner Reaction|Pinnick '
                  'Oxidation|Polonovski Reaction|Pomeranz-Fritsch Reaction|Prévost Reaction|Prilezhaev Reaction|Prins '
                  'Reaction|Prins-Pinacol Rearrangement|Pummerer Rearrangement|Quasi-Favorskii '
                  'Rearrangement|Ramberg-Bäcklund Rearrangement|Reformatsky Reaction|Regitz Diazo '
                  'Transfer|Reimer-Tiemann Reaction|Riley Selenium Dioxide Oxidation|Ritter Reaction|Robinson '
                  'Annulation|Roush Asymmetric Allylation|Rubottom Oxidation|Saegusa Oxidation|Sakurai '
                  'Allylation|Sandmeyer Reaction|Schmidt Reaction|Schotten-Baumann Reaction|Schwartz '
                  'Hydrozirconation|Seyferth-Gilbert Homologation',
  'image_filename': 'named reactions_50.jpg',
  'notes': 'High-value family-seed index page.',
  'page_kind': 'named_reaction_index',
  'page_label': 'l',
  'page_no': 50,
  'reference_family_name': None,
  'section_name': 'VI. List of Named Organic Reactions',
  'source_label': 'named_reactions_frontmatter_batch4',
  'summary': 'Alphabetical named reaction index page listing 34 reactions from Payne Rearrangement to Seyferth-Gilbert '
             'Homologation.',
  'title': 'VI. List of Named Organic Reactions (Payne Rearrangement to Seyferth-Gilbert Homologation)'},
 {'family_names': 'Sharpless Asymmetric Aminohydroxylation|Sharpless Asymmetric Dihydroxylation|Sharpless Asymmetric '
                  'Epoxidation|Shi Asymmetric Epoxidation|Simmons-Smith Cyclopropanation|Skraup and Doebner-Miller '
                  'Quinoline Synthesis|Smiles Rearrangement|Smith-Tietze Multicomponent Dithiane Linchpin '
                  'Coupling|Snieckus Directed Ortho Metalation|Sommelet-Hauser Rearrangement|Sonogashira '
                  'Cross-Coupling|Staudinger Ketene Cycloaddition|Staudinger Reaction|Stephen Aldehyde Synthesis '
                  '(Stephen Reduction)|Stetter Reaction|Stevens Rearrangement|Stille Carbonylative '
                  'Cross-Coupling|Stille Cross-Coupling (Migita-Kosugi-Stille Coupling)|Stille-Kelly Coupling|Stobbe '
                  'Condensation|Stork Enamine Synthesis|Strecker Reaction|Suzuki Cross-Coupling (Suzuki-Miyaura '
                  'Cross-Coupling)|Swern Oxidation|Takai-Utimoto Olefination (Takai Reaction)|Tebbe '
                  'Olefination/Petasis-Tebbe Olefination|Tishchenko Reaction|Tsuji-Trost '
                  'Reaction/Allylation|Tsuji-Wilkinson Decarbonylation Reaction|Ugi Multicomponent Reaction|Ullmann '
                  'Biaryl Ether and Biaryl Amine Synthesis/Condensation|Ullmann Reaction/Coupling/Biaryl '
                  'Synthesis|Vilsmeier-Haack Formylation|Vinylcyclopropane-Cyclopentene Rearrangement',
  'image_filename': 'named reactions_51.jpg',
  'notes': 'High-value family-seed index page.',
  'page_kind': 'named_reaction_index',
  'page_label': 'li',
  'page_no': 51,
  'reference_family_name': None,
  'section_name': 'VI. List of Named Organic Reactions',
  'source_label': 'named_reactions_frontmatter_batch4',
  'summary': 'Alphabetical named reaction index page listing 34 reactions from Sharpless Asymmetric Aminohydroxylation '
             'to Vinylcyclopropane-Cyclopentene Rearrangement.',
  'title': 'VI. List of Named Organic Reactions (Sharpless Asymmetric Aminohydroxylation to '
           'Vinylcyclopropane-Cyclopentene Rearrangement)'},
 {'family_names': 'von Pechmann Reaction|Wacker Oxidation|Wagner-Meerwein Rearrangement|Weinreb Ketone '
                  'Synthesis|Wharton Fragmentation|Wharton Olefin Synthesis (Wharton Transposition)|Williamson Ether '
                  'Synthesis|Wittig Reaction|Wittig Reaction - Schlosser Modification|Wittig-[1,2]- and '
                  '[2,3]-Rearrangement|Wohl-Ziegler Bromination|Wolff Rearrangement|Wolff-Kishner Reduction|Wurtz '
                  'Coupling|Yamaguchi Macrolactonization',
  'image_filename': 'named reactions_52.jpg',
  'notes': 'High-value family-seed index page.',
  'page_kind': 'named_reaction_index',
  'page_label': 'liii',
  'page_no': 52,
  'reference_family_name': None,
  'section_name': 'VI. List of Named Organic Reactions',
  'source_label': 'named_reactions_frontmatter_batch4',
  'summary': 'Alphabetical named reaction index page listing 15 reactions from von Pechmann Reaction to Yamaguchi '
             'Macrolactonization.',
  'title': 'VI. List of Named Organic Reactions (von Pechmann Reaction to Yamaguchi Macrolactonization)'}]

CURATED_FRONTMATTER_BATCH4_ABBREVIATIONS = [{'alias': 'TPS',
  'canonical_name': 'triphenylsilyl',
  'entity_type': 'protecting_group',
  'notes': None,
  'source_page': 44},
 {'alias': 'Tr',
  'canonical_name': 'trityl (triphenylmethyl)',
  'entity_type': 'protecting_group',
  'notes': None,
  'source_page': 44},
 {'alias': 'Trisyl',
  'canonical_name': '2,4,6-triisopropylbenzenesulfonyl',
  'entity_type': 'protecting_group',
  'notes': None,
  'source_page': 44},
 {'alias': 'Troc',
  'canonical_name': '2,2,2-trichloroethoxycarbonyl',
  'entity_type': 'protecting_group',
  'notes': None,
  'source_page': 44},
 {'alias': 'TS',
  'canonical_name': 'transition state (or transition structure)',
  'entity_type': 'general_term',
  'notes': None,
  'source_page': 44},
 {'alias': 'Ts',
  'canonical_name': 'p-toluenesulfonyl',
  'entity_type': 'protecting_group',
  'notes': 'Shown as Ts (Tos).',
  'source_page': 44},
 {'alias': 'Tos',
  'canonical_name': 'p-toluenesulfonyl',
  'entity_type': 'protecting_group',
  'notes': 'Synonym shown with Ts.',
  'source_page': 44},
 {'alias': 'TSE',
  'canonical_name': '2-(trimethylsilyl)ethyl',
  'entity_type': 'protecting_group',
  'notes': 'Shown as TSE (TMSE).',
  'source_page': 44},
 {'alias': 'TMSE',
  'canonical_name': '2-(trimethylsilyl)ethyl',
  'entity_type': 'protecting_group',
  'notes': 'Synonym shown with TSE.',
  'source_page': 44},
 {'alias': 'TTBP',
  'canonical_name': '2,4,5-tri-tert-butylpyrimidine',
  'entity_type': 'base',
  'notes': None,
  'source_page': 44},
 {'alias': 'TTMSS',
  'canonical_name': 'tris(trimethylsilyl)silane',
  'entity_type': 'reagent',
  'notes': None,
  'source_page': 44},
 {'alias': 'TTN',
  'canonical_name': 'thallium(III)-trinitrate',
  'entity_type': 'oxidant',
  'notes': None,
  'source_page': 44},
 {'alias': 'UHP',
  'canonical_name': 'urea-hydrogen peroxide complex',
  'entity_type': 'oxidant',
  'notes': None,
  'source_page': 44},
 {'alias': 'Vitride',
  'canonical_name': 'sodium bis(2-methoxyethoxy)aluminum hydride',
  'entity_type': 'reducing_agent',
  'notes': 'Shown as Vitride (Red-Al).',
  'source_page': 44},
 {'alias': 'Red-Al',
  'canonical_name': 'sodium bis(2-methoxyethoxy)aluminum hydride',
  'entity_type': 'reducing_agent',
  'notes': 'Synonym shown with Vitride.',
  'source_page': 44},
 {'alias': 'wk',
  'canonical_name': 'weeks (length of reaction time)',
  'entity_type': 'condition_term',
  'notes': None,
  'source_page': 44},
 {'alias': 'Z',
  'canonical_name': 'benzyloxycarbonyl',
  'entity_type': 'protecting_group',
  'notes': 'Shown as Z (Cbz).',
  'source_page': 44},
 {'alias': 'Cbz',
  'canonical_name': 'benzyloxycarbonyl',
  'entity_type': 'protecting_group',
  'notes': 'Synonym shown with Z.',
  'source_page': 44}]

TOC_PAGE_FAMILY_SEEDS = {'l': ['Payne Rearrangement',
       'Perkin Reaction',
       'Petasis Boronic Acid-Mannich Reaction',
       'Petasis-Ferrier Rearrangement',
       'Peterson Olefination',
       'Pfitzner-Moffatt Oxidation',
       'Pictet-Spengler Tetrahydroisoquinoline Synthesis',
       'Pinacol and Semipinacol Rearrangement',
       'Pinner Reaction',
       'Pinnick Oxidation',
       'Polonovski Reaction',
       'Pomeranz-Fritsch Reaction',
       'Prévost Reaction',
       'Prilezhaev Reaction',
       'Prins Reaction',
       'Prins-Pinacol Rearrangement',
       'Pummerer Rearrangement',
       'Quasi-Favorskii Rearrangement',
       'Ramberg-Bäcklund Rearrangement',
       'Reformatsky Reaction',
       'Regitz Diazo Transfer',
       'Reimer-Tiemann Reaction',
       'Riley Selenium Dioxide Oxidation',
       'Ritter Reaction',
       'Robinson Annulation',
       'Roush Asymmetric Allylation',
       'Rubottom Oxidation',
       'Saegusa Oxidation',
       'Sakurai Allylation',
       'Sandmeyer Reaction',
       'Schmidt Reaction',
       'Schotten-Baumann Reaction',
       'Schwartz Hydrozirconation',
       'Seyferth-Gilbert Homologation'],
 'li': ['Sharpless Asymmetric Aminohydroxylation',
        'Sharpless Asymmetric Dihydroxylation',
        'Sharpless Asymmetric Epoxidation',
        'Shi Asymmetric Epoxidation',
        'Simmons-Smith Cyclopropanation',
        'Skraup and Doebner-Miller Quinoline Synthesis',
        'Smiles Rearrangement',
        'Smith-Tietze Multicomponent Dithiane Linchpin Coupling',
        'Snieckus Directed Ortho Metalation',
        'Sommelet-Hauser Rearrangement',
        'Sonogashira Cross-Coupling',
        'Staudinger Ketene Cycloaddition',
        'Staudinger Reaction',
        'Stephen Aldehyde Synthesis (Stephen Reduction)',
        'Stetter Reaction',
        'Stevens Rearrangement',
        'Stille Carbonylative Cross-Coupling',
        'Stille Cross-Coupling (Migita-Kosugi-Stille Coupling)',
        'Stille-Kelly Coupling',
        'Stobbe Condensation',
        'Stork Enamine Synthesis',
        'Strecker Reaction',
        'Suzuki Cross-Coupling (Suzuki-Miyaura Cross-Coupling)',
        'Swern Oxidation',
        'Takai-Utimoto Olefination (Takai Reaction)',
        'Tebbe Olefination/Petasis-Tebbe Olefination',
        'Tishchenko Reaction',
        'Tsuji-Trost Reaction/Allylation',
        'Tsuji-Wilkinson Decarbonylation Reaction',
        'Ugi Multicomponent Reaction',
        'Ullmann Biaryl Ether and Biaryl Amine Synthesis/Condensation',
        'Ullmann Reaction/Coupling/Biaryl Synthesis',
        'Vilsmeier-Haack Formylation',
        'Vinylcyclopropane-Cyclopentene Rearrangement'],
 'liii': ['von Pechmann Reaction',
          'Wacker Oxidation',
          'Wagner-Meerwein Rearrangement',
          'Weinreb Ketone Synthesis',
          'Wharton Fragmentation',
          'Wharton Olefin Synthesis (Wharton Transposition)',
          'Williamson Ether Synthesis',
          'Wittig Reaction',
          'Wittig Reaction - Schlosser Modification',
          'Wittig-[1,2]- and [2,3]-Rearrangement',
          'Wohl-Ziegler Bromination',
          'Wolff Rearrangement',
          'Wolff-Kishner Reduction',
          'Wurtz Coupling',
          'Yamaguchi Macrolactonization'],
 'xlix': ['Luche Reduction',
          'Madelung Indole Synthesis',
          'Malonic Ester Synthesis',
          'Mannich Reaction',
          'McMurry Coupling',
          'Meerwein Arylation',
          'Meerwein-Ponndorf-Verley Reduction',
          'Meisenheimer Rearrangement',
          'Meyer-Schuster and Rupe Rearrangement',
          'Michael Addition Reaction',
          'Midland Alpine Borane Reduction',
          'Minisci Reaction',
          'Mislow-Evans Rearrangement',
          'Mitsunobu Reaction',
          'Miyaura Boration',
          'Mukaiyama Aldol Reaction',
          'Myers Asymmetric Alkylation',
          'Nagata Hydrocyanation',
          'Nazarov Cyclization',
          'Neber Rearrangement',
          'Nef Reaction',
          'Negishi Cross-Coupling',
          'Nenitzescu Indole Synthesis',
          'Nicholas Reaction',
          'Noyori Asymmetric Hydrogenation',
          'Nozaki-Hiyama-Kishi Reaction',
          'Oppenauer Oxidation',
          'Overman Rearrangement',
          'Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement',
          'Paal-Knorr Furan Synthesis',
          'Paal-Knorr Pyrrole Synthesis',
          'Passerini Multicomponent Reaction',
          'Paternò-Büchi Reaction',
          'Pauson-Khand Reaction'],
 'xlv': ['Acetoacetic Ester Synthesis',
         'Acyloin Condensation',
         'Alder (Ene) Reaction (Hydro-Allyl Addition)',
         'Aldol Reaction',
         'Alkene (Olefin) Metathesis',
         'Alkyne Metathesis',
         'Amadori Reaction/Rearrangement',
         'Arbuzov Reaction (Michaelis-Arbuzov Reaction)',
         'Arndt-Eistert Homologation/Synthesis',
         'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)',
         'Aza-Cope Rearrangement',
         'Aza-Wittig Reaction',
         'Aza[2,3]-Wittig Rearrangement',
         'Baeyer-Villiger Oxidation/Rearrangement',
         'Baker-Venkataraman Rearrangement',
         "Baldwin's Rules/Guidelines for Ring-Closing Reactions",
         'Balz-Schiemann Reaction (Schiemann Reaction)',
         'Bamford-Stevens-Shapiro Olefination',
         'Barbier Coupling Reaction',
         'Bartoli Indole Synthesis',
         'Barton Nitrite Ester Reaction',
         'Barton Radical Decarboxylation Reaction',
         'Barton-McCombie Radical Deoxygenation Reaction',
         'Baylis-Hillman Reaction',
         'Beckmann Rearrangement',
         'Benzilic Acid Rearrangement',
         'Benzoin and Retro-Benzoin Condensation',
         'Bergman Cycloaromatization Reaction',
         'Biginelli Reaction',
         'Birch Reduction',
         'Bischler-Napieralski Isoquinoline Synthesis'],
 'xlvi': ['Brook Rearrangement',
          'Brown Hydroboration Reaction',
          'Buchner Method of Ring Expansion (Buchner Reaction)',
          'Buchwald-Hartwig Cross-Coupling',
          'Burgess Dehydration Reaction',
          'Cannizzaro Reaction',
          'Carroll Rearrangement (Kimel-Cope Rearrangement)',
          'Castro-Stephens Coupling',
          'Chichibabin Amination Reaction (Chichibabin Reaction)',
          'Chugaev Elimination Reaction (Xanthate Ester Pyrolysis)',
          'Ciamician-Dennstedt Rearrangement',
          'Claisen Condensation/Claisen Reaction',
          'Claisen Rearrangement',
          'Claisen-Ireland Rearrangement',
          'Clemmensen Reduction',
          'Combes Quinoline Synthesis',
          'Cope Elimination / Cope Reaction',
          'Cope Rearrangement',
          'Corey-Bakshi-Shibata Reduction (CBS Reduction)',
          'Corey-Chaykovsky Epoxidation and Cyclopropanation',
          'Corey-Fuchs Alkyne Synthesis',
          'Corey-Kim Oxidation',
          'Corey-Nicolaou Macrolactonization',
          'Corey-Winter Olefination',
          'Cornforth Rearrangement',
          'Criegee Oxidation',
          'Curtius Rearrangement',
          'Dakin Oxidation',
          'Dakin-West Reaction',
          'Danheiser Benzannulation',
          'Danheiser Cyclopentene Annulation',
          "Danishefsky's Diene Cycloaddition",
          'Darzens Glycidic Ester Condensation',
          "Davis' Oxaziridine Oxidations"],
 'xlvii': ['De Mayo Cycloaddition (Enone-Alkene [2+2] Photocycloaddition)',
           'Demjanov Rearrangement and Tiffeneau-Demjanov Rearrangement',
           'Dess-Martin Oxidation',
           'Dieckmann Condensation',
           'Diels-Alder Cycloaddition',
           'Dienone-Phenol Rearrangement',
           'Dimroth Rearrangement',
           'Doering-LaFlamme Allene Synthesis',
           'Dötz Benzannulation Reaction',
           'Enders SAMP/RAMP Hydrazone Alkylation',
           'Enyne Metathesis',
           'Eschenmoser Methenylation',
           'Eschenmoser-Claisen Rearrangement',
           'Eschenmoser-Tanabe Fragmentation',
           'Eschweiler-Clarke Methylation (Reductive Alkylation)',
           'Evans Aldol Reaction',
           'Favorskii and Homo-Favorskii Rearrangement',
           'Feist-Bénary Furan Synthesis',
           'Ferrier Reaction/Rearrangement',
           'Finkelstein Reaction',
           'Fischer Indole Synthesis',
           'Fleming-Tamao Oxidation',
           'Friedel-Crafts Acylation',
           'Friedel-Crafts Alkylation',
           'Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement',
           'Gabriel Synthesis',
           'Gattermann and Gattermann-Koch Formylation',
           'Glaser Coupling',
           'Grignard Reaction',
           'Grob Fragmentation',
           'Hajos-Parrish Reaction',
           'Hantzsch Dihydropyridine Synthesis',
           'Heck Reaction',
           'Heine Reaction'],
 'xlviii': ['Hell-Volhard-Zelinsky Reaction',
            'Henry Reaction',
            'Hetero Diels-Alder Cycloaddition (HDA)',
            'Hofmann Elimination',
            'Hofmann-Löffler-Freytag Reaction (Remote Functionalization)',
            'Hofmann Rearrangement',
            'Horner-Wadsworth-Emmons Olefination',
            'Horner-Wadsworth-Emmons Olefination – Still-Gennari Modification',
            'Houben-Hoesch Reaction/Synthesis',
            'Hunsdiecker Reaction',
            'Jacobsen Hydrolytic Kinetic Resolution',
            'Jacobsen-Katsuki Epoxidation',
            'Japp-Klingemann Reaction',
            'Johnson-Claisen Rearrangement',
            'Jones Oxidation/Oxidation of Alcohols by Chromium Reagents',
            'Julia-Lythgoe Olefination',
            'Kagan-Molander Samarium Diiodide-Mediated Coupling',
            'Kahne Glycosidation',
            'Keck Asymmetric Allylation',
            'Keck Macrolactonization',
            'Keck Radical Allylation',
            'Knoevenagel Condensation',
            'Knorr Pyrrole Synthesis',
            'Koenigs-Knorr Glycosidation',
            'Kolbe-Schmitt Reaction',
            'Kornblum Oxidation',
            'Krapcho Dealkoxycarbonylation (Krapcho Reaction)',
            'Kröhnke Pyridine Synthesis',
            'Kulinkovich Reaction',
            'Kumada Cross-Coupling',
            'Larock Indole Synthesis',
            'Ley Oxidation',
            'Lieben Haloform Reaction',
            'Lossen Rearrangement']}

PAGE_LABEL_BY_NO = {row['page_no']: row['page_label'] for row in PAGE_KNOWLEDGE_SEEDS}

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
                row['source_label'], row['page_label'], row['page_no'], row['title'], row['section_name'], row['page_kind'],
                row['summary'], row.get('family_names'), row.get('reference_family_name'), row.get('notes'), row.get('image_filename'),
            ),
        )
        inserted += 1
    return inserted


def _seed_abbreviations(con: sqlite3.Connection) -> int:
    if not _table_exists(con, 'abbreviation_aliases'):
        return 0
    inserted = 0
    for seed in CURATED_FRONTMATTER_BATCH4_ABBREVIATIONS:
        alias_norm = normalize_key(seed['alias'])
        canon_norm = normalize_key(seed['canonical_name'])
        con.execute(
            """
            INSERT OR IGNORE INTO abbreviation_aliases (
              alias, alias_norm, canonical_name, canonical_name_norm, entity_type,
              smiles, molblock, notes, source_label, source_page, confidence, updated_at
            ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, 'front_matter_seed_batch4', ?, 0.94, datetime('now'))
            """,
            (seed['alias'], alias_norm, seed['canonical_name'], canon_norm, seed['entity_type'], seed.get('notes'), seed['source_page']),
        )
        inserted += 1
    return inserted


def _family_meta(name: str) -> dict[str, str | None]:
    lname = name.lower()
    family_class = 'named reaction'
    transformation = 'other'
    mechanism = None

    if 'cross-coupling' in lname or ' coupling' in lname or lname.endswith('coupling'):
        family_class = 'coupling'
        transformation = 'cross-coupling' if 'cross-coupling' in lname else 'coupling'
    elif 'oxidation' in lname:
        family_class = 'oxidation'
        transformation = 'oxidation'
    elif 'reduction' in lname:
        family_class = 'reduction'
        transformation = 'reduction'
    elif 'rearrangement' in lname:
        family_class = 'rearrangement'
        transformation = 'rearrangement'
    elif 'condensation' in lname:
        family_class = 'condensation'
        transformation = 'condensation'
    elif 'cycloaddition' in lname:
        family_class = 'cycloaddition'
        transformation = 'cycloaddition'
    elif 'metathesis' in lname:
        family_class = 'metathesis'
        transformation = 'metathesis'
    elif 'olefination' in lname:
        family_class = 'olefination'
        transformation = 'olefination'
    elif 'annulation' in lname:
        family_class = 'annulation'
        transformation = 'annulation'
    elif 'synthesis' in lname:
        family_class = 'synthesis'
        transformation = 'synthesis'
    elif 'cyclization' in lname:
        family_class = 'cyclization'
        transformation = 'cyclization'
    elif 'elimination' in lname:
        family_class = 'elimination'
        transformation = 'elimination'
    elif 'fragmentation' in lname:
        family_class = 'fragmentation'
        transformation = 'fragmentation'
    elif 'hydrogenation' in lname:
        family_class = 'reduction'
        transformation = 'hydrogenation'
    elif 'hydroboration' in lname:
        family_class = 'addition'
        transformation = 'hydroboration'
    elif 'formylation' in lname:
        family_class = 'functionalization'
        transformation = 'formylation'
    elif 'bromination' in lname or 'haloform' in lname or 'diazo transfer' in lname:
        family_class = 'functionalization'
        transformation = 'functionalization'
    elif 'alkylation' in lname or 'allylation' in lname:
        family_class = 'functionalization'
        transformation = 'alkylation'
    elif 'amination' in lname:
        family_class = 'functionalization'
        transformation = 'amination'
    elif 'glycosidation' in lname:
        family_class = 'glycosidation'
        transformation = 'glycosidation'
    elif 'epoxidation' in lname:
        family_class = 'oxidation'
        transformation = 'epoxidation'

    if 'radical' in lname:
        mechanism = 'radical'
    elif 'metathesis' in lname:
        mechanism = 'organometallic'
    elif 'cross-coupling' in lname or ' coupling' in lname or lname.endswith('coupling'):
        mechanism = 'organometallic'
    elif 'cycloaddition' in lname:
        mechanism = 'cycloaddition'
    elif 'oxidation' in lname or 'reduction' in lname:
        mechanism = 'redox'
    elif 'multicomponent' in lname:
        mechanism = 'multicomponent'
    elif 'claisen' in lname or 'cope' in lname or 'ene' in lname or 'wittig' in lname or 'fries' in lname or 'wagner-meerwein' in lname:
        mechanism = 'pericyclic'

    return {
        'family_class': family_class,
        'transformation_type': transformation,
        'mechanism_type': mechanism,
    }


def _synonym_names(name: str) -> str | None:
    out: list[str] = []
    if '(' in name and ')' in name:
        first = name.split('(', 1)[0].strip().rstrip('-/–— ')
        inside = name.split('(', 1)[1].rsplit(')', 1)[0].strip()
        if inside:
            out.append(inside)
        if first and first != name:
            out.append(first)
    if ' / ' in name:
        parts = [p.strip() for p in name.split(' / ') if p.strip()]
        if len(parts) > 1:
            out.extend(parts)
    if '/' in name and ' / ' not in name and '[' not in name:
        parts = [p.strip() for p in name.split('/') if p.strip()]
        if len(parts) > 1:
            out.extend(parts)
    cleaned: list[str] = []
    seen = set()
    for item in out:
        if item == name:
            continue
        norm = normalize_key(item)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        cleaned.append(item)
    return '|'.join(cleaned) if cleaned else None


def _build_manual_family_seeds() -> list[dict[str, Any]]:
    seeds: list[dict[str, Any]] = []
    for page_label, families in TOC_PAGE_FAMILY_SEEDS.items():
        for fam in families:
            meta = _family_meta(fam)
            seeds.append({
                'page_label': page_label,
                'family_name': fam,
                'family_class': meta['family_class'],
                'transformation_type': meta['transformation_type'],
                'mechanism_type': meta['mechanism_type'],
                'reactant_pattern_text': None,
                'product_pattern_text': None,
                'key_reagents_clue': None,
                'common_solvents': None,
                'common_conditions': None,
                'synonym_names': _synonym_names(fam),
                'description_short': 'Named reaction family listed in the alphabetical reaction index pages of the source book.',
                'seeded_from': 'frontmatter_batch4_index',
            })
    return seeds


MANUAL_FAMILY_BATCH4_SEEDS = _build_manual_family_seeds()


def _seed_manual_family_patterns(con: sqlite3.Connection) -> int:
    if not _table_exists(con, 'reaction_family_patterns'):
        return 0
    changes = 0
    for seed in MANUAL_FAMILY_BATCH4_SEEDS:
        family_name_norm = normalize_key(seed['family_name'])
        con.execute(
            """
            INSERT INTO reaction_family_patterns (
              family_name, family_name_norm, family_class, transformation_type, mechanism_type,
              reactant_pattern_text, product_pattern_text, key_reagents_clue,
              common_solvents, common_conditions, synonym_names, description_short,
              evidence_extract_count, overview_count, application_count, mechanism_count,
              latest_source_zip, latest_updated_at, seeded_from, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 'named_reactions_frontmatter_batch4', datetime('now'), ?, datetime('now'))
            ON CONFLICT(family_name_norm) DO UPDATE SET
              family_name=excluded.family_name,
              family_class=COALESCE(reaction_family_patterns.family_class, excluded.family_class),
              transformation_type=COALESCE(reaction_family_patterns.transformation_type, excluded.transformation_type),
              mechanism_type=COALESCE(reaction_family_patterns.mechanism_type, excluded.mechanism_type),
              reactant_pattern_text=COALESCE(reaction_family_patterns.reactant_pattern_text, excluded.reactant_pattern_text),
              product_pattern_text=COALESCE(reaction_family_patterns.product_pattern_text, excluded.product_pattern_text),
              key_reagents_clue=COALESCE(reaction_family_patterns.key_reagents_clue, excluded.key_reagents_clue),
              common_solvents=COALESCE(reaction_family_patterns.common_solvents, excluded.common_solvents),
              common_conditions=COALESCE(reaction_family_patterns.common_conditions, excluded.common_conditions),
              synonym_names=CASE
                    WHEN excluded.synonym_names IS NULL OR trim(excluded.synonym_names) = '' THEN reaction_family_patterns.synonym_names
                    WHEN reaction_family_patterns.synonym_names IS NULL OR trim(reaction_family_patterns.synonym_names) = '' THEN excluded.synonym_names
                    WHEN instr(lower(reaction_family_patterns.synonym_names), lower(excluded.synonym_names)) > 0 THEN reaction_family_patterns.synonym_names
                    ELSE reaction_family_patterns.synonym_names || '|' || excluded.synonym_names
                END,
              description_short=COALESCE(reaction_family_patterns.description_short, excluded.description_short),
              latest_source_zip='named_reactions_frontmatter_batch4',
              latest_updated_at=datetime('now'),
              updated_at=datetime('now')
            """,
            (
                seed['family_name'], family_name_norm, seed['family_class'], seed['transformation_type'], seed['mechanism_type'],
                seed['reactant_pattern_text'], seed['product_pattern_text'], seed['key_reagents_clue'], seed['common_solvents'], seed['common_conditions'],
                seed['synonym_names'], seed['description_short'], seed['seeded_from'],
            ),
        )
        changes += 1
    return changes


def _build_page_entity_seeds() -> list[dict[str, Any]]:
    seeds: list[dict[str, Any]] = []
    for row in CURATED_FRONTMATTER_BATCH4_ABBREVIATIONS:
        page_label = PAGE_LABEL_BY_NO.get(row['source_page'])
        if not page_label:
            continue
        seeds.append({
            'page_label': page_label,
            'entity_text': row['alias'],
            'canonical_name': row['canonical_name'],
            'entity_type': 'abbreviation',
            'family_name': None,
            'notes': row.get('notes'),
            'confidence': 0.93,
        })
    for page_label, families in TOC_PAGE_FAMILY_SEEDS.items():
        for fam in families:
            seeds.append({
                'page_label': page_label,
                'entity_text': fam,
                'canonical_name': fam,
                'entity_type': 'reaction_family',
                'family_name': fam,
                'notes': 'Named reaction index entry.',
                'confidence': 0.95,
            })
    return seeds


MANUAL_PAGE_ENTITY_BATCH4_SEEDS = _build_page_entity_seeds()


def _seed_page_entities(con: sqlite3.Connection) -> int:
    if not _table_exists(con, 'manual_page_entities') or not _table_exists(con, 'manual_page_knowledge'):
        return 0
    page_lookup = {row[0]: row[1] for row in con.execute("SELECT page_label, id FROM manual_page_knowledge WHERE source_label = 'named_reactions_frontmatter_batch4'").fetchall()}
    alias_lookup = {}
    if _table_exists(con, 'abbreviation_aliases'):
        alias_lookup = {row[0]: row[1] for row in con.execute('SELECT alias_norm, id FROM abbreviation_aliases').fetchall()}
    inserted = 0
    for seed in MANUAL_PAGE_ENTITY_BATCH4_SEEDS:
        page_id = page_lookup.get(seed['page_label'])
        if not page_id:
            continue
        entity_text_norm = normalize_key(seed['entity_text'])
        alias_id = alias_lookup.get(entity_text_norm) if seed['entity_type'] == 'abbreviation' else None
        con.execute(
            """
            INSERT OR REPLACE INTO manual_page_entities (
              page_knowledge_id, entity_text, entity_text_norm, canonical_name,
              entity_type, alias_id, family_name, notes, confidence, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                page_id, seed['entity_text'], entity_text_norm, seed.get('canonical_name'), seed['entity_type'], alias_id,
                seed.get('family_name'), seed.get('notes'), seed.get('confidence', 0.9),
            ),
        )
        inserted += 1
    return inserted


def get_frontmatter_batch4_counts(db_path: str | Path, con: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
    close_after = False
    if con is None:
        con = sqlite3.connect(str(db_path))
        close_after = True
    try:
        def q(sql: str) -> int:
            return int(con.execute(sql).fetchone()[0])
        meta = {}
        if _table_exists(con, 'labint_schema_meta'):
            meta = {row[0]: row[1] for row in con.execute("SELECT key, value FROM labint_schema_meta WHERE key LIKE 'frontmatter_%'")}
        return {
            'frontmatter_schema_version': meta.get('frontmatter_schema_version'),
            'manual_page_knowledge': q('SELECT COUNT(*) FROM manual_page_knowledge') if _table_exists(con, 'manual_page_knowledge') else 0,
            'manual_page_entities': q('SELECT COUNT(*) FROM manual_page_entities') if _table_exists(con, 'manual_page_entities') else 0,
            'frontmatter_batch4_pages': q("SELECT COUNT(*) FROM manual_page_knowledge WHERE source_label = 'named_reactions_frontmatter_batch4'") if _table_exists(con, 'manual_page_knowledge') else 0,
            'frontmatter_batch4_abbreviation_aliases': q("SELECT COUNT(*) FROM abbreviation_aliases WHERE source_label = 'front_matter_seed_batch4'") if _table_exists(con, 'abbreviation_aliases') else 0,
            'frontmatter_batch4_manual_families': q("SELECT COUNT(*) FROM reaction_family_patterns WHERE latest_source_zip = 'named_reactions_frontmatter_batch4'") if _table_exists(con, 'reaction_family_patterns') else 0,
        }
    finally:
        if close_after:
            con.close()


def apply_frontmatter_batch4(db_path: str | Path) -> Dict[str, Any]:
    db_path = Path(db_path)
    ensure_frontmatter_schema(db_path)
    con = sqlite3.connect(str(db_path))
    try:
        con.execute('PRAGMA foreign_keys=ON')
        page_rows = _insert_page_knowledge(con)
        alias_rows = _seed_abbreviations(con)
        family_rows = _seed_manual_family_patterns(con)
        entity_rows = _seed_page_entities(con)
        if _table_exists(con, 'labint_schema_meta'):
            con.execute("""
                INSERT INTO labint_schema_meta(key, value) VALUES('frontmatter_batch4_last_applied_at', datetime('now'))
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')
            """)
        con.commit()
        out = get_frontmatter_batch4_counts(db_path, con)
        out.update({'page_seed_rows': page_rows, 'alias_seed_rows': alias_rows, 'manual_family_seed_ops': family_rows, 'page_entity_seed_rows': entity_rows})
        return out
    finally:
        con.close()


def export_frontmatter_batch4_seed_templates(output_dir: str | Path) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    with (out / 'frontmatter_batch4_manual_pages.csv').open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['source_label', 'page_label', 'page_no', 'title', 'section_name', 'page_kind', 'summary', 'family_names', 'reference_family_name', 'notes', 'image_filename'])
        writer.writeheader()
        for row in PAGE_KNOWLEDGE_SEEDS:
            writer.writerow(row)
    with (out / 'frontmatter_batch4_abbreviation_seed.csv').open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['alias', 'canonical_name', 'entity_type', 'source_page', 'notes'])
        writer.writeheader()
        for row in CURATED_FRONTMATTER_BATCH4_ABBREVIATIONS:
            writer.writerow(row)
    with (out / 'frontmatter_batch4_family_seed.csv').open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['page_label', 'family_name', 'family_class', 'transformation_type', 'mechanism_type', 'synonym_names', 'description_short', 'seeded_from'])
        writer.writeheader()
        for row in MANUAL_FAMILY_BATCH4_SEEDS:
            writer.writerow({
                'page_label': row['page_label'],
                'family_name': row['family_name'],
                'family_class': row['family_class'],
                'transformation_type': row['transformation_type'],
                'mechanism_type': row['mechanism_type'],
                'synonym_names': row['synonym_names'],
                'description_short': row['description_short'],
                'seeded_from': row['seeded_from'],
            })
