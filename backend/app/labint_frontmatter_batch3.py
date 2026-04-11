from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from app.labint_frontmatter import ensure_frontmatter_schema, normalize_key, _table_exists

FRONTMATTER_BATCH3_VERSION = 'labint_frontmatter_batch3_v1_20260411'

PAGE_KNOWLEDGE_SEEDS = [{'family_names': None,
  'image_filename': 'named reactions_34.jpg',
  'notes': 'Alias-normalization seed page.',
  'page_kind': 'abbreviation_glossary',
  'page_label': 'xxxiv',
  'page_no': 34,
  'reference_family_name': None,
  'section_name': 'V. List of Abbreviations',
  'source_label': 'named_reactions_frontmatter_batch3',
  'summary': 'Abbreviation glossary page covering MPPC to NADPH, including mesyl derivatives, MTAD, MVK, and the biomolecule NADPH.',
  'title': 'V. List of Abbreviations (MPPC to NADPH)'},
 {'family_names': None,
  'image_filename': 'named reactions_35.jpg',
  'notes': 'Alias-normalization seed page.',
  'page_kind': 'abbreviation_glossary',
  'page_label': 'xxxv',
  'page_no': 35,
  'reference_family_name': None,
  'section_name': 'V. List of Abbreviations',
  'source_label': 'named_reactions_frontmatter_batch3',
  'summary': 'Abbreviation glossary page covering NaHMDS to Nos, including NBS, NCS, NIS, NMO, NORPHOS, and sulfonyl abbreviations.',
  'title': 'V. List of Abbreviations (NaHMDS to Nos)'},
 {'family_names': None,
  'image_filename': 'named reactions_36.jpg',
  'notes': 'Alias-normalization seed page.',
  'page_kind': 'abbreviation_glossary',
  'page_label': 'xxxvi',
  'page_no': 36,
  'reference_family_name': None,
  'section_name': 'V. List of Abbreviations',
  'source_label': 'named_reactions_frontmatter_batch3',
  'summary': 'Abbreviation glossary page covering NPM to phen, including Oxone, PCC, PDC, phenyl/phthalazine abbreviations, and several '
             'general reaction notations.',
  'title': 'V. List of Abbreviations (NPM to phen)'},
 {'family_names': None,
  'image_filename': 'named reactions_37.jpg',
  'notes': 'Alias-normalization seed page.',
  'page_kind': 'abbreviation_glossary',
  'page_label': 'xxxvii',
  'page_no': 37,
  'reference_family_name': None,
  'section_name': 'V. List of Abbreviations',
  'source_label': 'named_reactions_frontmatter_batch3',
  'summary': 'Abbreviation glossary page covering Phth to PPSE, including PIFA, PMB/MPM, PNZ, PPA, PPO, and phosphoric-acid derivatives.',
  'title': 'V. List of Abbreviations (Phth to PPSE)'},
 {'family_names': 'Ring-Closing Alkyne Metathesis|Ring-Closing Metathesis|Ring-Opening Metathesis|Ring-Opening Metathesis Polymerization',
  'image_filename': 'named reactions_38.jpg',
  'notes': 'Alias-normalization seed page with explicit named-reaction abbreviations.',
  'page_kind': 'abbreviation_glossary',
  'page_label': 'xxxviii',
  'page_no': 38,
  'reference_family_name': None,
  'section_name': 'V. List of Abbreviations',
  'source_label': 'named_reactions_frontmatter_batch3',
  'summary': 'Abbreviation glossary page covering PPTS to ROMP, including PTSA, Raney nickel, Rose Bengal, and explicit metathesis family '
             'abbreviations RCAM, RCM, ROM, and ROMP.',
  'title': 'V. List of Abbreviations (PPTS to ROMP)'},
 {'family_names': None,
  'image_filename': 'named reactions_39.jpg',
  'notes': 'Alias-normalization seed page.',
  'page_kind': 'abbreviation_glossary',
  'page_label': 'xxxix',
  'page_no': 39,
  'reference_family_name': None,
  'section_name': 'V. List of Abbreviations',
  'source_label': 'named_reactions_frontmatter_batch3',
  'summary': 'Abbreviation glossary page covering Rose Bengal to SPB, including Salen, salophen, SAMP, SEM/SES, SET, and perborate '
             'abbreviations.',
  'title': 'V. List of Abbreviations (Rose Bengal to SPB)'},
 {'family_names': None,
  'image_filename': 'named reactions_40.jpg',
  'notes': 'Alias-normalization seed page.',
  'page_kind': 'abbreviation_glossary',
  'page_label': 'xl',
  'page_no': 40,
  'reference_family_name': None,
  'section_name': 'V. List of Abbreviations',
  'source_label': 'named_reactions_frontmatter_batch3',
  'summary': 'Abbreviation glossary page covering TADDOL to TBTSP, including common tetrabutylammonium salts, silyl protecting groups, '
             'TBHP, and tributyltin hydride.',
  'title': 'V. List of Abbreviations (TADDOL to TBTSP)'},
 {'family_names': None,
  'image_filename': 'named reactions_41.jpg',
  'notes': 'Alias-normalization seed page.',
  'page_kind': 'abbreviation_glossary',
  'page_label': 'xli',
  'page_no': 41,
  'reference_family_name': None,
  'section_name': 'V. List of Abbreviations',
  'source_label': 'named_reactions_frontmatter_batch3',
  'summary': 'Abbreviation glossary page covering TCCA to Tfa, including TEMPO, TCCA, TCNE, TCNQ, TEA, TFA, and triethylsilyl-related '
             'abbreviations.',
  'title': 'V. List of Abbreviations (TCCA to Tfa)'},
 {'family_names': None,
  'image_filename': 'named reactions_42.jpg',
  'notes': 'Alias-normalization seed page.',
  'page_kind': 'abbreviation_glossary',
  'page_label': 'xlii',
  'page_no': 42,
  'reference_family_name': None,
  'section_name': 'V. List of Abbreviations',
  'source_label': 'named_reactions_frontmatter_batch3',
  'summary': 'Abbreviation glossary page covering TFAA to TMGA, including THF, THP, TIPS, TMEDA, TMAO, and triflic-acid related '
             'abbreviations.',
  'title': 'V. List of Abbreviations (TFAA to TMGA)'},
 {'family_names': None,
  'image_filename': 'named reactions_43.jpg',
  'notes': 'Alias-normalization seed page.',
  'page_kind': 'abbreviation_glossary',
  'page_label': 'xliii',
  'page_no': 43,
  'reference_family_name': None,
  'section_name': 'V. List of Abbreviations',
  'source_label': 'named_reactions_frontmatter_batch3',
  'summary': 'Abbreviation glossary page covering Tmob to TPP, including TMSA, TMSEE, TPAP, triphenylphosphine, and tolbinap.',
  'title': 'V. List of Abbreviations (Tmob to TPP)'}]

CURATED_FRONTMATTER_BATCH3_ABBREVIATIONS = [{'alias': 'MPPC', 'canonical_name': 'N-methyl piperidinium chlorochromate', 'entity_type': 'oxidant', 'notes': None, 'source_page': 34},
 {'alias': 'Ms', 'canonical_name': 'mesyl (methanesulfonyl)', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 34},
 {'alias': 'MS',
  'canonical_name': 'mass spectrometry / molecular sieves',
  'entity_type': 'analysis_term',
  'notes': 'Ambiguous glossary abbreviation on page: also molecular sieves.',
  'source_page': 34},
 {'alias': 'MSA', 'canonical_name': 'methanesulfonic acid', 'entity_type': 'reagent', 'notes': None, 'source_page': 34},
 {'alias': 'MSH', 'canonical_name': 'o-mesitylenesulfonyl hydroxylamine', 'entity_type': 'reagent', 'notes': None, 'source_page': 34},
 {'alias': 'MSTFA',
  'canonical_name': 'N-methyl-N-(trimethylsilyl)trifluoroacetamide',
  'entity_type': 'reagent',
  'notes': None,
  'source_page': 34},
 {'alias': 'MTAD', 'canonical_name': 'N-methyltriazolinedione', 'entity_type': 'reagent', 'notes': None, 'source_page': 34},
 {'alias': 'MTEE',
  'canonical_name': 'methyl tert-butyl ether',
  'entity_type': 'solvent',
  'notes': 'Shown as MTEE (MTBE).',
  'source_page': 34},
 {'alias': 'MTBE',
  'canonical_name': 'methyl tert-butyl ether',
  'entity_type': 'solvent',
  'notes': 'Synonym shown on glossary page.',
  'source_page': 34},
 {'alias': 'MTM', 'canonical_name': 'methylthiomethyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 34},
 {'alias': 'MTO', 'canonical_name': 'methyltrioxorhenium', 'entity_type': 'catalyst', 'notes': None, 'source_page': 34},
 {'alias': 'Mtr',
  'canonical_name': '(4-methoxy-2,3,6-trimethylphenyl)sulfonyl',
  'entity_type': 'protecting_group',
  'notes': None,
  'source_page': 34},
 {'alias': 'MVK', 'canonical_name': 'methyl vinyl ketone', 'entity_type': 'reagent', 'notes': None, 'source_page': 34},
 {'alias': 'mw', 'canonical_name': 'microwave', 'entity_type': 'condition_term', 'notes': None, 'source_page': 34},
 {'alias': 'n', 'canonical_name': 'normal (unbranched alkyl chain)', 'entity_type': 'general_term', 'notes': None, 'source_page': 34},
 {'alias': 'NADPH',
  'canonical_name': 'nicotinamide adenine dinucleotide phosphate',
  'entity_type': 'reagent',
  'notes': 'Biological redox cofactor.',
  'source_page': 34},
 {'alias': 'NaHMDS', 'canonical_name': 'sodium bis(trimethylsilyl)amide', 'entity_type': 'base', 'notes': None, 'source_page': 35},
 {'alias': 'Naph', 'canonical_name': 'naphthyl', 'entity_type': 'substituent', 'notes': 'Shown as Naph (Np).', 'source_page': 35},
 {'alias': 'Np', 'canonical_name': 'naphthyl', 'entity_type': 'substituent', 'notes': 'Alias shown with Naph.', 'source_page': 35},
 {'alias': 'NBA', 'canonical_name': 'N-bromoacetamide', 'entity_type': 'reagent', 'notes': None, 'source_page': 35},
 {'alias': 'NBD', 'canonical_name': 'norbornadiene', 'entity_type': 'ligand', 'notes': 'Shown as NBD (nbd).', 'source_page': 35},
 {'alias': 'nbd',
  'canonical_name': 'norbornadiene',
  'entity_type': 'ligand',
  'notes': 'Lowercase alias shown with NBD.',
  'source_page': 35},
 {'alias': 'NBS', 'canonical_name': 'N-bromosuccinimide', 'entity_type': 'reagent', 'notes': None, 'source_page': 35},
 {'alias': 'NCS', 'canonical_name': 'N-chlorosuccinimide', 'entity_type': 'reagent', 'notes': None, 'source_page': 35},
 {'alias': 'Nf', 'canonical_name': 'nonafluorobutanesulfonyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 35},
 {'alias': 'NHPI', 'canonical_name': 'N-hydroxyphthalimide', 'entity_type': 'reagent', 'notes': None, 'source_page': 35},
 {'alias': 'NIS', 'canonical_name': 'N-iodosuccinimide', 'entity_type': 'reagent', 'notes': None, 'source_page': 35},
 {'alias': 'NMM', 'canonical_name': 'N-methylmorpholine', 'entity_type': 'base', 'notes': None, 'source_page': 35},
 {'alias': 'NMO', 'canonical_name': 'N-methylmorpholine oxide', 'entity_type': 'oxidant', 'notes': None, 'source_page': 35},
 {'alias': 'NMP', 'canonical_name': 'N-methyl-2-pyrrolidone', 'entity_type': 'solvent', 'notes': None, 'source_page': 35},
 {'alias': 'NMR', 'canonical_name': 'nuclear magnetic resonance', 'entity_type': 'analysis_term', 'notes': None, 'source_page': 35},
 {'alias': 'NORPHOS',
  'canonical_name': 'bis(diphenylphosphino)bicyclo[2.2.1]hept-5-ene',
  'entity_type': 'ligand',
  'notes': None,
  'source_page': 35},
 {'alias': 'Nos', 'canonical_name': '4-nitrobenzenesulfonyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 35},
 {'alias': 'NPM', 'canonical_name': 'N-phenylmaleimide', 'entity_type': 'reagent', 'notes': None, 'source_page': 36},
 {'alias': 'NR', 'canonical_name': 'no reaction', 'entity_type': 'general_term', 'notes': None, 'source_page': 36},
 {'alias': 'Ns', 'canonical_name': '2-nitrobenzenesulfonyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 36},
 {'alias': 'NSAID',
  'canonical_name': 'non steroidal anti-inflammatory drug',
  'entity_type': 'general_term',
  'notes': None,
  'source_page': 36},
 {'alias': 'Nuc', 'canonical_name': 'nucleophile (general)', 'entity_type': 'general_term', 'notes': None, 'source_page': 36},
 {'alias': 'o', 'canonical_name': 'ortho', 'entity_type': 'general_term', 'notes': None, 'source_page': 36},
 {'alias': 'Oxone', 'canonical_name': 'potassium peroxymonosulfate', 'entity_type': 'oxidant', 'notes': None, 'source_page': 36},
 {'alias': 'p', 'canonical_name': 'para', 'entity_type': 'general_term', 'notes': None, 'source_page': 36},
 {'alias': 'PAP',
  'canonical_name': '2,8,9-trialkyl-2,5,8,9-tetraaza-1-phospha-bicyclo[3.3.3]undecane',
  'entity_type': 'base',
  'notes': None,
  'source_page': 36},
 {'alias': 'PBP', 'canonical_name': 'pyridinium bromide perbromide', 'entity_type': 'reagent', 'notes': None, 'source_page': 36},
 {'alias': 'PCC', 'canonical_name': 'pyridinium chlorochromate', 'entity_type': 'oxidant', 'notes': None, 'source_page': 36},
 {'alias': 'PDC', 'canonical_name': 'pyridinium dichromate', 'entity_type': 'oxidant', 'notes': None, 'source_page': 36},
 {'alias': 'PEG', 'canonical_name': 'polyethylene glycol', 'entity_type': 'solvent', 'notes': None, 'source_page': 36},
 {'alias': 'Pf', 'canonical_name': '9-phenylfluorenyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 36},
 {'alias': 'pfb', 'canonical_name': 'perfluorobutyrate', 'entity_type': 'reagent', 'notes': None, 'source_page': 36},
 {'alias': 'Ph', 'canonical_name': 'phenyl', 'entity_type': 'substituent', 'notes': None, 'source_page': 36},
 {'alias': 'PHAL', 'canonical_name': 'phthalazine', 'entity_type': 'ligand', 'notes': None, 'source_page': 36},
 {'alias': 'phen', 'canonical_name': '9,10-phenanthroline', 'entity_type': 'ligand', 'notes': None, 'source_page': 36},
 {'alias': 'Phth', 'canonical_name': 'phthaloyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 37},
 {'alias': 'pic', 'canonical_name': '2-pyridinecarboxylate', 'entity_type': 'ligand', 'notes': None, 'source_page': 37},
 {'alias': 'PIFA', 'canonical_name': 'phenyliodonium bis(trifluoroacetate)', 'entity_type': 'oxidant', 'notes': None, 'source_page': 37},
 {'alias': 'Piv', 'canonical_name': 'pivaloyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 37},
 {'alias': 'PLE',
  'canonical_name': 'pig liver esterase',
  'entity_type': 'catalyst',
  'notes': 'Biocatalyst / enzyme abbreviation.',
  'source_page': 37},
 {'alias': 'PMB',
  'canonical_name': 'p-methoxybenzyl',
  'entity_type': 'protecting_group',
  'notes': 'Shown as PMB (MPM).',
  'source_page': 37},
 {'alias': 'MPM',
  'canonical_name': 'p-methoxybenzyl',
  'entity_type': 'protecting_group',
  'notes': 'Alias shown with PMB.',
  'source_page': 37},
 {'alias': 'PMP',
  'canonical_name': '4-methoxyphenyl / 1,2,2,6,6-pentamethylpiperidine',
  'entity_type': 'general_term',
  'notes': 'Ambiguous glossary abbreviation on page.',
  'source_page': 37},
 {'alias': 'PNB', 'canonical_name': 'p-nitrobenzyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 37},
 {'alias': 'PNZ', 'canonical_name': 'p-nitrobenzyloxycarbonyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 37},
 {'alias': 'PPA', 'canonical_name': 'polyphosphoric acid', 'entity_type': 'reagent', 'notes': None, 'source_page': 37},
 {'alias': 'PPI', 'canonical_name': '2-phenyl-2-(2-pyridyl)-2H-imidazole', 'entity_type': 'ligand', 'notes': None, 'source_page': 37},
 {'alias': 'PPL',
  'canonical_name': 'pig pancreatic lipase',
  'entity_type': 'catalyst',
  'notes': 'Biocatalyst / enzyme abbreviation.',
  'source_page': 37},
 {'alias': 'PPO', 'canonical_name': '4-(3-phenylpropyl)pyridine-N-oxide', 'entity_type': 'reagent', 'notes': None, 'source_page': 37},
 {'alias': 'PPSE',
  'canonical_name': 'polyphosphoric acid trimethylsilyl ester',
  'entity_type': 'reagent',
  'notes': None,
  'source_page': 37},
 {'alias': 'PPTS', 'canonical_name': 'pyridinium p-toluenesulfonate', 'entity_type': 'reagent', 'notes': None, 'source_page': 38},
 {'alias': 'Pr', 'canonical_name': 'propyl', 'entity_type': 'substituent', 'notes': None, 'source_page': 38},
 {'alias': 'psi', 'canonical_name': 'pounds per square inch', 'entity_type': 'condition_term', 'notes': None, 'source_page': 38},
 {'alias': 'PT', 'canonical_name': '1-phenyl-1H-tetrazol-5-yl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 38},
 {'alias': 'P.T.', 'canonical_name': 'proton transfer', 'entity_type': 'reaction_term', 'notes': None, 'source_page': 38},
 {'alias': 'PTAB', 'canonical_name': 'phenyltrimethylammonium perbromide', 'entity_type': 'reagent', 'notes': None, 'source_page': 38},
 {'alias': 'PTC', 'canonical_name': 'phase transfer catalyst', 'entity_type': 'catalyst', 'notes': None, 'source_page': 38},
 {'alias': 'PTMSE',
  'canonical_name': '(2-phenyl-2-trimethylsilyl)ethyl',
  'entity_type': 'protecting_group',
  'notes': None,
  'source_page': 38},
 {'alias': 'PTSA',
  'canonical_name': 'p-toluenesulfonic acid',
  'entity_type': 'reagent',
  'notes': 'Shown as PTSA (or TsOH).',
  'source_page': 38},
 {'alias': 'TsOH',
  'canonical_name': 'p-toluenesulfonic acid',
  'entity_type': 'reagent',
  'notes': 'Synonym shown with PTSA.',
  'source_page': 38},
 {'alias': 'PVP', 'canonical_name': 'poly(4-vinylpyridine)', 'entity_type': 'support', 'notes': None, 'source_page': 38},
 {'alias': 'Py', 'canonical_name': 'pyridine', 'entity_type': 'base', 'notes': 'Shown as Py (pyr).', 'source_page': 38},
 {'alias': 'pyr', 'canonical_name': 'pyridine', 'entity_type': 'base', 'notes': 'Alias shown with Py.', 'source_page': 38},
 {'alias': 'r.t.', 'canonical_name': 'room temperature', 'entity_type': 'condition_term', 'notes': None, 'source_page': 38},
 {'alias': 'rac', 'canonical_name': 'racemic', 'entity_type': 'general_term', 'notes': None, 'source_page': 38},
 {'alias': 'RAMP', 'canonical_name': '(R)-1-amino-2-(methoxymethyl)pyrrolidine', 'entity_type': 'ligand', 'notes': None, 'source_page': 38},
 {'alias': 'RaNi', 'canonical_name': 'Raney nickel', 'entity_type': 'catalyst', 'notes': None, 'source_page': 38},
 {'alias': 'RB',
  'canonical_name': 'Rose Bengal',
  'entity_type': 'catalyst',
  'notes': 'Cross-reference abbreviation shown on page xxxviii.',
  'source_page': 38},
 {'alias': 'RCAM', 'canonical_name': 'ring-closing alkyne metathesis', 'entity_type': 'reaction_term', 'notes': None, 'source_page': 38},
 {'alias': 'RCM', 'canonical_name': 'ring-closing metathesis', 'entity_type': 'reaction_term', 'notes': None, 'source_page': 38},
 {'alias': 'Rds',
  'canonical_name': 'rate-determining step',
  'entity_type': 'general_term',
  'notes': 'Shown as Rds (or RDS).',
  'source_page': 38},
 {'alias': 'RDS',
  'canonical_name': 'rate-determining step',
  'entity_type': 'general_term',
  'notes': 'Alias shown with Rds.',
  'source_page': 38},
 {'alias': 'Red-Al',
  'canonical_name': 'sodium bis(2-methoxyethoxy) aluminum hydride',
  'entity_type': 'reagent',
  'notes': None,
  'source_page': 38},
 {'alias': 'Rham', 'canonical_name': 'rhamnosyl', 'entity_type': 'substituent', 'notes': None, 'source_page': 38},
 {'alias': 'Rf',
  'canonical_name': 'perfluoroalkyl group / retention factor in chromatography',
  'entity_type': 'general_term',
  'notes': 'Ambiguous glossary abbreviation on page.',
  'source_page': 38},
 {'alias': 'ROM', 'canonical_name': 'ring-opening metathesis', 'entity_type': 'reaction_term', 'notes': None, 'source_page': 38},
 {'alias': 'ROMP',
  'canonical_name': 'ring-opening metathesis polymerization',
  'entity_type': 'reaction_term',
  'notes': None,
  'source_page': 38},
 {'alias': 'Rose Bengal',
  'canonical_name': "2,4,5,7-tetraiodo-3',4',5',6'-tetrachlorofluorescein disodium salt",
  'entity_type': 'catalyst',
  'notes': 'Photosensitizer; glossary labels it as RB.',
  'source_page': 39},
 {'alias': 's', 'canonical_name': 'seconds (length of reaction time)', 'entity_type': 'condition_term', 'notes': None, 'source_page': 39},
 {'alias': 'S,S,-chiraphos',
  'canonical_name': '(S,S)-2,3-bis(diphenylphosphino)butane',
  'entity_type': 'ligand',
  'notes': None,
  'source_page': 39},
 {'alias': 'Salen',
  'canonical_name': "N,N'-ethylenebis(salicylideneiminato) bis(salicylidene)",
  'entity_type': 'ligand',
  'notes': None,
  'source_page': 39},
 {'alias': 'salophen', 'canonical_name': 'o-phenylenebis(salicylideneimino)', 'entity_type': 'ligand', 'notes': None, 'source_page': 39},
 {'alias': 'SAMP', 'canonical_name': '(S)-1-amino-2-(methoxymethyl)pyrrolidine', 'entity_type': 'ligand', 'notes': None, 'source_page': 39},
 {'alias': 'SC CO2', 'canonical_name': 'supercritical carbon-dioxide', 'entity_type': 'solvent', 'notes': None, 'source_page': 39},
 {'alias': 'SDS', 'canonical_name': 'sodium dodecylsulfate', 'entity_type': 'additive', 'notes': None, 'source_page': 39},
 {'alias': 'sec', 'canonical_name': 'secondary', 'entity_type': 'general_term', 'notes': None, 'source_page': 39},
 {'alias': 'SEM', 'canonical_name': '2-(trimethylsilyl)ethoxymethyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 39},
 {'alias': 'SES',
  'canonical_name': '2-[(trimethylsilyl)ethyl]sulfonyl',
  'entity_type': 'protecting_group',
  'notes': None,
  'source_page': 39},
 {'alias': 'SET', 'canonical_name': 'single electron transfer', 'entity_type': 'reaction_term', 'notes': None, 'source_page': 39},
 {'alias': 'Sia',
  'canonical_name': '1,2-dimethylpropyl (secondary isoamyl)',
  'entity_type': 'substituent',
  'notes': None,
  'source_page': 39},
 {'alias': 'SPB', 'canonical_name': 'sodium perborate', 'entity_type': 'oxidant', 'notes': None, 'source_page': 39},
 {'alias': 'TADDOL',
  'canonical_name': "2,2-dimethyl-α,α,α',α'-tetraaryl-1,3-dioxolane-4,5-dimethanol",
  'entity_type': 'ligand',
  'notes': None,
  'source_page': 40},
 {'alias': 'TASF',
  'canonical_name': 'tris(diethylamino)sulfonium difluorotrimethylsilicate',
  'entity_type': 'reagent',
  'notes': None,
  'source_page': 40},
 {'alias': 'TBAB', 'canonical_name': 'tetra-n-butylammonium bromide', 'entity_type': 'additive', 'notes': None, 'source_page': 40},
 {'alias': 'TBAF', 'canonical_name': 'tetra-n-butylammonium fluoride', 'entity_type': 'reagent', 'notes': None, 'source_page': 40},
 {'alias': 'TBAI', 'canonical_name': 'tetra-n-butylammonium iodide', 'entity_type': 'additive', 'notes': None, 'source_page': 40},
 {'alias': 'TBCO', 'canonical_name': 'tetrabromocyclohexadienone', 'entity_type': 'reagent', 'notes': None, 'source_page': 40},
 {'alias': 'TBDMS',
  'canonical_name': 't-butyldimethylsilyl',
  'entity_type': 'protecting_group',
  'notes': 'Shown as TBDMS (TBS).',
  'source_page': 40},
 {'alias': 'TBS',
  'canonical_name': 't-butyldimethylsilyl',
  'entity_type': 'protecting_group',
  'notes': 'Alias shown with TBDMS.',
  'source_page': 40},
 {'alias': 'TBDPS',
  'canonical_name': 't-butyldiphenylsilyl',
  'entity_type': 'protecting_group',
  'notes': 'Shown as TBDPS (BPS).',
  'source_page': 40},
 {'alias': 'BPS',
  'canonical_name': 't-butyldiphenylsilyl',
  'entity_type': 'protecting_group',
  'notes': 'Alias shown with TBDPS.',
  'source_page': 40},
 {'alias': 'TBH', 'canonical_name': 'tert-butyl hypochlorite', 'entity_type': 'oxidant', 'notes': None, 'source_page': 40},
 {'alias': 'TBHP', 'canonical_name': 'tert-butyl hydroperoxide', 'entity_type': 'oxidant', 'notes': None, 'source_page': 40},
 {'alias': 'TBP', 'canonical_name': 'tributylphosphine', 'entity_type': 'reagent', 'notes': None, 'source_page': 40},
 {'alias': 'TBT', 'canonical_name': '1-tert-butyl-1H-tetrazol-5-yl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 40},
 {'alias': 'TBTH', 'canonical_name': 'tributyltin hydride', 'entity_type': 'reagent', 'notes': None, 'source_page': 40},
 {'alias': 'TBTSP', 'canonical_name': 't-butyl trimethylsilyl peroxide', 'entity_type': 'oxidant', 'notes': None, 'source_page': 40},
 {'alias': 'TCCA', 'canonical_name': 'trichloroisocyanuric acid', 'entity_type': 'oxidant', 'notes': None, 'source_page': 41},
 {'alias': 'TCDI', 'canonical_name': 'thiocarbonyl diimidazole', 'entity_type': 'reagent', 'notes': None, 'source_page': 41},
 {'alias': 'TCNE', 'canonical_name': 'tetracyanoethylene', 'entity_type': 'reagent', 'notes': None, 'source_page': 41},
 {'alias': 'TCNQ', 'canonical_name': '7,7,8,8-tetracyano-p-quinodimethane', 'entity_type': 'reagent', 'notes': None, 'source_page': 41},
 {'alias': 'TDS', 'canonical_name': 'dimethyl thexylsilyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 41},
 {'alias': 'TEA', 'canonical_name': 'triethylamine', 'entity_type': 'base', 'notes': None, 'source_page': 41},
 {'alias': 'TEBACI', 'canonical_name': 'benzyl trimethylammonium chloride', 'entity_type': 'additive', 'notes': None, 'source_page': 41},
 {'alias': 'TEMPO',
  'canonical_name': '2,2,6,6-tetramethyl-1-piperidinyloxy free radical',
  'entity_type': 'catalyst',
  'notes': None,
  'source_page': 41},
 {'alias': 'Teoc',
  'canonical_name': '2-(trimethylsilyl)ethoxycarbonyl',
  'entity_type': 'protecting_group',
  'notes': None,
  'source_page': 41},
 {'alias': 'TEP', 'canonical_name': 'triethylphosphite', 'entity_type': 'reagent', 'notes': None, 'source_page': 41},
 {'alias': 'TES', 'canonical_name': 'triethylsilyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 41},
 {'alias': 'Tf', 'canonical_name': 'trifluoromethanesulfonyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 41},
 {'alias': 'TFA', 'canonical_name': 'trifluoroacetic acid', 'entity_type': 'reagent', 'notes': None, 'source_page': 41},
 {'alias': 'Tfa', 'canonical_name': 'trifluoroacetamide', 'entity_type': 'reagent', 'notes': None, 'source_page': 41},
 {'alias': 'TFAA', 'canonical_name': 'trifluoroacetic anhydride', 'entity_type': 'reagent', 'notes': None, 'source_page': 42},
 {'alias': 'TFE', 'canonical_name': '2,2,2-trifluoroethanol', 'entity_type': 'solvent', 'notes': None, 'source_page': 42},
 {'alias': 'TFMSA',
  'canonical_name': 'trifluoromethanesulfonic acid',
  'entity_type': 'reagent',
  'notes': 'Triflic acid.',
  'source_page': 42},
 {'alias': 'TFP', 'canonical_name': 'tris(2-furyl)phosphine', 'entity_type': 'ligand', 'notes': None, 'source_page': 42},
 {'alias': 'Th', 'canonical_name': '2-thienyl', 'entity_type': 'substituent', 'notes': None, 'source_page': 42},
 {'alias': 'thexyl', 'canonical_name': '1,1,2-trimethylpropyl', 'entity_type': 'substituent', 'notes': None, 'source_page': 42},
 {'alias': 'THF', 'canonical_name': 'tetrahydrofuran', 'entity_type': 'solvent', 'notes': None, 'source_page': 42},
 {'alias': 'THP', 'canonical_name': '2-tetrahydropyranyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 42},
 {'alias': 'TIPB', 'canonical_name': '1,3,5-triisopropylbenzene', 'entity_type': 'solvent', 'notes': None, 'source_page': 42},
 {'alias': 'TIPS', 'canonical_name': 'triisopropylsilyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 42},
 {'alias': 'TMAO',
  'canonical_name': 'trimethylamine N-oxide',
  'entity_type': 'reagent',
  'notes': 'Shown as TMAO (TMANO).',
  'source_page': 42},
 {'alias': 'TMANO',
  'canonical_name': 'trimethylamine N-oxide',
  'entity_type': 'reagent',
  'notes': 'Alias shown with TMAO.',
  'source_page': 42},
 {'alias': 'TMEDA', 'canonical_name': "N,N,N',N'-tetramethylethylenediamine", 'entity_type': 'base', 'notes': None, 'source_page': 42},
 {'alias': 'TMG', 'canonical_name': '1,1,3,3-tetramethylguanidine', 'entity_type': 'base', 'notes': None, 'source_page': 42},
 {'alias': 'TMGA', 'canonical_name': 'tetramethylguanidinium azide', 'entity_type': 'reagent', 'notes': None, 'source_page': 42},
 {'alias': 'Tmob', 'canonical_name': '2,4,6-trimethoxybenzyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 43},
 {'alias': 'TMP', 'canonical_name': '2,2,6,6-tetramethylpiperidine', 'entity_type': 'base', 'notes': None, 'source_page': 43},
 {'alias': 'TMS', 'canonical_name': 'trimethylsilyl', 'entity_type': 'protecting_group', 'notes': None, 'source_page': 43},
 {'alias': 'TMSA', 'canonical_name': 'trimethylsilyl azide', 'entity_type': 'reagent', 'notes': None, 'source_page': 43},
 {'alias': 'TMSEE', 'canonical_name': '(trimethylsilyl)ethynyl ether', 'entity_type': 'reagent', 'notes': None, 'source_page': 43},
 {'alias': 'TMU', 'canonical_name': 'tetramethylurea', 'entity_type': 'solvent', 'notes': None, 'source_page': 43},
 {'alias': 'TNM', 'canonical_name': 'tetranitromethane', 'entity_type': 'reagent', 'notes': None, 'source_page': 43},
 {'alias': 'Tol', 'canonical_name': 'p-tolyl', 'entity_type': 'substituent', 'notes': None, 'source_page': 43},
 {'alias': 'tolbinap',
  'canonical_name': "2,2'-bis(di-p-tolylphosphino)-1,1'-binaphthyl",
  'entity_type': 'ligand',
  'notes': None,
  'source_page': 43},
 {'alias': 'TPAP', 'canonical_name': 'tetra-n-propylammonium perruthenate', 'entity_type': 'oxidant', 'notes': None, 'source_page': 43},
 {'alias': 'TPP',
  'canonical_name': 'triphenylphosphine / 5,10,15,20-tetraphenylporphyrin',
  'entity_type': 'general_term',
  'notes': 'Ambiguous glossary abbreviation on page.',
  'source_page': 43}]

MANUAL_FAMILY_BATCH3_SEEDS = [{'common_conditions': None,
  'common_solvents': None,
  'description_short': 'Intramolecular alkyne metathesis forming cyclic unsaturated products.',
  'family_class': 'metathesis',
  'family_name': 'Ring-Closing Alkyne Metathesis',
  'key_reagents_clue': 'RCAM; alkyne metathesis catalyst',
  'mechanism_type': 'metal-carbynes / metathesis',
  'product_pattern_text': 'cyclic alkyne / enyne product',
  'reactant_pattern_text': 'diyne or alkyne tether',
  'seeded_from': 'frontmatter_batch3',
  'synonym_names': 'RCAM',
  'transformation_type': 'alkyne cyclization'},
 {'common_conditions': None,
  'common_solvents': None,
  'description_short': 'Intramolecular alkene metathesis forming cyclic alkenes.',
  'family_class': 'metathesis',
  'family_name': 'Ring-Closing Metathesis',
  'key_reagents_clue': 'RCM; alkene metathesis catalyst',
  'mechanism_type': 'alkene metathesis',
  'product_pattern_text': 'cyclic alkene product',
  'reactant_pattern_text': 'diene tether',
  'seeded_from': 'frontmatter_batch3',
  'synonym_names': 'RCM',
  'transformation_type': 'alkene cyclization'},
 {'common_conditions': None,
  'common_solvents': None,
  'description_short': 'Metathesis-driven opening of strained cyclic alkenes.',
  'family_class': 'metathesis',
  'family_name': 'Ring-Opening Metathesis',
  'key_reagents_clue': 'ROM',
  'mechanism_type': 'alkene metathesis',
  'product_pattern_text': 'opened unsaturated chain',
  'reactant_pattern_text': 'strained cyclic alkene',
  'seeded_from': 'frontmatter_batch3',
  'synonym_names': 'ROM',
  'transformation_type': 'strained ring opening'},
 {'common_conditions': None,
  'common_solvents': None,
  'description_short': 'Polymerization of strained cyclic olefins through ring-opening metathesis.',
  'family_class': 'metathesis polymerization',
  'family_name': 'Ring-Opening Metathesis Polymerization',
  'key_reagents_clue': 'ROMP',
  'mechanism_type': 'alkene metathesis polymerization',
  'product_pattern_text': 'metathesis polymer',
  'reactant_pattern_text': 'strained cyclic olefin monomer',
  'seeded_from': 'frontmatter_batch3',
  'synonym_names': 'ROMP',
  'transformation_type': 'polymerization'}]

PAGE_LABEL_BY_NO = {row['page_no']: row['page_label'] for row in PAGE_KNOWLEDGE_SEEDS}


def _insert_page_knowledge(con: sqlite3.Connection) -> int:
    if not _table_exists(con, 'manual_page_knowledge'):
        return 0
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
    for seed in CURATED_FRONTMATTER_BATCH3_ABBREVIATIONS:
        alias_norm = normalize_key(seed['alias'])
        canon_norm = normalize_key(seed['canonical_name'])
        con.execute(
            """
            INSERT OR IGNORE INTO abbreviation_aliases (
              alias, alias_norm, canonical_name, canonical_name_norm, entity_type,
              smiles, molblock, notes, source_label, source_page, confidence, updated_at
            ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, 'front_matter_seed_batch3', ?, 0.94, datetime('now'))
            """,
            (seed['alias'], alias_norm, seed['canonical_name'], canon_norm, seed['entity_type'], seed.get('notes'), seed['source_page']),
        )
        inserted += 1
    return inserted


def _seed_manual_family_patterns(con: sqlite3.Connection) -> int:
    if not _table_exists(con, 'reaction_family_patterns'):
        return 0
    changes = 0
    for seed in MANUAL_FAMILY_BATCH3_SEEDS:
        family_name_norm = normalize_key(seed['family_name'])
        con.execute(
            """
            INSERT INTO reaction_family_patterns (
              family_name, family_name_norm, family_class, transformation_type, mechanism_type,
              reactant_pattern_text, product_pattern_text, key_reagents_clue,
              common_solvents, common_conditions, synonym_names, description_short,
              evidence_extract_count, overview_count, application_count, mechanism_count,
              latest_source_zip, latest_updated_at, seeded_from, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 'named_reactions_frontmatter_batch3', datetime('now'), ?, datetime('now'))
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
                    WHEN reaction_family_patterns.synonym_names IS NULL OR trim(reaction_family_patterns.synonym_names) = '' THEN excluded.synonym_names
                    WHEN instr(lower(reaction_family_patterns.synonym_names), lower(excluded.synonym_names)) > 0 THEN reaction_family_patterns.synonym_names
                    ELSE reaction_family_patterns.synonym_names || '|' || excluded.synonym_names
                END,
              description_short=COALESCE(reaction_family_patterns.description_short, excluded.description_short),
              latest_source_zip='named_reactions_frontmatter_batch3',
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
    for row in CURATED_FRONTMATTER_BATCH3_ABBREVIATIONS:
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
    for fam in MANUAL_FAMILY_BATCH3_SEEDS:
        seeds.append({
            'page_label': 'xxxviii',
            'entity_text': fam['synonym_names'],
            'canonical_name': fam['family_name'],
            'entity_type': 'reaction_family',
            'family_name': fam['family_name'],
            'notes': 'Named reaction abbreviation from glossary page.',
            'confidence': 0.95,
        })
    return seeds


MANUAL_PAGE_ENTITY_BATCH3_SEEDS = _build_page_entity_seeds()


def _seed_page_entities(con: sqlite3.Connection) -> int:
    if not _table_exists(con, 'manual_page_entities') or not _table_exists(con, 'manual_page_knowledge'):
        return 0
    page_lookup = {row[0]: row[1] for row in con.execute("SELECT page_label, id FROM manual_page_knowledge WHERE source_label = 'named_reactions_frontmatter_batch3'").fetchall()}
    alias_lookup = {}
    if _table_exists(con, 'abbreviation_aliases'):
        alias_lookup = {row[0]: row[1] for row in con.execute('SELECT alias_norm, id FROM abbreviation_aliases').fetchall()}
    inserted = 0
    for seed in MANUAL_PAGE_ENTITY_BATCH3_SEEDS:
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


def get_frontmatter_batch3_counts(db_path: str | Path, con: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
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
            'frontmatter_batch3_pages': q("SELECT COUNT(*) FROM manual_page_knowledge WHERE source_label = 'named_reactions_frontmatter_batch3'") if _table_exists(con, 'manual_page_knowledge') else 0,
            'frontmatter_batch3_abbreviation_aliases': q("SELECT COUNT(*) FROM abbreviation_aliases WHERE source_label = 'front_matter_seed_batch3'") if _table_exists(con, 'abbreviation_aliases') else 0,
            'frontmatter_batch3_manual_families': q("SELECT COUNT(*) FROM reaction_family_patterns WHERE latest_source_zip = 'named_reactions_frontmatter_batch3'") if _table_exists(con, 'reaction_family_patterns') else 0,
        }
    finally:
        if close_after:
            con.close()


def apply_frontmatter_batch3(db_path: str | Path) -> Dict[str, Any]:
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
                INSERT INTO labint_schema_meta(key, value) VALUES('frontmatter_batch3_last_applied_at', datetime('now'))
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')
            """)
        con.commit()
        out = get_frontmatter_batch3_counts(db_path, con)
        out.update({'page_seed_rows': page_rows, 'alias_seed_rows': alias_rows, 'manual_family_seed_ops': family_rows, 'page_entity_seed_rows': entity_rows})
        return out
    finally:
        con.close()


def export_frontmatter_batch3_seed_templates(output_dir: str | Path) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    with (out / 'frontmatter_batch3_manual_pages.csv').open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['source_label', 'page_label', 'page_no', 'title', 'section_name', 'page_kind', 'summary', 'family_names', 'reference_family_name', 'notes', 'image_filename'])
        writer.writeheader()
        for row in PAGE_KNOWLEDGE_SEEDS:
            writer.writerow(row)
    with (out / 'frontmatter_batch3_abbreviation_seed.csv').open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['alias', 'canonical_name', 'entity_type', 'source_page', 'notes'])
        writer.writeheader()
        for row in CURATED_FRONTMATTER_BATCH3_ABBREVIATIONS:
            writer.writerow(row)
