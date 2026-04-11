from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

FRONTMATTER_SCHEMA_VERSION = "labint_frontmatter_batch1_v1_20260411"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS manual_page_knowledge (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_label TEXT NOT NULL,
  page_label TEXT NOT NULL,
  page_no INTEGER,
  title TEXT,
  section_name TEXT,
  page_kind TEXT NOT NULL,
  summary TEXT,
  family_names TEXT,
  reference_family_name TEXT,
  notes TEXT,
  image_filename TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(source_label, page_label)
);
CREATE INDEX IF NOT EXISTS idx_manual_page_knowledge_kind ON manual_page_knowledge(page_kind);
CREATE INDEX IF NOT EXISTS idx_manual_page_knowledge_ref_family ON manual_page_knowledge(reference_family_name);

CREATE TABLE IF NOT EXISTS manual_page_entities (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  page_knowledge_id INTEGER NOT NULL,
  entity_text TEXT NOT NULL,
  entity_text_norm TEXT NOT NULL,
  canonical_name TEXT,
  entity_type TEXT NOT NULL,
  alias_id INTEGER,
  family_name TEXT,
  notes TEXT,
  confidence REAL NOT NULL DEFAULT 0.7,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (page_knowledge_id) REFERENCES manual_page_knowledge(id) ON DELETE CASCADE,
  FOREIGN KEY (alias_id) REFERENCES abbreviation_aliases(id) ON DELETE SET NULL,
  UNIQUE(page_knowledge_id, entity_text_norm, entity_type)
);
CREATE INDEX IF NOT EXISTS idx_manual_page_entities_page ON manual_page_entities(page_knowledge_id, entity_type);
CREATE INDEX IF NOT EXISTS idx_manual_page_entities_norm ON manual_page_entities(entity_text_norm, entity_type);
CREATE UNIQUE INDEX IF NOT EXISTS idx_family_references_unique_frontmatter ON family_references(family_name, citation_text, source_page, reference_group);

CREATE VIEW IF NOT EXISTS v_manual_frontmatter_lookup AS
SELECT
  mpk.id AS page_knowledge_id,
  mpk.source_label,
  mpk.page_label,
  mpk.page_no,
  mpk.title,
  mpk.page_kind,
  mpk.summary,
  mpe.id AS page_entity_id,
  mpe.entity_text,
  mpe.canonical_name,
  mpe.entity_type,
  mpe.family_name,
  mpe.notes,
  mpe.confidence,
  aa.alias,
  aa.canonical_name AS alias_canonical_name
FROM manual_page_knowledge mpk
LEFT JOIN manual_page_entities mpe ON mpe.page_knowledge_id = mpk.id
LEFT JOIN abbreviation_aliases aa ON aa.id = mpe.alias_id;
"""


def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ?", (name,)
    ).fetchone()
    return row is not None


def normalize_key(text: Optional[str]) -> str:
    text = (text or "").strip().lower()
    out = []
    last_space = False
    for ch in text:
        if ch.isalnum():
            out.append(ch)
            last_space = False
        else:
            if not last_space:
                out.append(" ")
                last_space = True
    return " ".join("".join(out).split())


def ensure_frontmatter_schema(db_path: str | Path) -> None:
    db_path = Path(db_path)
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("PRAGMA foreign_keys=ON")
        con.executescript(SCHEMA_SQL)
        if _table_exists(con, "labint_schema_meta"):
            con.execute(
                """
                INSERT INTO labint_schema_meta(key, value) VALUES('frontmatter_schema_version', ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')
                """,
                (FRONTMATTER_SCHEMA_VERSION,),
            )
        con.commit()
    finally:
        con.close()


PAGE_KNOWLEDGE_SEEDS: List[Dict[str, Any]] = [
    {
        "source_label": "named_reactions_frontmatter_batch1",
        "page_label": "xiv",
        "page_no": 14,
        "title": "IV. Explanation of the Use of Colors in the Schemes and Text",
        "section_name": "IV. Explanation of the Use of Colors",
        "page_kind": "meta_explanatory",
        "summary": "Explains the textbook color convention and introduces example named reactions and target-molecule context. Family names explicitly visible include acetoacetic ester synthesis, retro-Claisen reaction, Simmons-Smith, Furukawa modification, and Charette asymmetric modification.",
        "family_names": "Acetoacetic Ester Synthesis|Retro-Claisen Reaction|Simmons-Smith Reaction|Furukawa Modification|Charette Asymmetric Cyclopropanation",
        "reference_family_name": None,
        "notes": "Useful as family seed page rather than direct application example.",
        "image_filename": "named reactions_14.jpg",
    },
    {
        "source_label": "named_reactions_frontmatter_batch1",
        "page_label": "xv",
        "page_no": 15,
        "title": "Mechanistic Schemes: Suzuki cross-coupling and Swern oxidation",
        "section_name": "IV. Explanation of the Use of Colors",
        "page_kind": "mechanism_demo",
        "summary": "Contains the catalytic cycle of the Suzuki cross-coupling and a detailed mechanism panel for Swern oxidation, including DMSO activation and alkoxysulfonium intermediates.",
        "family_names": "Suzuki Cross-Coupling|Swern Oxidation",
        "reference_family_name": None,
        "notes": "High-value family-pattern seed page.",
        "image_filename": "named reactions_15.jpg",
    },
    {
        "source_label": "named_reactions_frontmatter_batch1",
        "page_label": "xvi",
        "page_no": 16,
        "title": "2-aza-Cope example and Dakin oxidation references",
        "section_name": "IV. Explanation of the Use of Colors",
        "page_kind": "reference_explanatory",
        "summary": "Shows a 2-aza-Cope rearrangement example, epothilone B context, and a reference block for Dakin oxidation. Also explicitly states that the Dakin oxidation is mechanistically similar to Baeyer-Villiger oxidation.",
        "family_names": "Aza-Cope Rearrangement|Dakin Oxidation|Baeyer-Villiger Oxidation",
        "reference_family_name": "Dakin Oxidation",
        "notes": "High-value reference anchor page.",
        "image_filename": "named reactions_16.jpg",
    },
    {
        "source_label": "named_reactions_frontmatter_batch1",
        "page_label": "xvii",
        "page_no": 17,
        "title": "V. List of Abbreviations (18-Cr-6 to Ar)",
        "section_name": "V. List of Abbreviations",
        "page_kind": "abbreviation_glossary",
        "summary": "Abbreviation glossary page covering common reaction terms, protecting groups, reagents, and general solvent descriptors from 18-Cr-6 through Ar.",
        "family_names": None,
        "reference_family_name": None,
        "notes": "Alias-normalization seed page.",
        "image_filename": "named reactions_17.jpg",
    },
    {
        "source_label": "named_reactions_frontmatter_batch1",
        "page_label": "xviii",
        "page_no": 18,
        "title": "V. List of Abbreviations (ATD to BINAP)",
        "section_name": "V. List of Abbreviations",
        "page_kind": "abbreviation_glossary",
        "summary": "Abbreviation glossary page covering ATD to BINAP, including 9-BBN, BHT, BINAL-H, BINAP, and other named ligands/additives.",
        "family_names": None,
        "reference_family_name": None,
        "notes": "Alias-normalization seed page.",
        "image_filename": "named reactions_18.jpg",
    },
    {
        "source_label": "named_reactions_frontmatter_batch1",
        "page_label": "xix",
        "page_no": 19,
        "title": "V. List of Abbreviations (BINOL to BPS/TBDPS)",
        "section_name": "V. List of Abbreviations",
        "page_kind": "abbreviation_glossary",
        "summary": "Abbreviation glossary page covering BINOL to BPS/TBDPS, including Boc, BOP-Cl, BPD, and BPO.",
        "family_names": None,
        "reference_family_name": None,
        "notes": "Alias-normalization seed page.",
        "image_filename": "named reactions_19.jpg",
    },
    {
        "source_label": "named_reactions_frontmatter_batch1",
        "page_label": "xx",
        "page_no": 20,
        "title": "V. List of Abbreviations (BQ to c)",
        "section_name": "V. List of Abbreviations",
        "page_kind": "abbreviation_glossary",
        "summary": "Abbreviation glossary page covering BQ to c, including Bt, BTAF, BTMSA, BTSA, and benzyltrialkylammonium salts.",
        "family_names": None,
        "reference_family_name": None,
        "notes": "Alias-normalization seed page.",
        "image_filename": "named reactions_20.jpg",
    },
    {
        "source_label": "named_reactions_frontmatter_batch1",
        "page_label": "xxi",
        "page_no": 21,
        "title": "V. List of Abbreviations (ca to CPTS)",
        "section_name": "V. List of Abbreviations",
        "page_kind": "abbreviation_glossary",
        "summary": "Abbreviation glossary page covering ca to CPTS, including CAN, CBS, CDI, COD, COT, and cross metathesis abbreviation CM/XMET.",
        "family_names": "Cross Metathesis",
        "reference_family_name": None,
        "notes": "Alias-normalization seed page.",
        "image_filename": "named reactions_21.jpg",
    },
    {
        "source_label": "named_reactions_frontmatter_batch1",
        "page_label": "xxii",
        "page_no": 22,
        "title": "V. List of Abbreviations (CRA to DBI)",
        "section_name": "V. List of Abbreviations",
        "page_kind": "abbreviation_glossary",
        "summary": "Abbreviation glossary page covering CRA to DBI, including CSA, CSI, CTAB, DABCO, DAST, DBA, DBAD, and DBI.",
        "family_names": None,
        "reference_family_name": None,
        "notes": "Alias-normalization seed page.",
        "image_filename": "named reactions_22.jpg",
    },
    {
        "source_label": "named_reactions_frontmatter_batch1",
        "page_label": "xxiii",
        "page_no": 23,
        "title": "V. List of Abbreviations (DBM to de)",
        "section_name": "V. List of Abbreviations",
        "page_kind": "abbreviation_glossary",
        "summary": "Abbreviation glossary page covering DBM to de, including DBU, DCC, DCE, DCM, DDQ, and de.",
        "family_names": None,
        "reference_family_name": None,
        "notes": "Alias-normalization seed page.",
        "image_filename": "named reactions_23.jpg",
    },
]

# Additional curated alias seeds from pages xvii-xxiii.
CURATED_FRONTMATTER_ABBREVIATIONS: List[Dict[str, Any]] = [
    # p.17
    {"alias": "Ac", "canonical_name": "acetyl", "entity_type": "protecting_group", "source_page": 17, "notes": None},
    {"alias": "acac", "canonical_name": "acetylacetonyl", "entity_type": "ligand", "source_page": 17, "notes": None},
    {"alias": "AA", "canonical_name": "asymmetric aminohydroxylation", "entity_type": "reaction_term", "source_page": 17, "notes": None},
    {"alias": "AD", "canonical_name": "asymmetric dihydroxylation", "entity_type": "reaction_term", "source_page": 17, "notes": None},
    {"alias": "ad", "canonical_name": "adamantyl", "entity_type": "substituent", "source_page": 17, "notes": None},
    {"alias": "ADDP", "canonical_name": "1,1'-(azodicarbonyl)dipiperidine", "entity_type": "reagent", "source_page": 17, "notes": None},
    {"alias": "ADMET", "canonical_name": "acyclic diene metathesis polymerization", "entity_type": "reaction_term", "source_page": 17, "notes": None},
    {"alias": "acaen", "canonical_name": "N,N'-bis(1-methyl-3-oxobutylidene)ethylenediamine", "entity_type": "ligand", "source_page": 17, "notes": None},
    {"alias": "Alloc", "canonical_name": "allyloxycarbonyl", "entity_type": "protecting_group", "source_page": 17, "notes": None},
    {"alias": "Am", "canonical_name": "amyl (n-pentyl)", "entity_type": "substituent", "source_page": 17, "notes": None},
    {"alias": "An", "canonical_name": "p-anisyl", "entity_type": "substituent", "source_page": 17, "notes": None},
    {"alias": "ANRORC", "canonical_name": "anionic ring-opening ring-closing", "entity_type": "reaction_term", "source_page": 17, "notes": None},
    {"alias": "aq", "canonical_name": "aqueous", "entity_type": "condition_term", "source_page": 17, "notes": None},
    {"alias": "Ar", "canonical_name": "aryl (substituted aromatic ring)", "entity_type": "substituent", "source_page": 17, "notes": None},
    # p.18
    {"alias": "ATD", "canonical_name": "aluminum tris(2,6-di-tert-butyl-4-methylphenoxide)", "entity_type": "reagent", "source_page": 18, "notes": None},
    {"alias": "atm", "canonical_name": "1 atmosphere = 10^5 Pa (pressure)", "entity_type": "condition_term", "source_page": 18, "notes": None},
    {"alias": "ATPH", "canonical_name": "aluminum tris(2,6-diphenylphenoxide)", "entity_type": "reagent", "source_page": 18, "notes": None},
    {"alias": "BCME", "canonical_name": "bis(chloromethyl)ether", "entity_type": "reagent", "source_page": 18, "notes": None},
    {"alias": "BCN", "canonical_name": "N-benzyloxycarbonyloxy-5-norbornene-2,3-dicarboximide", "entity_type": "reagent", "source_page": 18, "notes": None},
    {"alias": "BDPP", "canonical_name": "bis(diphenylphosphino)pentane", "entity_type": "ligand", "source_page": 18, "notes": "(2R,4R) or (2S,4S) stereochemical variants shown."},
    {"alias": "BER", "canonical_name": "borohydride exchange resin", "entity_type": "reagent", "source_page": 18, "notes": None},
    {"alias": "BHT", "canonical_name": "2,6-di-tert-butyl-p-cresol", "entity_type": "additive", "source_page": 18, "notes": "butylated hydroxytoluene"},
    {"alias": "BICP", "canonical_name": "2(R)-2'-(R)-bis(diphenylphosphino)-1(R),1'(R)-dicyclopentane", "entity_type": "ligand", "source_page": 18, "notes": "Complex chiral diphosphine ligand; transcription conservatively normalized."},
    # p.19
    {"alias": "Bip", "canonical_name": "biphenyl-4-sulfonyl", "entity_type": "substituent", "source_page": 19, "notes": None},
    {"alias": "bipy", "canonical_name": "2,2'-bipyridyl", "entity_type": "ligand", "source_page": 19, "notes": None},
    {"alias": "BLA", "canonical_name": "Brønsted acid assisted chiral Lewis acid", "entity_type": "reaction_term", "source_page": 19, "notes": None},
    {"alias": "bmin", "canonical_name": "1-butyl-3-methylimidazolium cation", "entity_type": "solvent", "source_page": 19, "notes": "ionic-liquid cation descriptor"},
    {"alias": "Bn", "canonical_name": "benzyl", "entity_type": "substituent", "source_page": 19, "notes": None},
    {"alias": "BNAH", "canonical_name": "1-benzyl-1,4-dihydronicotinamide", "entity_type": "reagent", "source_page": 19, "notes": None},
    {"alias": "BOB", "canonical_name": "4-benzyloxybutyryl", "entity_type": "substituent", "source_page": 19, "notes": None},
    {"alias": "BOM", "canonical_name": "benzyloxymethyl", "entity_type": "protecting_group", "source_page": 19, "notes": None},
    {"alias": "bp", "canonical_name": "boiling point", "entity_type": "condition_term", "source_page": 19, "notes": None},
    {"alias": "BPD", "canonical_name": "bis(pinacolato)diboron", "entity_type": "reagent", "source_page": 19, "notes": None},
    {"alias": "BPS", "canonical_name": "tert-butyldiphenylsilyl", "entity_type": "protecting_group", "source_page": 19, "notes": "Shown as BPS (TBDPS)."},
    {"alias": "TBDPS", "canonical_name": "tert-butyldiphenylsilyl", "entity_type": "protecting_group", "source_page": 19, "notes": "Alias of BPS on glossary page."},
    # p.20
    {"alias": "BQ", "canonical_name": "benzoquinone", "entity_type": "reagent", "source_page": 20, "notes": None},
    {"alias": "Bs", "canonical_name": "brosyl = (4-bromobenzenesulfonyl)", "entity_type": "protecting_group", "source_page": 20, "notes": None},
    {"alias": "Bt", "canonical_name": "1- or 2-benzotriazolyl", "entity_type": "substituent", "source_page": 20, "notes": None},
    {"alias": "BTAF", "canonical_name": "benzyltrimethylammonium fluoride", "entity_type": "reagent", "source_page": 20, "notes": None},
    {"alias": "BTEA", "canonical_name": "benzyltriethylammonium", "entity_type": "additive", "source_page": 20, "notes": None},
    {"alias": "BTEAC", "canonical_name": "benzyltriethylammonium chloride", "entity_type": "additive", "source_page": 20, "notes": None},
    {"alias": "BTFP", "canonical_name": "3-bromo-1,1,1-trifluoro-propan-2-one", "entity_type": "reagent", "source_page": 20, "notes": None},
    {"alias": "BTMA", "canonical_name": "benzyltrimethylammonium", "entity_type": "additive", "source_page": 20, "notes": None},
    {"alias": "BTMSA", "canonical_name": "bis(trimethylsilyl)acetylene", "entity_type": "reagent", "source_page": 20, "notes": None},
    {"alias": "BTS", "canonical_name": "bis(trimethylsilyl) sulfate", "entity_type": "reagent", "source_page": 20, "notes": None},
    {"alias": "BTSA", "canonical_name": "benzothiazole 2-sulfonic acid", "entity_type": "reagent", "source_page": 20, "notes": None},
    {"alias": "BTSP", "canonical_name": "bis(trimethylsilyl) peroxide", "entity_type": "reagent", "source_page": 20, "notes": None},
    {"alias": "Bz", "canonical_name": "benzoyl", "entity_type": "substituent", "source_page": 20, "notes": None},
    {"alias": "Bu", "canonical_name": "n-butyl", "entity_type": "substituent", "source_page": 20, "notes": "Shown as Bu (nBu)."},
    {"alias": "nBu", "canonical_name": "n-butyl", "entity_type": "substituent", "source_page": 20, "notes": "Shown as Bu (nBu)."},
    {"alias": "c", "canonical_name": "cyclo", "entity_type": "substituent", "source_page": 20, "notes": None},
    # p.21
    {"alias": "ca", "canonical_name": "circa (approximately)", "entity_type": "condition_term", "source_page": 21, "notes": None},
    {"alias": "CA", "canonical_name": "chloroacetyl", "entity_type": "substituent", "source_page": 21, "notes": None},
    {"alias": "cat.", "canonical_name": "catalytic", "entity_type": "condition_term", "source_page": 21, "notes": None},
    {"alias": "CB", "canonical_name": "catecholborane", "entity_type": "reagent", "source_page": 21, "notes": None},
    {"alias": "cc. or conc.", "canonical_name": "concentrated", "entity_type": "condition_term", "source_page": 21, "notes": None},
    {"alias": "CCE", "canonical_name": "constant current electrolysis", "entity_type": "reaction_term", "source_page": 21, "notes": None},
    {"alias": "CHD", "canonical_name": "1,3 or 1,4-cyclohexadiene", "entity_type": "reagent", "source_page": 21, "notes": None},
    {"alias": "CHIRAPHOS", "canonical_name": "2,3-bis(diphenylphosphino)butane", "entity_type": "ligand", "source_page": 21, "notes": None},
    {"alias": "Chx", "canonical_name": "cyclohexyl", "entity_type": "substituent", "source_page": 21, "notes": "Shown as Chx (Cy)."},
    {"alias": "Cy", "canonical_name": "cyclohexyl", "entity_type": "substituent", "source_page": 21, "notes": "Shown as Chx (Cy)."},
    {"alias": "CIP", "canonical_name": "2-chloro-1,3-dimethylimidazolinium hexafluorophosphate", "entity_type": "reagent", "source_page": 21, "notes": None},
    {"alias": "CM", "canonical_name": "cross metathesis", "entity_type": "reaction_term", "source_page": 21, "notes": "Shown as CM (XMET)."},
    {"alias": "XMET", "canonical_name": "cross metathesis", "entity_type": "reaction_term", "source_page": 21, "notes": "Shown as CM (XMET)."},
    {"alias": "CMMP", "canonical_name": "cyanomethylenetrimethyl phosphorane", "entity_type": "reagent", "source_page": 21, "notes": None},
    {"alias": "COD", "canonical_name": "1,5-cyclooctadiene", "entity_type": "ligand", "source_page": 21, "notes": None},
    {"alias": "COT", "canonical_name": "1,3,5-cyclooctatriene", "entity_type": "ligand", "source_page": 21, "notes": None},
    {"alias": "Cp", "canonical_name": "cyclopentadienyl", "entity_type": "ligand", "source_page": 21, "notes": None},
    {"alias": "CPTS", "canonical_name": "collidinium p-toluenesulfonate", "entity_type": "additive", "source_page": 21, "notes": None},
    # p.22
    {"alias": "CRA", "canonical_name": "complex reducing agent", "entity_type": "reaction_term", "source_page": 22, "notes": None},
    {"alias": "Cr-PILC", "canonical_name": "chromium-pillared clay catalyst", "entity_type": "catalyst", "source_page": 22, "notes": None},
    {"alias": "CTAP", "canonical_name": "cetyl trimethylammonium permanganate", "entity_type": "additive", "source_page": 22, "notes": None},
    {"alias": "Δ", "canonical_name": "heat", "entity_type": "condition_term", "source_page": 22, "notes": None},
    {"alias": "d", "canonical_name": "days (length of reaction time)", "entity_type": "condition_term", "source_page": 22, "notes": None},
    {"alias": "DATMP", "canonical_name": "diethylaluminum 2,2,6,6-tetramethylpiperidide", "entity_type": "reagent", "source_page": 22, "notes": None},
    {"alias": "DBA", "canonical_name": "dibenzylideneacetone", "entity_type": "ligand", "source_page": 22, "notes": "Shown as DBA (dba)."},
    {"alias": "dba", "canonical_name": "dibenzylideneacetone", "entity_type": "ligand", "source_page": 22, "notes": "Shown as DBA (dba)."},
    {"alias": "DBAD", "canonical_name": "di-tert-butyl azodicarboxylate", "entity_type": "reagent", "source_page": 22, "notes": None},
    {"alias": "DBI", "canonical_name": "dibromoisocyanuric acid", "entity_type": "reagent", "source_page": 22, "notes": None},
    # p.23
    {"alias": "DBM", "canonical_name": "dibenzoylmethane", "entity_type": "reagent", "source_page": 23, "notes": None},
    {"alias": "DBS", "canonical_name": "dibenzosuberyl", "entity_type": "substituent", "source_page": 23, "notes": None},
    {"alias": "DCA", "canonical_name": "9,10-dicyanoanthracene", "entity_type": "reagent", "source_page": 23, "notes": None},
    {"alias": "DCB", "canonical_name": "1,2-dichlorobenzene", "entity_type": "solvent", "source_page": 23, "notes": None},
    {"alias": "DCE", "canonical_name": "1,1-dichloroethane", "entity_type": "solvent", "source_page": 23, "notes": None},
    {"alias": "DCN", "canonical_name": "1,4-dicyanonaphthalene", "entity_type": "reagent", "source_page": 23, "notes": None},
    {"alias": "Dcpm", "canonical_name": "dicyclopropylmethyl", "entity_type": "substituent", "source_page": 23, "notes": None},
    {"alias": "DCU", "canonical_name": "N,N'-dicyclohexylurea", "entity_type": "byproduct", "source_page": 23, "notes": None},
    {"alias": "de", "canonical_name": "diastereomeric excess", "entity_type": "condition_term", "source_page": 23, "notes": None},
]

MANUAL_FAMILY_SEEDS: List[Dict[str, Any]] = [
    {
        "family_name": "Suzuki Cross-Coupling",
        "family_class": "cross_coupling",
        "transformation_type": "organoboron + organohalide -> C-C bond formation",
        "mechanism_type": "Pd-catalyzed oxidative addition / transmetallation / reductive elimination",
        "reactant_pattern_text": "aryl/vinyl boronic acid or boronate + aryl/vinyl halide or pseudohalide",
        "product_pattern_text": "biaryl / styrenyl / C(sp2)-C(sp2) coupled product",
        "key_reagents_clue": "boronic acid or boronate; Pd catalyst; base",
        "common_solvents": "THF, dioxane, DMF, toluene, water mixtures",
        "common_conditions": "base present; catalytic Pd",
        "synonym_names": "Suzuki coupling|Suzuki-Miyaura coupling|Suzuki cross-coupling",
        "description_short": "Front-matter mechanism panel seed from page xv.",
        "seeded_from": "frontmatter_manual",
    },
    {
        "family_name": "Swern Oxidation",
        "family_class": "oxidation",
        "transformation_type": "alcohol -> aldehyde or ketone",
        "mechanism_type": "activated DMSO oxidation via alkoxysulfonium intermediate",
        "reactant_pattern_text": "primary/secondary alcohol",
        "product_pattern_text": "aldehyde or ketone",
        "key_reagents_clue": "DMSO; oxalyl chloride or TFAA; triethylamine; low temperature",
        "common_solvents": "DCM and low-temperature conditions are common",
        "common_conditions": "typically below 0 °C during activation",
        "synonym_names": "Swern oxidation",
        "description_short": "Front-matter mechanism panel seed from page xv.",
        "seeded_from": "frontmatter_manual",
    },
    {
        "family_name": "Dakin Oxidation",
        "family_class": "oxidation_rearrangement",
        "transformation_type": "o/p-hydroxyaryl aldehyde or ketone -> phenol derivative",
        "mechanism_type": "peroxide oxidation related to Baeyer-Villiger-type rearrangement logic",
        "reactant_pattern_text": "o- or p-hydroxyaryl aldehyde/ketone",
        "product_pattern_text": "phenol / hydroquinone-type product",
        "key_reagents_clue": "hydrogen peroxide or peroxide source; base or acid activation",
        "common_solvents": "aq-organic media common",
        "common_conditions": "oxidative conditions",
        "synonym_names": "Dakin oxidation",
        "description_short": "Reference-anchor page seed from page xvi.",
        "seeded_from": "frontmatter_manual",
    },
    {
        "family_name": "Simmons-Smith Reaction",
        "family_class": "cyclopropanation",
        "transformation_type": "alkene -> cyclopropane",
        "mechanism_type": "carbenoid cyclopropanation",
        "reactant_pattern_text": "alkene",
        "product_pattern_text": "cyclopropane",
        "key_reagents_clue": "CH2I2; Zn-Cu",
        "common_solvents": "ether solvents",
        "common_conditions": "Zn carbenoid generation",
        "synonym_names": "Simmons-Smith cyclopropanation|Simmons and Smith reaction",
        "description_short": "Color-explanation page family seed from page xiv.",
        "seeded_from": "frontmatter_manual",
    },
    {
        "family_name": "Furukawa Modification",
        "family_class": "cyclopropanation",
        "transformation_type": "alkene -> substituted cyclopropane",
        "mechanism_type": "Et2Zn-derived Simmons-Smith variant",
        "reactant_pattern_text": "alkene",
        "product_pattern_text": "cyclopropane",
        "key_reagents_clue": "Et2Zn; CH2I2 or diiodomethane source",
        "common_solvents": "non-coordinating solvents",
        "common_conditions": "zinc carbenoid conditions",
        "synonym_names": "Furukawa modification|Furukawa-Simmons-Smith variant",
        "description_short": "Color-explanation page family seed from page xiv.",
        "seeded_from": "frontmatter_manual",
    },
    {
        "family_name": "Charette Asymmetric Cyclopropanation",
        "family_class": "cyclopropanation",
        "transformation_type": "allylic alcohol/alkene -> optically active cyclopropane",
        "mechanism_type": "asymmetric zinc-carbenoid cyclopropanation",
        "reactant_pattern_text": "allylic alcohol or alkene partner with chiral dioxaborolane context",
        "product_pattern_text": "enantioenriched cyclopropane",
        "key_reagents_clue": "Et2Zn; CH2I2 derivative; dioxaborolane",
        "common_solvents": "DME/DCM",
        "common_conditions": "asymmetric cyclopropanation",
        "synonym_names": "Charette asymmetric modification|Charette asymmetric cyclopropanation",
        "description_short": "Color-explanation page family seed from page xiv.",
        "seeded_from": "frontmatter_manual",
    },
    {
        "family_name": "Retro-Claisen Reaction",
        "family_class": "cleavage",
        "transformation_type": "beta-keto ester / dicarbonyl cleavage",
        "mechanism_type": "base-induced retro-carbonyl condensation logic",
        "reactant_pattern_text": "beta-keto ester or related 1,3-dicarbonyl",
        "product_pattern_text": "carboxylate / ketone cleavage products",
        "key_reagents_clue": "aqueous base",
        "common_solvents": "aqueous conditions",
        "common_conditions": "base-induced cleavage",
        "synonym_names": "retro Claisen reaction|retro-Claisen reaction",
        "description_short": "Mentioned in color-explanation front matter on page xiv.",
        "seeded_from": "frontmatter_manual",
    },
]

# Existing family row to enrich rather than create duplicate.
MANUAL_FAMILY_UPDATES: List[Dict[str, Any]] = [
    {
        "family_name_norm": normalize_key("Aza-Cope Rearrangement"),
        "synonym_names": "Aza-Cope rearrangement|2-aza-Cope|2-aza-Cope rearrangement",
        "description_short": "Front-matter page xvi explicitly illustrates a [3,3]-2-aza-Cope rearrangement.",
    },
    {
        "family_name_norm": normalize_key("Acetoacetic Ester Synthesis"),
        "synonym_names": "Acetoacetic ester synthesis|acetoacetic ester alkylation",
        "description_short": "Front-matter page xiv defines ketone preparation via C-alkylation of acetoacetic esters.",
    },
]

DAKIN_REFERENCE_SEEDS: List[Dict[str, Any]] = [
    {
        "family_name": "Dakin Oxidation",
        "citation_text": "Hocking, M. B. Dakin oxidation of o-hydroxyacetophenone and some benzophenones. Rate enhancement and mechanistic aspects.",
        "citation_year": 1973,
        "citation_authors": "Hocking, M. B.",
        "source_doc": "named reactions front matter",
        "source_page": 16,
        "reference_group": "frontmatter_batch1",
        "notes": "Can. J. Chem. 1973, 51, 2384-2392.",
    },
    {
        "family_name": "Dakin Oxidation",
        "citation_text": "Matsumoto, M.; Kobayashi, K.; Hotta, Y. Acid-catalyzed oxidation of benzaldehydes to phenols by hydrogen peroxide.",
        "citation_year": 1984,
        "citation_authors": "Matsumoto, M.; Kobayashi, K.; Hotta, Y.",
        "source_doc": "named reactions front matter",
        "source_page": 16,
        "reference_group": "frontmatter_batch1",
        "notes": "J. Org. Chem. 1984, 49, 4740-4741.",
    },
    {
        "family_name": "Dakin Oxidation",
        "citation_text": "Ogata, Y.; Sawaki, Y. Kinetics of the Baeyer-Villiger reaction of benzaldehydes with perbenzoic acid in aq-organic solvents.",
        "citation_year": 1969,
        "citation_authors": "Ogata, Y.; Sawaki, Y.",
        "source_doc": "named reactions front matter",
        "source_page": 16,
        "reference_group": "frontmatter_batch1",
        "notes": "Mechanistic comparison cited near Dakin oxidation discussion.",
    },
    {
        "family_name": "Dakin Oxidation",
        "citation_text": "Boeseken, J.; Coden, W. D.; Kip, C. J. The synthesis of sesamol and of its β-glucoside.",
        "citation_year": 1936,
        "citation_authors": "Boeseken, J.; Coden, W. D.; Kip, C. J.",
        "source_doc": "named reactions front matter",
        "source_page": 16,
        "reference_group": "frontmatter_batch1",
        "notes": "Rec. trav. chim. 1936, 55, 815-820.",
    },
    {
        "family_name": "Dakin Oxidation",
        "citation_text": "Kabala, G. W.; Reddy, N. K.; Narayana, C. Sodium percarbonate: a convenient reagent for the Dakin reaction.",
        "citation_year": None,
        "citation_authors": "Kabala, G. W.; Reddy, N. K.; Narayana, C.",
        "source_doc": "named reactions front matter",
        "source_page": 16,
        "reference_group": "frontmatter_batch1",
        "notes": "Partially legible citation on page xvi; retained as reference seed.",
    },
    {
        "family_name": "Dakin Oxidation",
        "citation_text": "Hocking, M. B.; Ong, J. H. Kinetic studies of Dakin oxidation of o- and p-hydroxyacetophenones.",
        "citation_year": 1977,
        "citation_authors": "Hocking, M. B.; Ong, J. H.",
        "source_doc": "named reactions front matter",
        "source_page": 16,
        "reference_group": "frontmatter_batch1",
        "notes": "Can. J. Chem. 1977, 55, 102-110.",
    },
    {
        "family_name": "Dakin Oxidation",
        "citation_text": "Hocking, M. B.; Ko, M.; Smyth, T. A. Detection of intermediates and isolation of hydroquinone monoacetate in the Dakin oxidation of p-hydroxyacetophenone.",
        "citation_year": 1978,
        "citation_authors": "Hocking, M. B.; Ko, M.; Smyth, T. A.",
        "source_doc": "named reactions front matter",
        "source_page": 16,
        "reference_group": "frontmatter_batch1",
        "notes": "Can. J. Chem. 1978, 56, 2646-2649.",
    },
    {
        "family_name": "Dakin Oxidation",
        "citation_text": "Hocking, M. B.; Bhandari, K.; Shell, B.; Smyth, T. A. Steric and pH effects on the rate of Dakin oxidation of acylphenols.",
        "citation_year": 1982,
        "citation_authors": "Hocking, M. B.; Bhandari, K.; Shell, B.; Smyth, T. A.",
        "source_doc": "named reactions front matter",
        "source_page": 16,
        "reference_group": "frontmatter_batch1",
        "notes": "J. Org. Chem. 1982, 47, 4208-4215.",
    },
]

MANUAL_PAGE_ENTITY_SEEDS: List[Dict[str, Any]] = [
    # page xiv
    {"page_label": "xiv", "entity_text": "Acetoacetic Ester Synthesis", "canonical_name": "Acetoacetic Ester Synthesis", "entity_type": "reaction_family", "family_name": "Acetoacetic Ester Synthesis", "notes": "Named reaction family mentioned in introduction example.", "confidence": 0.98},
    {"page_label": "xiv", "entity_text": "retro-Claisen reaction", "canonical_name": "Retro-Claisen Reaction", "entity_type": "reaction_family", "family_name": "Retro-Claisen Reaction", "notes": "Mentioned as base-induced process.", "confidence": 0.95},
    {"page_label": "xiv", "entity_text": "Epothilone B", "canonical_name": "Epothilone B", "entity_type": "target_molecule", "family_name": None, "notes": "Synthetic application target molecule example.", "confidence": 0.95},
    {"page_label": "xiv", "entity_text": "Simmons & Smith", "canonical_name": "Simmons-Smith Reaction", "entity_type": "reaction_family", "family_name": "Simmons-Smith Reaction", "notes": "Cyclopropanation example.", "confidence": 0.98},
    {"page_label": "xiv", "entity_text": "Furukawa modification", "canonical_name": "Furukawa Modification", "entity_type": "reaction_family", "family_name": "Furukawa Modification", "notes": "Cyclopropanation variant.", "confidence": 0.97},
    {"page_label": "xiv", "entity_text": "Charette asymmetric modification", "canonical_name": "Charette Asymmetric Cyclopropanation", "entity_type": "reaction_family", "family_name": "Charette Asymmetric Cyclopropanation", "notes": "Asymmetric cyclopropanation example.", "confidence": 0.97},
    {"page_label": "xiv", "entity_text": "DCM", "canonical_name": "dichloromethane", "entity_type": "abbreviation", "family_name": None, "notes": "Non-coordinating solvent example in caption.", "confidence": 0.9},
    {"page_label": "xiv", "entity_text": "DCE", "canonical_name": "1,1-dichloroethane", "entity_type": "abbreviation", "family_name": None, "notes": "Non-coordinating solvent example in caption.", "confidence": 0.9},
    # page xv
    {"page_label": "xv", "entity_text": "Suzuki cross-coupling", "canonical_name": "Suzuki Cross-Coupling", "entity_type": "reaction_family", "family_name": "Suzuki Cross-Coupling", "notes": "Catalytic cycle shown.", "confidence": 0.99},
    {"page_label": "xv", "entity_text": "Swern oxidation", "canonical_name": "Swern Oxidation", "entity_type": "reaction_family", "family_name": "Swern Oxidation", "notes": "Detailed mechanism shown.", "confidence": 0.99},
    {"page_label": "xv", "entity_text": "DMSO", "canonical_name": "dimethyl sulfoxide", "entity_type": "reagent_name", "family_name": "Swern Oxidation", "notes": "Activated sulfoxide reagent in mechanism panel.", "confidence": 0.95},
    {"page_label": "xv", "entity_text": "TFAA", "canonical_name": "trifluoroacetic anhydride", "entity_type": "reagent_name", "family_name": "Swern Oxidation", "notes": "Activation reagent in mechanism panel.", "confidence": 0.9},
    {"page_label": "xv", "entity_text": "oxalyl chloride", "canonical_name": "oxalyl chloride", "entity_type": "reagent_name", "family_name": "Swern Oxidation", "notes": "Alternative activation reagent in mechanism panel.", "confidence": 0.95},
    {"page_label": "xv", "entity_text": "Et3N", "canonical_name": "triethylamine", "entity_type": "reagent_name", "family_name": "Swern Oxidation", "notes": "Base in final elimination step.", "confidence": 0.9},
    # page xvi
    {"page_label": "xvi", "entity_text": "2-aza-Cope", "canonical_name": "Aza-Cope Rearrangement", "entity_type": "reaction_family", "family_name": "Aza-Cope Rearrangement", "notes": "[3,3]-sigmatropic rearrangement example.", "confidence": 0.98},
    {"page_label": "xvi", "entity_text": "Dakin oxidation", "canonical_name": "Dakin Oxidation", "entity_type": "reaction_family", "family_name": "Dakin Oxidation", "notes": "Reference group shown on page.", "confidence": 0.99},
    {"page_label": "xvi", "entity_text": "Baeyer-Villiger oxidation", "canonical_name": "Baeyer-Villiger Oxidation", "entity_type": "reaction_family", "family_name": "Baeyer-Villiger Oxidation", "notes": "Mechanistically related family explicitly mentioned.", "confidence": 0.96},
]

# Populate glossary-page entities from abbreviation seeds automatically.
for seed in CURATED_FRONTMATTER_ABBREVIATIONS:
    page_label = {17: "xvii", 18: "xviii", 19: "xix", 20: "xx", 21: "xxi", 22: "xxii", 23: "xxiii"}.get(seed["source_page"])
    if page_label:
        MANUAL_PAGE_ENTITY_SEEDS.append(
            {
                "page_label": page_label,
                "entity_text": seed["alias"],
                "canonical_name": seed["canonical_name"],
                "entity_type": "abbreviation",
                "family_name": None,
                "notes": seed.get("notes"),
                "confidence": 0.94,
            }
        )


def _insert_page_knowledge(con: sqlite3.Connection) -> int:
    inserted = 0
    for seed in PAGE_KNOWLEDGE_SEEDS:
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
                seed["source_label"], seed["page_label"], seed["page_no"], seed["title"], seed["section_name"], seed["page_kind"],
                seed["summary"], seed["family_names"], seed["reference_family_name"], seed["notes"], seed["image_filename"],
            ),
        )
        inserted += 1
    return inserted


def _seed_abbreviations(con: sqlite3.Connection) -> int:
    if not _table_exists(con, "abbreviation_aliases"):
        return 0
    inserted = 0
    for seed in CURATED_FRONTMATTER_ABBREVIATIONS:
        alias_norm = normalize_key(seed["alias"])
        canon_norm = normalize_key(seed["canonical_name"])
        con.execute(
            """
            INSERT OR IGNORE INTO abbreviation_aliases (
              alias, alias_norm, canonical_name, canonical_name_norm, entity_type,
              smiles, molblock, notes, source_label, source_page, confidence, updated_at
            ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, 'front_matter_seed_batch1', ?, 0.94, datetime('now'))
            """,
            (seed["alias"], alias_norm, seed["canonical_name"], canon_norm, seed["entity_type"], seed.get("notes"), seed["source_page"]),
        )
        inserted += 1
    return inserted


def _seed_manual_family_patterns(con: sqlite3.Connection) -> int:
    if not _table_exists(con, "reaction_family_patterns"):
        return 0
    changes = 0
    for seed in MANUAL_FAMILY_SEEDS:
        family_name_norm = normalize_key(seed["family_name"])
        con.execute(
            """
            INSERT INTO reaction_family_patterns (
              family_name, family_name_norm, family_class, transformation_type, mechanism_type,
              reactant_pattern_text, product_pattern_text, key_reagents_clue,
              common_solvents, common_conditions, synonym_names, description_short,
              evidence_extract_count, overview_count, application_count, mechanism_count,
              latest_source_zip, latest_updated_at, seeded_from, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 'named_reactions_frontmatter_batch1', datetime('now'), ?, datetime('now'))
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
              synonym_names=COALESCE(reaction_family_patterns.synonym_names, excluded.synonym_names),
              description_short=COALESCE(reaction_family_patterns.description_short, excluded.description_short),
              latest_source_zip='named_reactions_frontmatter_batch1',
              latest_updated_at=datetime('now'),
              updated_at=datetime('now')
            """,
            (
                seed["family_name"], family_name_norm, seed["family_class"], seed["transformation_type"], seed["mechanism_type"],
                seed["reactant_pattern_text"], seed["product_pattern_text"], seed["key_reagents_clue"], seed["common_solvents"], seed["common_conditions"],
                seed["synonym_names"], seed["description_short"], seed["seeded_from"],
            ),
        )
        changes += 1
    for upd in MANUAL_FAMILY_UPDATES:
        con.execute(
            """
            UPDATE reaction_family_patterns
            SET synonym_names = CASE
                WHEN synonym_names IS NULL OR trim(synonym_names) = '' THEN ?
                WHEN instr(lower(synonym_names), lower(?)) > 0 THEN synonym_names
                ELSE synonym_names || '|' || ?
              END,
              description_short = COALESCE(description_short, ?),
              latest_source_zip = COALESCE(latest_source_zip, 'named_reactions_frontmatter_batch1'),
              latest_updated_at = datetime('now'),
              updated_at = datetime('now')
            WHERE family_name_norm = ?
            """,
            (upd["synonym_names"], upd["synonym_names"], upd["synonym_names"], upd["description_short"], upd["family_name_norm"]),
        )
        changes += 1
    return changes


def _seed_family_references(con: sqlite3.Connection) -> int:
    if not _table_exists(con, "family_references") or not _table_exists(con, "reaction_family_patterns"):
        return 0
    inserted = 0
    for seed in DAKIN_REFERENCE_SEEDS:
        family_name_norm = normalize_key(seed["family_name"])
        row = con.execute("SELECT id FROM reaction_family_patterns WHERE family_name_norm = ?", (family_name_norm,)).fetchone()
        family_pattern_id = row[0] if row else None
        con.execute(
            """
            INSERT OR IGNORE INTO family_references (
              family_pattern_id, family_name, citation_text, citation_year, citation_authors,
              source_doc, source_page, reference_group, notes, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                family_pattern_id, seed["family_name"], seed["citation_text"], seed["citation_year"], seed["citation_authors"],
                seed["source_doc"], seed["source_page"], seed["reference_group"], seed["notes"],
            ),
        )
        inserted += 1
    return inserted


def _seed_page_entities(con: sqlite3.Connection) -> int:
    if not _table_exists(con, "manual_page_entities") or not _table_exists(con, "manual_page_knowledge"):
        return 0
    page_lookup = {
        row[0]: row[1]
        for row in con.execute(
            "SELECT page_label, id FROM manual_page_knowledge WHERE source_label = 'named_reactions_frontmatter_batch1'"
        ).fetchall()
    }
    alias_lookup = {}
    if _table_exists(con, "abbreviation_aliases"):
        alias_lookup = {
            row[0]: row[1]
            for row in con.execute("SELECT alias_norm, id FROM abbreviation_aliases").fetchall()
        }
    inserted = 0
    for seed in MANUAL_PAGE_ENTITY_SEEDS:
        page_id = page_lookup.get(seed["page_label"])
        if not page_id:
            continue
        entity_text_norm = normalize_key(seed["entity_text"])
        alias_id = alias_lookup.get(entity_text_norm) if seed["entity_type"] == "abbreviation" else None
        con.execute(
            """
            INSERT OR REPLACE INTO manual_page_entities (
              page_knowledge_id, entity_text, entity_text_norm, canonical_name,
              entity_type, alias_id, family_name, notes, confidence, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                page_id, seed["entity_text"], entity_text_norm, seed.get("canonical_name"), seed["entity_type"], alias_id,
                seed.get("family_name"), seed.get("notes"), seed.get("confidence", 0.9),
            ),
        )
        inserted += 1
    return inserted


def apply_frontmatter_batch(db_path: str | Path) -> Dict[str, Any]:
    db_path = Path(db_path)
    ensure_frontmatter_schema(db_path)
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("PRAGMA foreign_keys=ON")
        page_rows = _insert_page_knowledge(con)
        alias_rows = _seed_abbreviations(con)
        family_rows = _seed_manual_family_patterns(con)
        ref_rows = _seed_family_references(con)
        entity_rows = _seed_page_entities(con)
        if _table_exists(con, "labint_schema_meta"):
            con.execute(
                """
                INSERT INTO labint_schema_meta(key, value) VALUES('frontmatter_batch1_last_applied_at', datetime('now'))
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')
                """
            )
        con.commit()
        out = get_frontmatter_counts(db_path, con)
        out.update(
            {
                "page_seed_rows": page_rows,
                "alias_seed_rows": alias_rows,
                "manual_family_seed_ops": family_rows,
                "family_reference_seed_rows": ref_rows,
                "page_entity_seed_rows": entity_rows,
            }
        )
        return out
    finally:
        con.close()


def get_frontmatter_counts(db_path: str | Path, con: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
    close_after = False
    if con is None:
        con = sqlite3.connect(str(db_path))
        close_after = True
    try:
        def q(sql: str) -> int:
            return int(con.execute(sql).fetchone()[0])
        meta = {}
        if _table_exists(con, "labint_schema_meta"):
            meta = {row[0]: row[1] for row in con.execute("SELECT key, value FROM labint_schema_meta WHERE key LIKE 'frontmatter_%'")}
        return {
            "frontmatter_schema_version": meta.get("frontmatter_schema_version"),
            "manual_page_knowledge": q("SELECT COUNT(*) FROM manual_page_knowledge") if _table_exists(con, "manual_page_knowledge") else 0,
            "manual_page_entities": q("SELECT COUNT(*) FROM manual_page_entities") if _table_exists(con, "manual_page_entities") else 0,
            "frontmatter_batch1_pages": q("SELECT COUNT(*) FROM manual_page_knowledge WHERE source_label = 'named_reactions_frontmatter_batch1'") if _table_exists(con, "manual_page_knowledge") else 0,
            "frontmatter_batch1_abbreviation_aliases": q("SELECT COUNT(*) FROM abbreviation_aliases WHERE source_label = 'front_matter_seed_batch1'") if _table_exists(con, "abbreviation_aliases") else 0,
            "frontmatter_batch1_family_refs": q("SELECT COUNT(*) FROM family_references WHERE reference_group = 'frontmatter_batch1'") if _table_exists(con, "family_references") else 0,
            "manual_frontmatter_families": q("SELECT COUNT(*) FROM reaction_family_patterns WHERE latest_source_zip = 'named_reactions_frontmatter_batch1'") if _table_exists(con, "reaction_family_patterns") else 0,
        }
    finally:
        if close_after:
            con.close()


def export_frontmatter_seed_templates(output_dir: str | Path) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    with (out / "frontmatter_batch1_manual_pages.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["source_label", "page_label", "page_no", "title", "section_name", "page_kind", "summary", "family_names", "reference_family_name", "notes", "image_filename"],
        )
        writer.writeheader()
        for row in PAGE_KNOWLEDGE_SEEDS:
            writer.writerow(row)
    with (out / "frontmatter_batch1_abbreviation_seed.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["alias", "canonical_name", "entity_type", "source_page", "notes"],
        )
        writer.writeheader()
        for row in CURATED_FRONTMATTER_ABBREVIATIONS:
            writer.writerow(row)
    with (out / "frontmatter_batch1_family_references.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["family_name", "citation_text", "citation_year", "citation_authors", "source_doc", "source_page", "reference_group", "notes"],
        )
        writer.writeheader()
        for row in DAKIN_REFERENCE_SEEDS:
            writer.writerow(row)
