from __future__ import annotations

import csv
import re
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    from rdkit import Chem  # type: ignore
except Exception:  # pragma: no cover
    Chem = None  # type: ignore

SCHEMA_VERSION = "labint_intel_v1_20260411"

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS labint_schema_meta (
  key TEXT PRIMARY KEY,
  value TEXT,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS reaction_family_patterns (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  family_name TEXT NOT NULL,
  family_name_norm TEXT NOT NULL UNIQUE,
  family_class TEXT,
  transformation_type TEXT,
  mechanism_type TEXT,
  reactant_pattern_text TEXT,
  product_pattern_text TEXT,
  key_reagents_clue TEXT,
  common_solvents TEXT,
  common_conditions TEXT,
  synonym_names TEXT,
  description_short TEXT,
  evidence_extract_count INTEGER NOT NULL DEFAULT 0,
  overview_count INTEGER NOT NULL DEFAULT 0,
  application_count INTEGER NOT NULL DEFAULT 0,
  mechanism_count INTEGER NOT NULL DEFAULT 0,
  latest_source_zip TEXT,
  latest_updated_at TEXT,
  seeded_from TEXT NOT NULL DEFAULT 'reaction_extracts',
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_reaction_family_patterns_class ON reaction_family_patterns(family_class);
CREATE INDEX IF NOT EXISTS idx_reaction_family_patterns_transform ON reaction_family_patterns(transformation_type);

CREATE TABLE IF NOT EXISTS abbreviation_aliases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alias TEXT NOT NULL,
  alias_norm TEXT NOT NULL,
  canonical_name TEXT,
  canonical_name_norm TEXT,
  entity_type TEXT NOT NULL DEFAULT 'chemical_term',
  smiles TEXT,
  molblock TEXT,
  notes TEXT,
  source_label TEXT,
  source_page INTEGER,
  confidence REAL NOT NULL DEFAULT 0.5,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(alias_norm, canonical_name_norm, entity_type)
);
CREATE INDEX IF NOT EXISTS idx_abbreviation_aliases_alias_norm ON abbreviation_aliases(alias_norm);
CREATE INDEX IF NOT EXISTS idx_abbreviation_aliases_entity_type ON abbreviation_aliases(entity_type);

CREATE TABLE IF NOT EXISTS extract_entities (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  extract_id INTEGER NOT NULL,
  source_field TEXT NOT NULL,
  entity_text TEXT NOT NULL,
  entity_text_norm TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  role TEXT,
  alias_id INTEGER,
  smiles TEXT,
  molblock TEXT,
  parse_mode TEXT NOT NULL DEFAULT 'raw_segment',
  confidence REAL NOT NULL DEFAULT 0.5,
  queryable INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (extract_id) REFERENCES reaction_extracts(id) ON DELETE CASCADE,
  FOREIGN KEY (alias_id) REFERENCES abbreviation_aliases(id) ON DELETE SET NULL,
  UNIQUE(extract_id, source_field, entity_text_norm, entity_type)
);
CREATE INDEX IF NOT EXISTS idx_extract_entities_extract ON extract_entities(extract_id, source_field);
CREATE INDEX IF NOT EXISTS idx_extract_entities_norm ON extract_entities(entity_text_norm, entity_type);
CREATE INDEX IF NOT EXISTS idx_extract_entities_queryable ON extract_entities(queryable, entity_type, role);

CREATE TABLE IF NOT EXISTS family_references (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  family_pattern_id INTEGER,
  family_name TEXT NOT NULL,
  citation_text TEXT NOT NULL,
  citation_year INTEGER,
  citation_authors TEXT,
  source_doc TEXT,
  source_page INTEGER,
  reference_group TEXT,
  notes TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (family_pattern_id) REFERENCES reaction_family_patterns(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_family_references_family ON family_references(family_name);
"""

VIEW_SQL = """
CREATE VIEW IF NOT EXISTS v_named_reaction_evidence AS
SELECT
  re.id AS extract_id,
  re.reaction_family_name,
  re.reaction_family_name_norm,
  re.extract_kind,
  re.transformation_text,
  re.reactants_text,
  re.products_text,
  re.intermediates_text,
  re.reagents_text,
  re.catalysts_text,
  re.solvents_text,
  re.temperature_text,
  re.time_text,
  re.yield_text,
  re.conditions_text,
  re.notes_text,
  sc.id AS scheme_candidate_id,
  sc.section_type,
  sc.scheme_role,
  sc.crop_path,
  sc.caption_text,
  sc.vision_summary,
  pi.id AS page_image_id,
  pi.source_zip,
  pi.source_doc,
  pi.page_no,
  pi.image_filename,
  rfp.family_class,
  rfp.transformation_type,
  rfp.mechanism_type,
  rfp.key_reagents_clue,
  rfp.common_solvents,
  rfp.common_conditions,
  rfp.description_short
FROM reaction_extracts re
JOIN scheme_candidates sc ON re.scheme_candidate_id = sc.id
JOIN page_images pi ON sc.page_image_id = pi.id
LEFT JOIN reaction_family_patterns rfp ON rfp.family_name_norm = re.reaction_family_name_norm;

CREATE VIEW IF NOT EXISTS v_extract_entity_context AS
SELECT
  ee.id,
  ee.extract_id,
  ee.source_field,
  ee.entity_text,
  ee.entity_text_norm,
  ee.entity_type,
  ee.role,
  ee.alias_id,
  ee.smiles,
  ee.parse_mode,
  ee.confidence,
  ee.queryable,
  aa.alias,
  aa.canonical_name,
  re.reaction_family_name,
  re.extract_kind,
  pi.source_zip,
  pi.page_no
FROM extract_entities ee
LEFT JOIN abbreviation_aliases aa ON aa.id = ee.alias_id
JOIN reaction_extracts re ON ee.extract_id = re.id
JOIN scheme_candidates sc ON re.scheme_candidate_id = sc.id
JOIN page_images pi ON sc.page_image_id = pi.id;
"""

EXTRACT_MOLECULE_COLUMNS = [
    ("normalized_text", "TEXT"),
    ("source_field", "TEXT"),
    ("structure_source", "TEXT"),
    ("alias_id", "INTEGER"),
    ("fg_tags", "TEXT"),
    ("role_confidence", "REAL"),
]

CURATED_ABBREVIATIONS: Sequence[Dict[str, Any]] = [
    {"alias": "18-Cr-6", "canonical_name": "18-crown-6", "entity_type": "reagent", "notes": "crown ether phase-transfer / coordination additive", "source_label": "front_matter_seed", "source_page": 17, "confidence": 0.95},
    {"alias": "AIBN", "canonical_name": "2,2'-azoisobutyronitrile", "entity_type": "reagent", "notes": "radical initiator", "source_label": "front_matter_seed", "source_page": 17, "confidence": 0.95},
    {"alias": "AQN", "canonical_name": "anthraquinone", "entity_type": "reagent", "source_label": "front_matter_seed", "source_page": 17, "confidence": 0.9},
    {"alias": "9-BBN", "canonical_name": "9-borabicyclo[3.3.1]nonane", "entity_type": "reagent", "source_label": "front_matter_seed", "source_page": 18, "confidence": 0.95},
    {"alias": "BINAP", "canonical_name": "2,2'-bis(diphenylphosphino)-1,1'-binaphthyl", "entity_type": "ligand", "source_label": "front_matter_seed", "source_page": 18, "confidence": 0.95},
    {"alias": "BINAL-H", "canonical_name": "2,2'-dihydroxy-1,1'-binaphthyl lithium aluminum hydride", "entity_type": "reagent", "source_label": "front_matter_seed", "source_page": 18, "confidence": 0.9},
    {"alias": "BINOL", "canonical_name": "1,1'-bi-2,2'-naphthol", "entity_type": "ligand", "source_label": "front_matter_seed", "source_page": 19, "confidence": 0.95},
    {"alias": "BMS", "canonical_name": "borane-dimethyl sulfide complex", "entity_type": "reagent", "source_label": "front_matter_seed", "source_page": 19, "confidence": 0.95},
    {"alias": "Boc", "canonical_name": "tert-butoxycarbonyl", "entity_type": "protecting_group", "source_label": "front_matter_seed", "source_page": 19, "confidence": 0.95},
    {"alias": "BOP-Cl", "canonical_name": "bis(2-oxo-3-oxazolidinyl)phosphinic chloride", "entity_type": "reagent", "source_label": "front_matter_seed", "source_page": 19, "confidence": 0.9},
    {"alias": "BPO", "canonical_name": "benzoyl peroxide", "entity_type": "reagent", "source_label": "front_matter_seed", "source_page": 19, "confidence": 0.95},
    {"alias": "BSA", "canonical_name": "N,O-bis(trimethylsilyl)acetamide", "entity_type": "reagent", "notes": "Not bovine serum albumin in reaction context", "source_label": "front_matter_seed", "source_page": 20, "confidence": 0.95},
    {"alias": "CAN", "canonical_name": "cerium(IV) ammonium nitrate", "entity_type": "reagent", "source_label": "front_matter_seed", "source_page": 21, "confidence": 0.95},
    {"alias": "CBS", "canonical_name": "Corey-Bakshi-Shibata reagent", "entity_type": "reagent", "source_label": "front_matter_seed", "source_page": 21, "confidence": 0.95},
    {"alias": "Cbz", "canonical_name": "benzyloxycarbonyl", "entity_type": "protecting_group", "source_label": "front_matter_seed", "source_page": 21, "confidence": 0.95},
    {"alias": "CDI", "canonical_name": "carbonyldiimidazole", "entity_type": "reagent", "source_label": "front_matter_seed", "source_page": 21, "confidence": 0.95},
    {"alias": "CSA", "canonical_name": "camphorsulfonic acid", "entity_type": "reagent", "source_label": "front_matter_seed", "source_page": 22, "confidence": 0.95},
    {"alias": "CSI", "canonical_name": "chlorosulfonyl isocyanate", "entity_type": "reagent", "source_label": "front_matter_seed", "source_page": 22, "confidence": 0.95},
    {"alias": "CTAB", "canonical_name": "cetyltrimethylammonium bromide", "entity_type": "additive", "source_label": "front_matter_seed", "source_page": 22, "confidence": 0.95},
    {"alias": "CTACl", "canonical_name": "cetyl trimethylammonium chloride", "entity_type": "additive", "source_label": "front_matter_seed", "source_page": 22, "confidence": 0.95},
    {"alias": "DABCO", "canonical_name": "1,4-diazabicyclo[2.2.2]octane", "entity_type": "base", "source_label": "front_matter_seed", "source_page": 22, "confidence": 0.95},
    {"alias": "DAST", "canonical_name": "diethylaminosulfur trifluoride", "entity_type": "reagent", "source_label": "front_matter_seed", "source_page": 22, "confidence": 0.95},
    {"alias": "DBN", "canonical_name": "1,5-diazabicyclo[4.3.0]non-5-ene", "entity_type": "base", "source_label": "front_matter_seed", "source_page": 23, "confidence": 0.95},
    {"alias": "DBU", "canonical_name": "1,8-diazabicyclo[5.4.0]undec-7-ene", "entity_type": "base", "source_label": "front_matter_seed", "source_page": 23, "confidence": 0.95},
    {"alias": "DCC", "canonical_name": "dicyclohexylcarbodiimide", "entity_type": "reagent", "source_label": "front_matter_seed", "source_page": 23, "confidence": 0.95},
    {"alias": "DCM", "canonical_name": "dichloromethane", "entity_type": "solvent", "source_label": "front_matter_seed", "source_page": 23, "confidence": 0.95},
    {"alias": "DCE", "canonical_name": "1,1-dichloroethane", "entity_type": "solvent", "source_label": "front_matter_seed", "source_page": 23, "confidence": 0.9},
    {"alias": "DDQ", "canonical_name": "2,3-dichloro-5,6-dicyano-1,4-benzoquinone", "entity_type": "oxidant", "source_label": "front_matter_seed", "source_page": 23, "confidence": 0.95},
    {"alias": "DCA", "canonical_name": "9,10-dicyanoanthracene", "entity_type": "reagent", "source_label": "front_matter_seed", "source_page": 23, "confidence": 0.9},
]

ENTITY_FIELD_SPECS: Sequence[Tuple[str, str, Optional[str]]] = [
    ("transformation_text", "transformation", None),
    ("reactants_text", "reactant", "reactant"),
    ("products_text", "product", "product"),
    ("intermediates_text", "intermediate", "intermediate"),
    ("reagents_text", "reagent", "reagent"),
    ("catalysts_text", "catalyst", "catalyst"),
    ("solvents_text", "solvent", "solvent"),
    ("temperature_text", "condition", None),
    ("time_text", "condition", None),
    ("yield_text", "condition", None),
    ("workup_text", "workup", None),
    ("conditions_text", "condition", None),
    ("notes_text", "note", None),
]


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return bool(row)


def _column_names(con: sqlite3.Connection, table: str) -> set[str]:
    return {str(r[1]) for r in con.execute(f"PRAGMA table_info({table})").fetchall()}


def _norm_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip()).strip()


def normalize_name_key(text: str) -> str:
    text = _norm_spaces(text).lower()
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"[^a-z0-9]+", "", text)
    return text


def normalize_text_key(text: str) -> str:
    text = _norm_spaces(text).lower()
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"\s+", " ", text)
    return text.strip(" ;,.:/\\")




def looks_like_smiles(text: str) -> bool:
    s = (text or "").strip()
    if not s or " " in s or len(s) < 2 or len(s) > 240:
        return False
    if re.search(r"[가-힣α-ωΑ-Ω]", s):
        return False
    if not re.fullmatch(r"[A-Za-z0-9@+\-\[\]\(\)=#$\/.%]+", s):
        return False
    if re.fullmatch(r"[A-Z]{2,6}", s):
        return False
    if re.search(r"[a-z]{4,}", s) and not re.search(r"[\[\]=#@\/()]", s):
        return False
    return True

def normalize_smiles(smiles: str) -> str:
    smi = (smiles or "").strip()
    if not smi or not looks_like_smiles(smi):
        return ""
    if Chem is None:
        return ""
    try:
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            return ""
        return Chem.MolToSmiles(mol, canonical=True)
    except Exception:
        return ""


def classify_family(name: str) -> Tuple[str, str, str]:
    n = (name or "").lower()
    if any(k in n for k in ["oxidation", "ozonolysis"]):
        return ("oxidation", "oxidation", "redox")
    if "reduction" in n or "deoxygenation" in n:
        return ("reduction", "reduction", "redox")
    if "rearrangement" in n or "cope" in n or "claisen" in n or "benzilic" in n:
        return ("rearrangement", "rearrangement", "sigmatropic_or_rearrangement")
    if any(k in n for k in ["coupling", "cross-coupling", "metathesis"]):
        t = "metathesis" if "metathesis" in n else "coupling"
        return (t, t, "bond_construction")
    if any(k in n for k in ["condensation", "aldol", "benzoin", "acyloin"]):
        return ("condensation", "condensation", "carbon-carbon_bond_formation")
    if any(k in n for k in ["elimination", "dehydration", "olefination"]):
        return ("elimination", "elimination", "bond_reorganization")
    if any(k in n for k in ["amination", "alkylation", "acylation", "homologation", "addition", "annulation"]):
        return ("functionalization", "functionalization", "named_transformation")
    if "synthesis" in n or "cyclization" in n:
        return ("synthesis", "synthesis", "named_transformation")
    return ("other", "other", "named_transformation")


def ensure_labint_intel_schema(db_path: str | Path) -> None:
    db_path = Path(db_path)
    if not db_path.exists():
        db_path.touch()
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(SCHEMA_SQL)
        _ensure_extract_molecules_schema(con)
        con.executescript(VIEW_SQL)
        con.execute(
            """
            INSERT INTO labint_schema_meta(key, value) VALUES('schema_version', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')
            """,
            (SCHEMA_VERSION,),
        )
        con.commit()
    finally:
        con.close()


def _ensure_extract_molecules_schema(con: sqlite3.Connection) -> None:
    if not _table_exists(con, "extract_molecules"):
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS extract_molecules (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              extract_id INTEGER NOT NULL,
              role TEXT NOT NULL,
              smiles TEXT,
              smiles_kind TEXT,
              quality_tier INTEGER NOT NULL DEFAULT 3,
              reaction_family_name TEXT,
              source_zip TEXT,
              page_no INTEGER,
              queryable INTEGER NOT NULL DEFAULT 0,
              note_text TEXT,
              morgan_fp BLOB,
              normalized_text TEXT,
              source_field TEXT,
              structure_source TEXT,
              alias_id INTEGER,
              fg_tags TEXT,
              role_confidence REAL,
              created_at TEXT DEFAULT CURRENT_TIMESTAMP,
              FOREIGN KEY (extract_id) REFERENCES reaction_extracts(id) ON DELETE CASCADE,
              FOREIGN KEY (alias_id) REFERENCES abbreviation_aliases(id) ON DELETE SET NULL
            )
            """
        )
    else:
        cols = _column_names(con, "extract_molecules")
        for name, coltype in EXTRACT_MOLECULE_COLUMNS:
            if name not in cols:
                con.execute(f"ALTER TABLE extract_molecules ADD COLUMN {name} {coltype}")
    con.execute("CREATE INDEX IF NOT EXISTS idx_extract_molecules_extract_id ON extract_molecules(extract_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_extract_molecules_tier_queryable ON extract_molecules(quality_tier, queryable)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_extract_molecules_family ON extract_molecules(reaction_family_name)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_extract_molecules_page ON extract_molecules(source_zip, page_no)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_extract_molecules_alias ON extract_molecules(alias_id)")


def seed_abbreviation_aliases(con: sqlite3.Connection) -> int:
    inserted = 0
    for row in CURATED_ABBREVIATIONS:
        alias = row["alias"].strip()
        canonical_name = (row.get("canonical_name") or "").strip() or None
        alias_norm = normalize_name_key(alias)
        canonical_name_norm = normalize_name_key(canonical_name or "") or None
        before = con.total_changes
        con.execute(
            """
            INSERT INTO abbreviation_aliases (
              alias, alias_norm, canonical_name, canonical_name_norm, entity_type,
              smiles, molblock, notes, source_label, source_page, confidence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(alias_norm, canonical_name_norm, entity_type) DO UPDATE SET
              canonical_name = COALESCE(abbreviation_aliases.canonical_name, excluded.canonical_name),
              notes = COALESCE(abbreviation_aliases.notes, excluded.notes),
              source_label = COALESCE(abbreviation_aliases.source_label, excluded.source_label),
              source_page = COALESCE(abbreviation_aliases.source_page, excluded.source_page),
              confidence = MAX(COALESCE(abbreviation_aliases.confidence, 0), excluded.confidence),
              updated_at = datetime('now')
            """,
            (
                alias,
                alias_norm,
                canonical_name,
                canonical_name_norm,
                row.get("entity_type") or "chemical_term",
                row.get("smiles"),
                row.get("molblock"),
                row.get("notes"),
                row.get("source_label"),
                row.get("source_page"),
                row.get("confidence", 0.5),
            ),
        )
        if con.total_changes > before:
            inserted += 1
    return inserted


def _top_non_empty(values: Iterable[str], limit: int = 3) -> str:
    cleaned = []
    seen = set()
    for value in values:
        v = _norm_spaces(value or "")
        if not v or v in seen:
            continue
        seen.add(v)
        cleaned.append(v)
        if len(cleaned) >= limit:
            break
    return " | ".join(cleaned)


def backfill_family_patterns(con: sqlite3.Connection) -> int:
    if not _table_exists(con, "reaction_extracts"):
        return 0
    rows = con.execute(
        """
        SELECT
          reaction_family_name,
          reaction_family_name_norm,
          extract_kind,
          transformation_text,
          reactants_text,
          products_text,
          reagents_text,
          catalysts_text,
          solvents_text,
          temperature_text,
          time_text,
          pi.source_zip,
          re.updated_at
        FROM reaction_extracts re
        LEFT JOIN scheme_candidates sc ON re.scheme_candidate_id = sc.id
        LEFT JOIN page_images pi ON sc.page_image_id = pi.id
        WHERE COALESCE(re.reaction_family_name_norm, '') <> ''
        ORDER BY re.id
        """
    ).fetchall()
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        fam = row[0]
        fam_norm = row[1] or normalize_text_key(fam or "")
        slot = grouped.setdefault(
            fam_norm,
            {
                "family_name": fam,
                "family_name_norm": fam_norm,
                "extracts": 0,
                "overview": 0,
                "application": 0,
                "mechanism": 0,
                "descriptions": [],
                "reactants": [],
                "products": [],
                "reagents": [],
                "solvents": [],
                "conditions": [],
                "source_zips": [],
                "updated": [],
            },
        )
        slot["extracts"] += 1
        kind = (row[2] or "").strip().lower()
        if kind == "canonical_overview":
            slot["overview"] += 1
        elif kind == "application_example":
            slot["application"] += 1
        elif kind == "mechanism_step":
            slot["mechanism"] += 1
        for key, idx in [
            ("descriptions", 3),
            ("reactants", 4),
            ("products", 5),
            ("reagents", 6),
            ("solvents", 8),
        ]:
            if row[idx]:
                slot[key].append(row[idx])
        condition_blob = " | ".join([_norm_spaces(row[9] or ""), _norm_spaces(row[10] or "")]).strip(" |")
        if condition_blob:
            slot["conditions"].append(condition_blob)
        if row[11]:
            slot["source_zips"].append(row[11])
        if row[12]:
            slot["updated"].append(row[12])

    upserts = 0
    for fam_norm, data in grouped.items():
        family_class, transformation_type, mechanism_type = classify_family(data["family_name"])
        before = con.total_changes
        con.execute(
            """
            INSERT INTO reaction_family_patterns (
              family_name, family_name_norm, family_class, transformation_type, mechanism_type,
              reactant_pattern_text, product_pattern_text, key_reagents_clue,
              common_solvents, common_conditions, synonym_names, description_short,
              evidence_extract_count, overview_count, application_count, mechanism_count,
              latest_source_zip, latest_updated_at, seeded_from
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'reaction_extracts')
            ON CONFLICT(family_name_norm) DO UPDATE SET
              family_name = excluded.family_name,
              family_class = excluded.family_class,
              transformation_type = excluded.transformation_type,
              mechanism_type = excluded.mechanism_type,
              reactant_pattern_text = COALESCE(NULLIF(excluded.reactant_pattern_text, ''), reaction_family_patterns.reactant_pattern_text),
              product_pattern_text = COALESCE(NULLIF(excluded.product_pattern_text, ''), reaction_family_patterns.product_pattern_text),
              key_reagents_clue = COALESCE(NULLIF(excluded.key_reagents_clue, ''), reaction_family_patterns.key_reagents_clue),
              common_solvents = COALESCE(NULLIF(excluded.common_solvents, ''), reaction_family_patterns.common_solvents),
              common_conditions = COALESCE(NULLIF(excluded.common_conditions, ''), reaction_family_patterns.common_conditions),
              description_short = COALESCE(NULLIF(excluded.description_short, ''), reaction_family_patterns.description_short),
              evidence_extract_count = excluded.evidence_extract_count,
              overview_count = excluded.overview_count,
              application_count = excluded.application_count,
              mechanism_count = excluded.mechanism_count,
              latest_source_zip = COALESCE(excluded.latest_source_zip, reaction_family_patterns.latest_source_zip),
              latest_updated_at = COALESCE(excluded.latest_updated_at, reaction_family_patterns.latest_updated_at),
              updated_at = datetime('now')
            """,
            (
                data["family_name"],
                fam_norm,
                family_class,
                transformation_type,
                mechanism_type,
                _top_non_empty(data["reactants"], limit=2),
                _top_non_empty(data["products"], limit=2),
                _top_non_empty(data["reagents"], limit=3),
                _top_non_empty(data["solvents"], limit=3),
                _top_non_empty(data["conditions"], limit=3),
                None,
                _top_non_empty(data["descriptions"], limit=1),
                data["extracts"],
                data["overview"],
                data["application"],
                data["mechanism"],
                (data["source_zips"][-1] if data["source_zips"] else None),
                (data["updated"][-1] if data["updated"] else None),
            ),
        )
        if con.total_changes > before:
            upserts += 1
    return upserts


def _split_segments(text: str, source_field: str) -> List[str]:
    raw = _norm_spaces(text)
    if not raw:
        return []
    splitter = r"[\n;|]+"
    if source_field in {"reagents_text", "catalysts_text", "solvents_text"}:
        splitter = r"[\n;|,/]+"
    parts = [p.strip() for p in re.split(splitter, raw) if p.strip()]
    cleaned: List[str] = []
    seen = set()
    for part in parts[:30]:
        part = re.sub(r"^\d+\.?\s*", "", part).strip()
        part = part.strip("-–—•· ")
        if len(part) < 2:
            continue
        if part not in seen:
            seen.add(part)
            cleaned.append(part)
    if not cleaned:
        return [raw]
    return cleaned


def _entity_queryable(entity_type: str, alias_id: Optional[int], smiles: str) -> int:
    if smiles:
        return 1
    if alias_id:
        return 1
    return 1 if entity_type in {"reactant", "product", "intermediate", "transformation"} else 0


def _alias_lookup(con: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    rows = con.execute(
        "SELECT id, alias, alias_norm, canonical_name, canonical_name_norm, entity_type, smiles FROM abbreviation_aliases"
    ).fetchall()
    for row in rows:
        payload = {
            "id": row[0],
            "alias": row[1],
            "canonical_name": row[3],
            "entity_type": row[5],
            "smiles": row[6],
        }
        out[row[2]] = payload
        if row[4]:
            out.setdefault(row[4], payload)
    return out


def _backfill_extract_entities(con: sqlite3.Connection) -> int:
    if not _table_exists(con, "reaction_extracts"):
        return 0
    con.execute("DELETE FROM extract_entities WHERE parse_mode <> 'manual'")
    alias_lookup = _alias_lookup(con)
    rows = con.execute(
        "SELECT id, transformation_text, reactants_text, products_text, intermediates_text, reagents_text, catalysts_text, solvents_text, temperature_text, time_text, yield_text, workup_text, conditions_text, notes_text FROM reaction_extracts ORDER BY id"
    ).fetchall()
    inserted = 0
    for row in rows:
        extract_id = int(row[0])
        row_map = {
            "transformation_text": row[1],
            "reactants_text": row[2],
            "products_text": row[3],
            "intermediates_text": row[4],
            "reagents_text": row[5],
            "catalysts_text": row[6],
            "solvents_text": row[7],
            "temperature_text": row[8],
            "time_text": row[9],
            "yield_text": row[10],
            "workup_text": row[11],
            "conditions_text": row[12],
            "notes_text": row[13],
        }
        for source_field, entity_type, role in ENTITY_FIELD_SPECS:
            text = row_map.get(source_field) or ""
            if not _norm_spaces(text):
                continue
            for segment in _split_segments(text, source_field):
                text_norm = normalize_text_key(segment)
                name_key = normalize_name_key(segment)
                alias = alias_lookup.get(name_key) or alias_lookup.get(text_norm)
                smiles = normalize_smiles(segment)
                if not smiles and alias and alias.get("smiles"):
                    smiles = normalize_smiles(alias.get("smiles") or "")
                parse_mode = "smiles" if smiles else ("alias_match" if alias else "raw_segment")
                confidence = 0.95 if smiles else (0.86 if alias else 0.55)
                queryable = _entity_queryable(entity_type, alias.get("id") if alias else None, smiles)
                con.execute(
                    """
                    INSERT OR REPLACE INTO extract_entities (
                      extract_id, source_field, entity_text, entity_text_norm, entity_type, role,
                      alias_id, smiles, molblock, parse_mode, confidence, queryable, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, datetime('now'))
                    """,
                    (
                        extract_id,
                        source_field,
                        segment,
                        text_norm,
                        entity_type,
                        role,
                        alias.get("id") if alias else None,
                        smiles or None,
                        parse_mode,
                        confidence,
                        queryable,
                    ),
                )
                inserted += 1
    return inserted


def _backfill_extract_molecule_metadata(con: sqlite3.Connection) -> int:
    if not _table_exists(con, "extract_molecules"):
        return 0
    rows = con.execute(
        "SELECT id, role, note_text, quality_tier, normalized_text, source_field, structure_source, alias_id, role_confidence FROM extract_molecules"
    ).fetchall()
    alias_lookup = _alias_lookup(con)
    updates = 0
    for row in rows:
        rid, role, note_text, quality_tier, normalized_text, source_field, structure_source, alias_id, role_confidence = row
        norm = normalized_text or normalize_text_key(note_text or "") or None
        inferred_source_field = source_field or (
            "reactants_text" if role == "reactant" else "products_text" if role == "product" else "reagents_text"
        )
        struct_src = structure_source or (
            "text_smiles" if int(quality_tier or 3) == 1 else "text_smarts" if int(quality_tier or 3) == 2 else "text_only"
        )
        alias_hit = alias_id
        if not alias_hit and note_text:
            alias = alias_lookup.get(normalize_name_key(note_text)) or alias_lookup.get(normalize_text_key(note_text))
            alias_hit = alias.get("id") if alias else None
        rc = role_confidence if role_confidence is not None else (0.92 if role in {"reactant", "product"} else 0.75)
        con.execute(
            """
            UPDATE extract_molecules
            SET normalized_text = COALESCE(normalized_text, ?),
                source_field = COALESCE(source_field, ?),
                structure_source = COALESCE(structure_source, ?),
                alias_id = COALESCE(alias_id, ?),
                role_confidence = COALESCE(role_confidence, ?)
            WHERE id = ?
            """,
            (norm, inferred_source_field, struct_src, alias_hit, rc, rid),
        )
        updates += 1
    return updates


def backfill_labint_intel(db_path: str | Path) -> Dict[str, Any]:
    db_path = Path(db_path)
    ensure_labint_intel_schema(db_path)
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("PRAGMA foreign_keys=ON")
        seeded_aliases = seed_abbreviation_aliases(con)
        family_upserts = backfill_family_patterns(con)
        entity_rows = _backfill_extract_entities(con)
        molecule_updates = _backfill_extract_molecule_metadata(con)
        con.execute(
            """
            INSERT INTO labint_schema_meta(key, value) VALUES('last_backfill_at', datetime('now'))
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')
            """
        )
        con.commit()
        summary = get_labint_intel_counts(db_path, con=con)
        summary.update(
            {
                "seeded_aliases": seeded_aliases,
                "family_upserts": family_upserts,
                "entity_rows_refreshed": entity_rows,
                "extract_molecule_metadata_updates": molecule_updates,
            }
        )
        return summary
    finally:
        con.close()


def get_labint_intel_counts(db_path: str | Path, con: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
    close_after = False
    if con is None:
        con = sqlite3.connect(str(db_path))
        close_after = True
    try:
        def q(sql: str) -> int:
            return int(con.execute(sql).fetchone()[0])

        meta = {}
        if _table_exists(con, "labint_schema_meta"):
            meta = {row[0]: row[1] for row in con.execute("SELECT key, value FROM labint_schema_meta")}
        return {
            "schema_version": meta.get("schema_version"),
            "reaction_family_patterns": q("SELECT COUNT(*) FROM reaction_family_patterns") if _table_exists(con, "reaction_family_patterns") else 0,
            "abbreviation_aliases": q("SELECT COUNT(*) FROM abbreviation_aliases") if _table_exists(con, "abbreviation_aliases") else 0,
            "extract_entities": q("SELECT COUNT(*) FROM extract_entities") if _table_exists(con, "extract_entities") else 0,
            "family_references": q("SELECT COUNT(*) FROM family_references") if _table_exists(con, "family_references") else 0,
            "extract_molecules": q("SELECT COUNT(*) FROM extract_molecules") if _table_exists(con, "extract_molecules") else 0,
            "queryable_extract_entities": q("SELECT COUNT(*) FROM extract_entities WHERE queryable = 1") if _table_exists(con, "extract_entities") else 0,
            "front_matter_seed_aliases": q("SELECT COUNT(*) FROM abbreviation_aliases WHERE source_label = 'front_matter_seed'") if _table_exists(con, "abbreviation_aliases") else 0,
            "families_with_examples": q("SELECT COUNT(*) FROM reaction_family_patterns WHERE application_count > 0") if _table_exists(con, "reaction_family_patterns") else 0,
            "families_with_mechanism": q("SELECT COUNT(*) FROM reaction_family_patterns WHERE mechanism_count > 0") if _table_exists(con, "reaction_family_patterns") else 0,
        }
    finally:
        if close_after:
            con.close()


def export_seed_templates(output_dir: str | Path) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    with (out / "abbreviation_aliases_seed_template.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["alias", "canonical_name", "entity_type", "smiles", "notes", "source_label", "source_page", "confidence"],
        )
        writer.writeheader()
        writer.writerow({"alias": "DBU", "canonical_name": "1,8-diazabicyclo[5.4.0]undec-7-ene", "entity_type": "base", "source_label": "manual_seed", "source_page": 23, "confidence": 0.95})
    with (out / "family_references_seed_template.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["family_name", "citation_text", "citation_year", "citation_authors", "source_doc", "source_page", "reference_group", "notes"],
        )
        writer.writeheader()
        writer.writerow({"family_name": "Dakin oxidation", "citation_text": "Hooking, M. B. Dakin oxidation of o-hydroxyacetophenone and some benzophenones.", "citation_year": 1973, "source_doc": "named reactions front matter", "source_page": 16})
