from __future__ import annotations

import hashlib
import os
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from rdkit import Chem  # type: ignore
    from rdkit.Chem import Descriptors, rdMolDescriptors  # type: ignore
except Exception:
    Chem = None  # type: ignore
    Descriptors = None  # type: ignore
    rdMolDescriptors = None  # type: ignore

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS ingest_jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_type TEXT NOT NULL,
  source_label TEXT,
  source_path TEXT,
  status TEXT NOT NULL DEFAULT 'queued',
  stats_json TEXT,
  notes TEXT,
  started_at TEXT NOT NULL DEFAULT (datetime('now')),
  finished_at TEXT
);

CREATE TABLE IF NOT EXISTS substances (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  canonical_smiles TEXT NOT NULL UNIQUE,
  raw_smiles TEXT,
  standard_inchikey TEXT,
  formula TEXT,
  molecular_weight REAL,
  exact_mass REAL,
  atom_count INTEGER,
  heavy_atom_count INTEGER,
  ring_count INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS substances_inchikey_idx ON substances(standard_inchikey);

CREATE TABLE IF NOT EXISTS substance_names (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  substance_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  name_type TEXT NOT NULL DEFAULT 'label',
  source TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (substance_id) REFERENCES substances(id) ON DELETE CASCADE,
  UNIQUE(substance_id, name, name_type)
);
CREATE INDEX IF NOT EXISTS substance_names_name_idx ON substance_names(name);

CREATE TABLE IF NOT EXISTS reactions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  reaction_hash TEXT NOT NULL UNIQUE,
  title TEXT,
  normalized_title TEXT,
  transformation TEXT,
  reaction_smiles TEXT,
  record_type TEXT NOT NULL DEFAULT 'text_fragment',
  is_structure_indexable INTEGER NOT NULL DEFAULT 0,
  quality_score REAL NOT NULL DEFAULT 0,
  source_summary TEXT,
  staging_card_id INTEGER UNIQUE,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS reactions_record_type_idx ON reactions(record_type, is_structure_indexable);
CREATE INDEX IF NOT EXISTS reactions_title_idx ON reactions(normalized_title);

CREATE TABLE IF NOT EXISTS reaction_participants (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  reaction_id INTEGER NOT NULL,
  substance_id INTEGER,
  role TEXT NOT NULL,
  display_smiles TEXT,
  source_text TEXT,
  order_no INTEGER NOT NULL DEFAULT 0,
  stoich REAL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (reaction_id) REFERENCES reactions(id) ON DELETE CASCADE,
  FOREIGN KEY (substance_id) REFERENCES substances(id) ON DELETE SET NULL,
  UNIQUE(reaction_id, role, order_no)
);
CREATE INDEX IF NOT EXISTS reaction_participants_rxn_role_idx ON reaction_participants(reaction_id, role);
CREATE INDEX IF NOT EXISTS reaction_participants_substance_idx ON reaction_participants(substance_id, role);

CREATE TABLE IF NOT EXISTS reaction_conditions (
  reaction_id INTEGER PRIMARY KEY,
  reagents TEXT,
  solvent TEXT,
  conditions TEXT,
  yield_pct REAL,
  notes TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (reaction_id) REFERENCES reactions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS reaction_occurrences (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  reaction_id INTEGER NOT NULL,
  source_label TEXT,
  source_ref TEXT,
  source_document TEXT,
  source_page INTEGER,
  asset_path TEXT,
  evidence_text TEXT,
  confidence REAL,
  extraction_method TEXT NOT NULL DEFAULT 'staging_card',
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (reaction_id) REFERENCES reactions(id) ON DELETE CASCADE,
  UNIQUE(reaction_id, source_ref, source_page, extraction_method)
);
CREATE INDEX IF NOT EXISTS reaction_occurrences_source_idx ON reaction_occurrences(source_document, source_page);

CREATE TABLE IF NOT EXISTS card_migration_map (
  reaction_card_id INTEGER PRIMARY KEY,
  reaction_id INTEGER NOT NULL,
  migration_version TEXT NOT NULL DEFAULT 'labint_v2_20260405',
  migrated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (reaction_id) REFERENCES reactions(id) ON DELETE CASCADE
);
"""


def _normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).strip()


def normalize_smiles(smiles: str) -> str:
    smi = (smiles or "").strip()
    if not smi:
        return ""
    if Chem is None:
        return smi
    try:
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            return ""
        return Chem.MolToSmiles(mol, canonical=True)
    except Exception:
        return ""


def smiles_metadata(smiles: str) -> Dict[str, Any]:
    smi = normalize_smiles(smiles)
    if not smi:
        return {}
    out: Dict[str, Any] = {"canonical_smiles": smi, "raw_smiles": (smiles or "").strip()}
    if Chem is None:
        return out
    try:
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            return out
        out["formula"] = rdMolDescriptors.CalcMolFormula(mol) if rdMolDescriptors is not None else None
        out["molecular_weight"] = float(Descriptors.MolWt(mol)) if Descriptors is not None else None
        out["exact_mass"] = float(rdMolDescriptors.CalcExactMolWt(mol)) if rdMolDescriptors is not None else None
        out["atom_count"] = int(mol.GetNumAtoms())
        out["heavy_atom_count"] = int(mol.GetNumHeavyAtoms())
        out["ring_count"] = int(rdMolDescriptors.CalcNumRings(mol)) if rdMolDescriptors is not None else None
        try:
            out["standard_inchikey"] = Chem.MolToInchiKey(mol)
        except Exception:
            out["standard_inchikey"] = None
    except Exception:
        pass
    return out


def _parse_source(source: str) -> Tuple[str, Optional[int], str]:
    s = (source or "").strip()
    if not s:
        return "", None, ""
    m = re.search(r"^(.*?)(?:#p(\d+))?$", s)
    if not m:
        return s, None, s
    doc = (m.group(1) or "").strip()
    page = int(m.group(2)) if m.group(2) else None
    return doc, page, s


def _classify_record(card: sqlite3.Row) -> Tuple[str, int, float]:
    sub = (card["substrate_smiles"] or "").strip()
    prod = (card["product_smiles"] or "").strip()
    title = _normalize_text(card["title"] or "")
    transformation = _normalize_text(card["transformation"] or "")
    notes = _normalize_text(card["notes"] or "")
    has_struct = bool(normalize_smiles(sub) or normalize_smiles(prod))
    if has_struct:
        return "reaction_or_molecule", 1, 1.0
    text_blob = f"{title} {transformation} {notes}".lower()
    if any(k in text_blob for k in ["reaction", "synthesis", "preparation", "합성", "반응", "제조"]):
        return "reaction_text", 0, 0.45
    if title or notes:
        return "text_fragment", 0, 0.15
    return "empty", 0, 0.0


def _reaction_hash(card: sqlite3.Row, sub_smi: str, prod_smi: str) -> str:
    material = "||".join([
        sub_smi,
        prod_smi,
        _normalize_text(card["title"] or ""),
        _normalize_text(card["transformation"] or ""),
        _normalize_text(card["reagents"] or ""),
        _normalize_text(card["solvent"] or ""),
        _normalize_text(card["conditions"] or ""),
        _normalize_text(card["source"] or ""),
        _normalize_text(card["notes"] or ""),
    ])
    return hashlib.sha256(material.encode("utf-8", errors="ignore")).hexdigest()


def ensure_labint_v2_schema(db_path: str | Path) -> None:
    db_path = Path(db_path)
    if not db_path.exists():
        db_path.touch()
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(SCHEMA_SQL)
        con.commit()
    finally:
        con.close()


def _upsert_substance(con: sqlite3.Connection, smiles: str, label: str = "", source: str = "") -> Optional[int]:
    meta = smiles_metadata(smiles)
    smi = meta.get("canonical_smiles") or ""
    if not smi:
        return None
    row = con.execute("SELECT id FROM substances WHERE canonical_smiles = ?", (smi,)).fetchone()
    if row:
        substance_id = int(row[0])
        con.execute(
            """
            UPDATE substances
            SET raw_smiles = COALESCE(raw_smiles, ?),
                standard_inchikey = COALESCE(standard_inchikey, ?),
                formula = COALESCE(formula, ?),
                molecular_weight = COALESCE(molecular_weight, ?),
                exact_mass = COALESCE(exact_mass, ?),
                atom_count = COALESCE(atom_count, ?),
                heavy_atom_count = COALESCE(heavy_atom_count, ?),
                ring_count = COALESCE(ring_count, ?),
                updated_at = datetime('now')
            WHERE id = ?
            """,
            (
                meta.get("raw_smiles"),
                meta.get("standard_inchikey"),
                meta.get("formula"),
                meta.get("molecular_weight"),
                meta.get("exact_mass"),
                meta.get("atom_count"),
                meta.get("heavy_atom_count"),
                meta.get("ring_count"),
                substance_id,
            ),
        )
    else:
        cur = con.execute(
            """
            INSERT INTO substances (
              canonical_smiles, raw_smiles, standard_inchikey, formula,
              molecular_weight, exact_mass, atom_count, heavy_atom_count, ring_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                smi,
                meta.get("raw_smiles"),
                meta.get("standard_inchikey"),
                meta.get("formula"),
                meta.get("molecular_weight"),
                meta.get("exact_mass"),
                meta.get("atom_count"),
                meta.get("heavy_atom_count"),
                meta.get("ring_count"),
            ),
        )
        substance_id = int(cur.lastrowid)
    if label:
        try:
            con.execute(
                "INSERT OR IGNORE INTO substance_names (substance_id, name, name_type, source) VALUES (?, ?, 'label', ?)",
                (substance_id, label[:500], source or None),
            )
        except Exception:
            pass
    return substance_id


def sync_reaction_card_to_v2(db_path: str | Path, card_id: int) -> Dict[str, Any]:
    ensure_labint_v2_schema(db_path)
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        row = con.execute("SELECT * FROM reaction_cards WHERE id = ?", (card_id,)).fetchone()
        if not row:
            return {"ok": False, "reason": "card_not_found", "card_id": card_id}

        mapped = con.execute("SELECT reaction_id FROM card_migration_map WHERE reaction_card_id = ?", (card_id,)).fetchone()
        if mapped:
            return {"ok": True, "card_id": card_id, "reaction_id": int(mapped[0]), "status": "already_migrated"}

        sub_smi = normalize_smiles(row["substrate_smiles"] or "")
        prod_smi = normalize_smiles(row["product_smiles"] or "")
        record_type, is_structure_indexable, quality_score = _classify_record(row)
        rxn_hash = _reaction_hash(row, sub_smi, prod_smi)
        title = _normalize_text(row["title"] or "")
        normalized_title = title.lower()[:500] if title else None
        source_doc, source_page, source_ref = _parse_source(row["source"] or "")
        source_page0 = source_page - 1 if source_page and source_page > 0 else source_page
        reaction_smiles = f"{sub_smi}>>{prod_smi}" if (sub_smi or prod_smi) else None

        existing = con.execute("SELECT id FROM reactions WHERE reaction_hash = ? LIMIT 1", (rxn_hash,)).fetchone()
        if existing:
            reaction_id = int(existing[0])
            con.execute(
                """
                UPDATE reactions
                SET title = COALESCE(title, ?),
                    normalized_title = COALESCE(normalized_title, ?),
                    transformation = COALESCE(transformation, ?),
                    reaction_smiles = COALESCE(reaction_smiles, ?),
                    record_type = CASE WHEN record_type = 'text_fragment' AND ? <> 'text_fragment' THEN ? ELSE record_type END,
                    is_structure_indexable = CASE WHEN is_structure_indexable = 0 AND ? = 1 THEN 1 ELSE is_structure_indexable END,
                    quality_score = MAX(COALESCE(quality_score, 0), ?),
                    source_summary = COALESCE(source_summary, ?),
                    updated_at = datetime('now')
                WHERE id = ?
                """,
                (
                    row["title"],
                    normalized_title,
                    row["transformation"],
                    reaction_smiles,
                    record_type,
                    record_type,
                    is_structure_indexable,
                    quality_score,
                    row["source"],
                    reaction_id,
                ),
            )
        else:
            cur = con.execute(
                """
                INSERT INTO reactions (
                  reaction_hash, title, normalized_title, transformation, reaction_smiles,
                  record_type, is_structure_indexable, quality_score, source_summary, staging_card_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rxn_hash,
                    row["title"],
                    normalized_title,
                    row["transformation"],
                    reaction_smiles,
                    record_type,
                    is_structure_indexable,
                    quality_score,
                    row["source"],
                    card_id,
                ),
            )
            reaction_id = int(cur.lastrowid)

        order_no = 0
        if sub_smi:
            sub_id = _upsert_substance(con, sub_smi, label=title or "substrate", source=row["source"] or "")
            con.execute(
                "INSERT OR IGNORE INTO reaction_participants (reaction_id, substance_id, role, display_smiles, source_text, order_no) VALUES (?, ?, 'reactant', ?, ?, ?)",
                (reaction_id, sub_id, sub_smi, row["transformation"], order_no),
            )
            order_no += 1
        if prod_smi:
            prod_id = _upsert_substance(con, prod_smi, label=title or "product", source=row["source"] or "")
            con.execute(
                "INSERT OR IGNORE INTO reaction_participants (reaction_id, substance_id, role, display_smiles, source_text, order_no) VALUES (?, ?, 'product', ?, ?, ?)",
                (reaction_id, prod_id, prod_smi, row["transformation"], order_no),
            )

        con.execute(
            """
            INSERT INTO reaction_conditions (reaction_id, reagents, solvent, conditions, yield_pct, notes)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(reaction_id) DO UPDATE SET
              reagents = COALESCE(reaction_conditions.reagents, excluded.reagents),
              solvent = COALESCE(reaction_conditions.solvent, excluded.solvent),
              conditions = COALESCE(reaction_conditions.conditions, excluded.conditions),
              yield_pct = COALESCE(reaction_conditions.yield_pct, excluded.yield_pct),
              notes = COALESCE(reaction_conditions.notes, excluded.notes),
              updated_at = datetime('now')
            """,
            (
                reaction_id,
                row["reagents"],
                row["solvent"],
                row["conditions"],
                row["yield_pct"],
                row["notes"],
            ),
        )
        con.execute(
            """
            INSERT OR IGNORE INTO reaction_occurrences (
              reaction_id, source_label, source_ref, source_document, source_page,
              asset_path, evidence_text, confidence, extraction_method
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'staging_card')
            """,
            (
                reaction_id,
                row["source"],
                source_ref or row["source"],
                source_doc or None,
                source_page0,
                None,
                row["notes"],
                quality_score,
            ),
        )
        con.execute(
            "INSERT OR REPLACE INTO card_migration_map (reaction_card_id, reaction_id) VALUES (?, ?)",
            (card_id, reaction_id),
        )
        con.commit()
        return {
            "ok": True,
            "status": "migrated",
            "card_id": card_id,
            "reaction_id": reaction_id,
            "record_type": record_type,
            "is_structure_indexable": is_structure_indexable,
        }
    finally:
        con.close()


def migrate_reaction_cards_to_v2(db_path: str | Path, limit: int = 0, since_id: int = 0) -> Dict[str, Any]:
    ensure_labint_v2_schema(db_path)
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            """
            SELECT rc.id
            FROM reaction_cards rc
            LEFT JOIN card_migration_map m ON m.reaction_card_id = rc.id
            WHERE m.reaction_card_id IS NULL AND rc.id > ?
            ORDER BY rc.id ASC
            """,
            (since_id,),
        ).fetchall()
    finally:
        con.close()
    ids = [int(r[0]) for r in rows]
    if limit:
        ids = ids[:limit]
    migrated = 0
    failed = 0
    for card_id in ids:
        try:
            res = sync_reaction_card_to_v2(db_path, card_id)
            if res.get("ok"):
                migrated += 1
            else:
                failed += 1
        except Exception:
            failed += 1
    return {"queued": len(ids), "migrated": migrated, "failed": failed, "limit": limit, "since_id": since_id}


def get_labint_v2_counts(db_path: str | Path) -> Dict[str, Any]:
    ensure_labint_v2_schema(db_path)
    con = sqlite3.connect(str(db_path))
    try:
        def q(sql: str) -> int:
            return int(con.execute(sql).fetchone()[0])
        return {
            "reaction_cards": q("SELECT COUNT(*) FROM reaction_cards") if _table_exists(con, "reaction_cards") else 0,
            "substances": q("SELECT COUNT(*) FROM substances"),
            "substance_names": q("SELECT COUNT(*) FROM substance_names"),
            "reactions": q("SELECT COUNT(*) FROM reactions"),
            "structure_indexable_reactions": q("SELECT COUNT(*) FROM reactions WHERE is_structure_indexable = 1"),
            "occurrences": q("SELECT COUNT(*) FROM reaction_occurrences"),
            "migrated_cards": q("SELECT COUNT(*) FROM card_migration_map"),
            "unmigrated_cards": q("SELECT COUNT(*) FROM reaction_cards rc LEFT JOIN card_migration_map m ON m.reaction_card_id = rc.id WHERE m.reaction_card_id IS NULL") if _table_exists(con, "reaction_cards") else 0,
        }
    finally:
        con.close()


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return bool(row)


def list_v2_substances(db_path: str | Path, query: str = "", limit: int = 20) -> List[Dict[str, Any]]:
    ensure_labint_v2_schema(db_path)
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        q = (query or "").strip()
        if q:
            smi = normalize_smiles(q)
            if smi:
                rows = con.execute(
                    "SELECT * FROM substances WHERE canonical_smiles = ? ORDER BY id DESC LIMIT ?",
                    (smi, limit),
                ).fetchall()
            else:
                like = f"%{q.lower()}%"
                rows = con.execute(
                    """
                    SELECT DISTINCT s.*
                    FROM substances s
                    LEFT JOIN substance_names sn ON sn.substance_id = s.id
                    WHERE lower(COALESCE(sn.name,'')) LIKE ? OR lower(COALESCE(s.canonical_smiles,'')) LIKE ?
                    ORDER BY s.id DESC
                    LIMIT ?
                    """,
                    (like, like, limit),
                ).fetchall()
        else:
            rows = con.execute("SELECT * FROM substances ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def list_v2_reactions(db_path: str | Path, query: str = "", structure_only: bool = False, limit: int = 50) -> List[Dict[str, Any]]:
    ensure_labint_v2_schema(db_path)
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        where = []
        params: List[Any] = []
        q = (query or "").strip()
        if q:
            like = f"%{q.lower()}%"
            where.append("(lower(COALESCE(r.title,'')) LIKE ? OR lower(COALESCE(r.transformation,'')) LIKE ? OR lower(COALESCE(rc.evidence_text,'')) LIKE ?)")
            params.extend([like, like, like])
        if structure_only:
            where.append("r.is_structure_indexable = 1")
        sql = """
        SELECT r.*, c.yield_pct, rc.source_ref, rc.source_document, rc.source_page, c.reagents, c.solvent, c.conditions
        FROM reactions r
        LEFT JOIN reaction_occurrences rc ON rc.reaction_id = r.id
        LEFT JOIN reaction_conditions c ON c.reaction_id = r.id
        """
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY r.id DESC LIMIT ?"
        params.append(limit)
        rows = con.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()
