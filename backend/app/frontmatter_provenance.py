from __future__ import annotations

import sqlite3
from typing import Any, Dict, Optional, Tuple


def ensure_abbreviation_provenance_schema(cur: sqlite3.Cursor) -> None:
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS abbreviation_alias_provenance (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          alias_id INTEGER NOT NULL,
          source_label TEXT NOT NULL,
          source_page INTEGER,
          notes TEXT,
          confidence REAL NOT NULL DEFAULT 0.7,
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at TEXT NOT NULL DEFAULT (datetime('now')),
          FOREIGN KEY (alias_id) REFERENCES abbreviation_aliases(id) ON DELETE CASCADE,
          UNIQUE(alias_id, source_label, source_page)
        );
        CREATE INDEX IF NOT EXISTS idx_abbreviation_alias_provenance_alias ON abbreviation_alias_provenance(alias_id);
        CREATE INDEX IF NOT EXISTS idx_abbreviation_alias_provenance_source ON abbreviation_alias_provenance(source_label, source_page);
        CREATE VIEW IF NOT EXISTS v_abbreviation_alias_provenance AS
        SELECT
          aap.alias_id,
          aa.alias,
          aa.canonical_name,
          aa.entity_type,
          aap.source_label,
          aap.source_page,
          aap.notes,
          aap.confidence,
          aap.created_at,
          aap.updated_at
        FROM abbreviation_alias_provenance aap
        JOIN abbreviation_aliases aa ON aa.id = aap.alias_id;
        """
    )


def get_source_page_range(cur: sqlite3.Cursor, source_label: Optional[str], cache: Optional[Dict[str, Optional[Tuple[int, int]]]] = None) -> Optional[Tuple[int, int]]:
    if not source_label:
        return None
    if cache is not None and source_label in cache:
        return cache[source_label]
    row = cur.execute(
        "SELECT MIN(page_no), MAX(page_no) FROM manual_page_knowledge WHERE source_label=? AND page_no IS NOT NULL",
        (source_label,),
    ).fetchone()
    value: Optional[Tuple[int, int]]
    if row and row[0] is not None and row[1] is not None:
        value = (int(row[0]), int(row[1]))
    else:
        value = None
    if cache is not None:
        cache[source_label] = value
    return value


def is_valid_source_page(cur: sqlite3.Cursor, source_label: Optional[str], source_page: Optional[int], cache: Optional[Dict[str, Optional[Tuple[int, int]]]] = None) -> bool:
    if source_page is None:
        return True
    rng = get_source_page_range(cur, source_label, cache=cache)
    if rng is None:
        return True
    return int(rng[0]) <= int(source_page) <= int(rng[1])


def merge_text(primary: Any, fallback: Any) -> Any:
    if primary is None:
        return fallback
    if isinstance(primary, str) and not primary.strip():
        return fallback
    return primary


def upsert_alias_provenance(
    cur: sqlite3.Cursor,
    *,
    alias_id: int,
    source_label: Optional[str],
    source_page: Optional[int] = None,
    notes: Optional[str] = None,
    confidence: Optional[float] = None,
) -> int:
    if not source_label:
        return 0
    ensure_abbreviation_provenance_schema(cur)
    existing = cur.execute(
        "SELECT id, notes, confidence FROM abbreviation_alias_provenance WHERE alias_id=? AND source_label=? AND source_page IS ?",
        (alias_id, source_label, source_page),
    ).fetchone()
    conf = float(confidence if confidence is not None else 0.9)
    if existing:
        prov_id, old_notes, old_conf = existing
        cur.execute(
            "UPDATE abbreviation_alias_provenance SET notes=?, confidence=?, updated_at=datetime('now') WHERE id=?",
            (merge_text(old_notes, notes), max(float(old_conf or 0.0), conf), prov_id),
        )
        return 0
    cur.execute(
        """
        INSERT INTO abbreviation_alias_provenance(alias_id, source_label, source_page, notes, confidence, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        """,
        (alias_id, source_label, source_page, notes, conf),
    )
    return 1


def backfill_provenance_from_alias_rows(cur: sqlite3.Cursor) -> int:
    ensure_abbreviation_provenance_schema(cur)
    rows = cur.execute(
        "SELECT id, source_label, source_page, notes, confidence FROM abbreviation_aliases WHERE source_label IS NOT NULL AND trim(source_label) <> ''"
    ).fetchall()
    inserted = 0
    for alias_id, source_label, source_page, notes, confidence in rows:
        inserted += upsert_alias_provenance(
            cur,
            alias_id=int(alias_id),
            source_label=source_label,
            source_page=source_page,
            notes=notes,
            confidence=confidence,
        )
    return inserted


def sanitize_provenance_pages(cur: sqlite3.Cursor) -> int:
    ensure_abbreviation_provenance_schema(cur)
    cache: Dict[str, Optional[Tuple[int, int]]] = {}
    rows = cur.execute("SELECT id, source_label, source_page FROM abbreviation_alias_provenance WHERE source_page IS NOT NULL").fetchall()
    changed = 0
    for prov_id, source_label, source_page in rows:
        if not is_valid_source_page(cur, source_label, source_page, cache=cache):
            cur.execute(
                "UPDATE abbreviation_alias_provenance SET source_page=NULL, updated_at=datetime('now') WHERE id=?",
                (prov_id,),
            )
            changed += 1
    return changed


def _provenance_rank(cur: sqlite3.Cursor, source_label: Optional[str], source_page: Optional[int], cache: Dict[str, Optional[Tuple[int, int]]]) -> Tuple[int, int, int, int]:
    label = source_label or ''
    is_named_batch = 1 if label.startswith('named_reactions_frontmatter_batch') else 0
    is_front_seed = 1 if label == 'front_matter_seed' else 0
    has_page = 1 if source_page is not None else 0
    is_valid_page = 1 if is_valid_source_page(cur, source_label, source_page, cache=cache) else 0
    return (is_valid_page, is_named_batch, has_page, is_front_seed)


def sync_alias_representative_provenance(cur: sqlite3.Cursor) -> int:
    ensure_abbreviation_provenance_schema(cur)
    cache: Dict[str, Optional[Tuple[int, int]]] = {}
    rows = cur.execute(
        "SELECT id FROM abbreviation_aliases ORDER BY id"
    ).fetchall()
    changed = 0
    for (alias_id,) in rows:
        prov_rows = cur.execute(
            "SELECT source_label, source_page, notes, confidence, id FROM abbreviation_alias_provenance WHERE alias_id=? ORDER BY id",
            (alias_id,),
        ).fetchall()
        if not prov_rows:
            continue
        best = sorted(
            prov_rows,
            key=lambda r: (_provenance_rank(cur, r[0], r[1], cache), float(r[3] or 0.0), int(r[4])),
            reverse=True,
        )[0]
        source_label, source_page, notes, confidence, _ = best
        current = cur.execute(
            "SELECT source_label, source_page, notes, confidence FROM abbreviation_aliases WHERE id=?",
            (alias_id,),
        ).fetchone()
        next_notes = merge_text(current[2] if current else None, notes)
        next_conf = max(float((current[3] if current else 0.0) or 0.0), float(confidence or 0.0))
        if not current or current[0] != source_label or current[1] != source_page or current[2] != next_notes or float(current[3] or 0.0) != next_conf:
            cur.execute(
                "UPDATE abbreviation_aliases SET source_label=?, source_page=?, notes=?, confidence=?, updated_at=datetime('now') WHERE id=?",
                (source_label, source_page, next_notes, next_conf, alias_id),
            )
            changed += 1
    return changed
