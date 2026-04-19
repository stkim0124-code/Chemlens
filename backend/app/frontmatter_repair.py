from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

from app.frontmatter_provenance import backfill_provenance_from_alias_rows, sanitize_provenance_pages, sync_alias_representative_provenance
from app.labint_frontmatter import ensure_frontmatter_schema, normalize_key

LEGACY_SOURCE_LABEL_MAP: Dict[str, str] = {
    'frontmatter_batch11_manual': 'named_reactions_frontmatter_batch11',
    'frontmatter_batch12': 'named_reactions_frontmatter_batch12',
}


def _coalesce_text(primary: Any, fallback: Any) -> Any:
    if primary is None:
        return fallback
    if isinstance(primary, str) and not primary.strip():
        return fallback
    return primary


def _prefer_page_value(current: Any, incoming: Any) -> Any:
    current = _coalesce_text(current, None)
    incoming = _coalesce_text(incoming, None)
    if current is None:
        return incoming
    if incoming is None:
        return current
    if isinstance(current, str) and isinstance(incoming, str):
        return incoming if len(incoming.strip()) > len(current.strip()) else current
    return current


def _merge_page_rows(cur: sqlite3.Cursor, legacy_id: int, canonical_id: int) -> Dict[str, int]:
    if legacy_id == canonical_id:
        return {'entities_relinked': 0, 'entities_deleted': 0, 'page_deleted': 0}

    cols = [
        'page_no', 'title', 'section_name', 'page_kind', 'summary', 'family_names',
        'reference_family_name', 'notes', 'image_filename',
    ]
    legacy = cur.execute(
        f"SELECT {', '.join(cols)} FROM manual_page_knowledge WHERE id=?", (legacy_id,)
    ).fetchone()
    canonical = cur.execute(
        f"SELECT {', '.join(cols)} FROM manual_page_knowledge WHERE id=?", (canonical_id,)
    ).fetchone()
    merged = []
    for c, l in zip(canonical, legacy):
        merged.append(_prefer_page_value(c, l))
    cur.execute(
        """
        UPDATE manual_page_knowledge
           SET page_no=?, title=?, section_name=?, page_kind=?, summary=?, family_names=?,
               reference_family_name=?, notes=?, image_filename=?, updated_at=datetime('now')
         WHERE id=?
        """,
        (*merged, canonical_id),
    )

    entities_relinked = 0
    entities_deleted = 0
    legacy_entities = cur.execute(
        """
        SELECT id, entity_text, entity_text_norm, canonical_name, entity_type, alias_id, family_name, notes, confidence
          FROM manual_page_entities
         WHERE page_knowledge_id=?
         ORDER BY id
        """,
        (legacy_id,),
    ).fetchall()
    for ent in legacy_entities:
        ent_id, entity_text, entity_text_norm, canonical_name, entity_type, alias_id, family_name, notes, confidence = ent
        existing = cur.execute(
            "SELECT id, canonical_name, alias_id, family_name, notes, confidence FROM manual_page_entities WHERE page_knowledge_id=? AND entity_text_norm=? AND entity_type=?",
            (canonical_id, entity_text_norm, entity_type),
        ).fetchone()
        if existing:
            existing_id, ex_can, ex_alias_id, ex_family, ex_notes, ex_conf = existing
            cur.execute(
                """
                UPDATE manual_page_entities
                   SET canonical_name=?,
                       alias_id=?,
                       family_name=?,
                       notes=?,
                       confidence=?,
                       updated_at=datetime('now')
                 WHERE id=?
                """,
                (
                    _prefer_page_value(ex_can, canonical_name),
                    ex_alias_id or alias_id,
                    _prefer_page_value(ex_family, family_name),
                    _prefer_page_value(ex_notes, notes),
                    max(float(ex_conf or 0.0), float(confidence or 0.0)),
                    existing_id,
                ),
            )
            cur.execute("DELETE FROM manual_page_entities WHERE id=?", (ent_id,))
            entities_deleted += 1
        else:
            cur.execute(
                "UPDATE manual_page_entities SET page_knowledge_id=?, updated_at=datetime('now') WHERE id=?",
                (canonical_id, ent_id),
            )
            entities_relinked += 1
    cur.execute("DELETE FROM manual_page_knowledge WHERE id=?", (legacy_id,))
    return {'entities_relinked': entities_relinked, 'entities_deleted': entities_deleted, 'page_deleted': 1}


def _rename_legacy_page_labels(cur: sqlite3.Cursor) -> Dict[str, int]:
    stats = {
        'legacy_pages_relabelled': 0,
        'legacy_pages_merged': 0,
        'legacy_page_entities_relinked': 0,
        'legacy_page_entities_deleted': 0,
    }
    for legacy_label, canonical_label in LEGACY_SOURCE_LABEL_MAP.items():
        page_rows = cur.execute(
            "SELECT id, page_label FROM manual_page_knowledge WHERE source_label=? ORDER BY page_no, id",
            (legacy_label,),
        ).fetchall()
        for legacy_id, page_label in page_rows:
            canonical = cur.execute(
                "SELECT id FROM manual_page_knowledge WHERE source_label=? AND page_label=?",
                (canonical_label, page_label),
            ).fetchone()
            if canonical:
                merged = _merge_page_rows(cur, legacy_id, int(canonical[0]))
                stats['legacy_pages_merged'] += merged['page_deleted']
                stats['legacy_page_entities_relinked'] += merged['entities_relinked']
                stats['legacy_page_entities_deleted'] += merged['entities_deleted']
            else:
                cur.execute(
                    "UPDATE manual_page_knowledge SET source_label=?, updated_at=datetime('now') WHERE id=?",
                    (canonical_label, legacy_id),
                )
                stats['legacy_pages_relabelled'] += 1
    return stats


def _normalize_abbreviation_keys(cur: sqlite3.Cursor) -> int:
    rows = cur.execute("SELECT id, alias, canonical_name FROM abbreviation_aliases ORDER BY id").fetchall()
    changed = 0
    for row_id, alias, canonical_name in rows:
        alias_norm = normalize_key(alias)
        canonical_norm = normalize_key(canonical_name) if canonical_name else None
        cur.execute(
            "UPDATE abbreviation_aliases SET alias_norm=?, canonical_name_norm=?, updated_at=datetime('now') WHERE id=?",
            (alias_norm, canonical_norm, row_id),
        )
        changed += 1
    return changed


def _dedupe_abbreviations(cur: sqlite3.Cursor) -> Dict[str, int]:
    rows = cur.execute(
        "SELECT id, alias, canonical_name, entity_type, notes, source_label, source_page, confidence FROM abbreviation_aliases ORDER BY id"
    ).fetchall()
    groups: Dict[tuple[str, str | None, str], list[tuple[Any, ...]]] = {}
    for row in rows:
        row_id, alias, canonical_name, entity_type, notes, source_label, source_page, confidence = row
        key = (normalize_key(alias), normalize_key(canonical_name) if canonical_name else None, entity_type)
        groups.setdefault(key, []).append(row)

    merged_groups = 0
    deleted_rows = 0
    alias_links_repointed = 0
    for key, members in groups.items():
        if len(members) < 2:
            continue

        def rank(row: tuple[Any, ...]) -> tuple[int, int, int, int, int]:
            row_id, alias, canonical_name, entity_type, notes, source_label, source_page, confidence = row
            return (
                1 if source_page is not None else 0,
                1 if (source_label or '').startswith('named_reactions_frontmatter_batch') else 0,
                1 if canonical_name else 0,
                1 if notes else 0,
                int(row_id),
            )

        keeper = sorted(members, key=rank, reverse=True)[0]
        keeper_id = int(keeper[0])
        merged_groups += 1

        for row in members:
            row_id = int(row[0])
            if row_id == keeper_id:
                continue
            cur.execute(
                "UPDATE manual_page_entities SET alias_id=? WHERE alias_id=?",
                (keeper_id, row_id),
            )
            alias_links_repointed += cur.rowcount or 0

            keeper_row = cur.execute(
                "SELECT notes, source_label, source_page, confidence FROM abbreviation_aliases WHERE id=?",
                (keeper_id,),
            ).fetchone()
            loser_row = cur.execute(
                "SELECT notes, source_label, source_page, confidence FROM abbreviation_aliases WHERE id=?",
                (row_id,),
            ).fetchone()
            cur.execute(
                """
                UPDATE abbreviation_aliases
                   SET notes=?,
                       source_label=?,
                       source_page=?,
                       confidence=?,
                       updated_at=datetime('now')
                 WHERE id=?
                """,
                (
                    _prefer_page_value(keeper_row[0], loser_row[0]),
                    _prefer_page_value(keeper_row[1], loser_row[1]),
                    keeper_row[2] if keeper_row[2] is not None else loser_row[2],
                    max(float(keeper_row[3] or 0.0), float(loser_row[3] or 0.0)),
                    keeper_id,
                ),
            )
            cur.execute("DELETE FROM abbreviation_aliases WHERE id=?", (row_id,))
            deleted_rows += 1

        cur.execute(
            "UPDATE abbreviation_aliases SET alias_norm=?, canonical_name_norm=?, updated_at=datetime('now') WHERE id=?",
            (key[0], key[1], keeper_id),
        )

    return {
        'abbr_duplicate_groups_merged': merged_groups,
        'abbr_duplicate_rows_deleted': deleted_rows,
        'abbr_alias_links_repointed': alias_links_repointed,
    }


def _rename_legacy_abbreviation_labels(cur: sqlite3.Cursor) -> int:
    changed = 0
    for legacy_label, canonical_label in LEGACY_SOURCE_LABEL_MAP.items():
        cur.execute(
            "UPDATE abbreviation_aliases SET source_label=?, updated_at=datetime('now') WHERE source_label=?",
            (canonical_label, legacy_label),
        )
        changed += cur.rowcount or 0
    return changed


def _repair_batch13_source_pages(cur: sqlite3.Cursor) -> int:
    # Representative source_page repair is now driven by provenance rows and batch page ranges.
    return 0


def repair_frontmatter_db(db_path: str | Path) -> Dict[str, Any]:
    ensure_frontmatter_schema(db_path)
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    stats: Dict[str, Any] = {}
    try:
        stats.update(_rename_legacy_page_labels(cur))
        stats['legacy_abbreviation_labels_renamed'] = _rename_legacy_abbreviation_labels(cur)
        stats.update(_dedupe_abbreviations(cur))
        stats['abbr_norm_rows_refreshed'] = _normalize_abbreviation_keys(cur)
        stats['batch13_source_pages_repaired'] = _repair_batch13_source_pages(cur)
        stats['abbreviation_provenance_backfilled'] = backfill_provenance_from_alias_rows(cur)
        stats['abbreviation_provenance_pages_sanitized'] = sanitize_provenance_pages(cur)
        stats['abbreviation_representatives_synced'] = sync_alias_representative_provenance(cur)
        conn.commit()
        stats['manual_page_knowledge'] = cur.execute('SELECT COUNT(*) FROM manual_page_knowledge').fetchone()[0]
        stats['manual_page_entities'] = cur.execute('SELECT COUNT(*) FROM manual_page_entities').fetchone()[0]
        stats['abbreviation_aliases'] = cur.execute('SELECT COUNT(*) FROM abbreviation_aliases').fetchone()[0]
        stats['abbreviation_alias_provenance'] = cur.execute("SELECT COUNT(*) FROM abbreviation_alias_provenance").fetchone()[0] if cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='abbreviation_alias_provenance'").fetchone() else 0
        stats['frontmatter_batch11_pages'] = cur.execute("SELECT COUNT(*) FROM manual_page_knowledge WHERE source_label='named_reactions_frontmatter_batch11'").fetchone()[0]
        stats['frontmatter_batch12_pages'] = cur.execute("SELECT COUNT(*) FROM manual_page_knowledge WHERE source_label='named_reactions_frontmatter_batch12'").fetchone()[0]
        stats['frontmatter_batch13_pages'] = cur.execute("SELECT COUNT(*) FROM manual_page_knowledge WHERE source_label='named_reactions_frontmatter_batch13'").fetchone()[0]
        stats['frontmatter_batch14_pages'] = cur.execute("SELECT COUNT(*) FROM manual_page_knowledge WHERE source_label='named_reactions_frontmatter_batch14'").fetchone()[0]
        return stats
    finally:
        conn.close()
