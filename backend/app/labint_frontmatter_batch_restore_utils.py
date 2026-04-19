from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.labint_frontmatter import ensure_frontmatter_schema, normalize_key


def _backend_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _batch_seed_dir(batch_no: int) -> Path:
    return _backend_root() / 'seed_templates' / f'frontmatter_batch{batch_no}'


def _read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    with path.open('r', encoding='utf-8', newline='') as f:
        return list(csv.DictReader(f))


def load_page_knowledge_seeds(batch_no: int, source_label: str) -> List[Dict[str, Any]]:
    rows = _read_csv_rows(_batch_seed_dir(batch_no) / f'frontmatter_batch{batch_no}_manual_pages.csv')
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append({
            'source_label': source_label,
            'page_label': row['page_label'],
            'page_no': int(row['page_no']) if row.get('page_no') else None,
            'title': row.get('title') or None,
            'section_name': row.get('section_name') or None,
            'page_kind': row.get('page_kind') or 'reference_explanatory',
            'summary': row.get('summary') or None,
            'family_names': row.get('family_names') or None,
            'reference_family_name': row.get('reference_family_name') or None,
            'notes': row.get('notes') or None,
            'image_filename': row.get('image_filename') or None,
        })
    return out


def load_family_pattern_seeds(batch_no: int, version: str) -> List[Dict[str, Any]]:
    rows = _read_csv_rows(_batch_seed_dir(batch_no) / f'frontmatter_batch{batch_no}_family_seed.csv')
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append({
            'family_name': row['family_name'],
            'family_class': row.get('family_class') or None,
            'transformation_type': row.get('transformation_type') or None,
            'mechanism_type': row.get('mechanism_type') or None,
            'reactant_pattern_text': row.get('reactant_pattern_text') or None,
            'product_pattern_text': row.get('product_pattern_text') or None,
            'key_reagents_clue': row.get('key_reagents_clue') or None,
            'common_solvents': row.get('common_solvents') or None,
            'common_conditions': row.get('common_conditions') or None,
            'synonym_names': row.get('synonym_names') or None,
            'description_short': row.get('description_short') or None,
            'overview_count': int(row.get('overview_count') or 1),
            'application_count': int(row.get('application_count') or 1),
            'mechanism_count': int(row.get('mechanism_count') or 1),
            'seeded_from': version,
        })
    return out


def load_abbreviation_seeds(
    batch_no: int,
    source_label: str,
    source_pages: Optional[Dict[str, int]] = None,
    default_note: Optional[str] = None,
) -> List[Dict[str, Any]]:
    rows = _read_csv_rows(_batch_seed_dir(batch_no) / f'frontmatter_batch{batch_no}_abbreviation_seed.csv')
    out: List[Dict[str, Any]] = []
    for row in rows:
        alias = row['alias']
        out.append({
            'alias': alias,
            'canonical_name': row.get('canonical_name') or None,
            'entity_type': row.get('entity_type') or 'chemical_term',
            'source_page': (source_pages or {}).get(alias),
            'notes': default_note,
            'source_label': source_label,
        })
    return out


def _upsert_family(cur: sqlite3.Cursor, seed: Dict[str, Any], latest_source_zip: str, version: str) -> None:
    vals = (
        seed['family_name'], normalize_key(seed['family_name']), seed.get('family_class'), seed.get('transformation_type'), seed.get('mechanism_type'),
        seed.get('reactant_pattern_text'), seed.get('product_pattern_text'), seed.get('key_reagents_clue'), seed.get('common_solvents'), seed.get('common_conditions'),
        seed.get('synonym_names'), seed.get('description_short'), int(seed.get('overview_count') or 0), int(seed.get('application_count') or 0), int(seed.get('mechanism_count') or 0),
        latest_source_zip, seed.get('seeded_from') or version,
    )
    cur.execute(
        """
        INSERT INTO reaction_family_patterns (
          family_name,family_name_norm,family_class,transformation_type,mechanism_type,reactant_pattern_text,product_pattern_text,
          key_reagents_clue,common_solvents,common_conditions,synonym_names,description_short,overview_count,application_count,mechanism_count,
          latest_source_zip,seeded_from,latest_updated_at,created_at,updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),datetime('now'),datetime('now'))
        ON CONFLICT(family_name_norm) DO UPDATE SET
          family_name=excluded.family_name,
          family_class=COALESCE(excluded.family_class, reaction_family_patterns.family_class),
          transformation_type=COALESCE(excluded.transformation_type, reaction_family_patterns.transformation_type),
          mechanism_type=COALESCE(excluded.mechanism_type, reaction_family_patterns.mechanism_type),
          reactant_pattern_text=COALESCE(excluded.reactant_pattern_text, reaction_family_patterns.reactant_pattern_text),
          product_pattern_text=COALESCE(excluded.product_pattern_text, reaction_family_patterns.product_pattern_text),
          key_reagents_clue=COALESCE(excluded.key_reagents_clue, reaction_family_patterns.key_reagents_clue),
          common_solvents=COALESCE(excluded.common_solvents, reaction_family_patterns.common_solvents),
          common_conditions=COALESCE(excluded.common_conditions, reaction_family_patterns.common_conditions),
          synonym_names=CASE
            WHEN excluded.synonym_names IS NULL OR trim(excluded.synonym_names) = '' THEN reaction_family_patterns.synonym_names
            WHEN reaction_family_patterns.synonym_names IS NULL OR trim(reaction_family_patterns.synonym_names) = '' THEN excluded.synonym_names
            WHEN instr(lower(reaction_family_patterns.synonym_names), lower(excluded.synonym_names)) > 0 THEN reaction_family_patterns.synonym_names
            ELSE reaction_family_patterns.synonym_names || '|' || excluded.synonym_names
          END,
          description_short=COALESCE(excluded.description_short, reaction_family_patterns.description_short),
          overview_count=MAX(COALESCE(reaction_family_patterns.overview_count, 0), COALESCE(excluded.overview_count, 0)),
          application_count=MAX(COALESCE(reaction_family_patterns.application_count, 0), COALESCE(excluded.application_count, 0)),
          mechanism_count=MAX(COALESCE(reaction_family_patterns.mechanism_count, 0), COALESCE(excluded.mechanism_count, 0)),
          latest_source_zip=excluded.latest_source_zip,
          seeded_from=excluded.seeded_from,
          latest_updated_at=datetime('now'),
          updated_at=datetime('now')
        """,
        vals,
    )


def apply_frontmatter_batch_generic(
    *,
    db_path: str | Path,
    source_label: str,
    version: str,
    latest_source_zip: str,
    batch_no: int,
    page_knowledge_seeds: List[Dict[str, Any]],
    family_pattern_seeds: List[Dict[str, Any]],
    abbreviation_seeds: List[Dict[str, Any]],
    page_entity_seeds: List[Dict[str, Any]],
    replace_existing_page_entities: bool = False,
) -> Dict[str, Any]:
    db_path = str(db_path)
    ensure_frontmatter_schema(db_path)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    for seed in family_pattern_seeds:
        _upsert_family(cur, seed, latest_source_zip=latest_source_zip, version=version)

    page_ids: Dict[str, int] = {}
    for seed in page_knowledge_seeds:
        cur.execute(
            """
            INSERT INTO manual_page_knowledge (
              source_label,page_label,page_no,title,section_name,page_kind,summary,family_names,reference_family_name,notes,image_filename,created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'),datetime('now'))
            ON CONFLICT(source_label,page_label) DO UPDATE SET
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
                seed['source_label'], seed['page_label'], seed.get('page_no'), seed.get('title'), seed.get('section_name'), seed.get('page_kind'),
                seed.get('summary'), seed.get('family_names'), seed.get('reference_family_name'), seed.get('notes'), seed.get('image_filename'),
            ),
        )
        cur.execute('SELECT id FROM manual_page_knowledge WHERE source_label=? AND page_label=?', (seed['source_label'], seed['page_label']))
        page_ids[seed['page_label']] = cur.fetchone()[0]

    if replace_existing_page_entities and page_ids:
        cur.executemany(
            'DELETE FROM manual_page_entities WHERE page_knowledge_id=?',
            [(pid,) for pid in page_ids.values()],
        )

    for seed in abbreviation_seeds:
        can_norm = normalize_key(seed['canonical_name']) if seed.get('canonical_name') else None
        entity_type = seed.get('entity_type') or 'chemical_term'
        existing = cur.execute(
            "SELECT id FROM abbreviation_aliases WHERE lower(alias)=lower(?) AND coalesce(lower(canonical_name),'')=coalesce(lower(?),'') AND entity_type=? LIMIT 1",
            (seed['alias'], seed.get('canonical_name'), entity_type),
        ).fetchone()
        if existing:
            cur.execute(
                "UPDATE abbreviation_aliases SET notes=COALESCE(?, notes), source_label=?, source_page=COALESCE(?, source_page), updated_at=datetime('now') WHERE id=?",
                (seed.get('notes'), seed.get('source_label') or source_label, seed.get('source_page'), existing[0]),
            )
        else:
            cur.execute(
                """
                INSERT INTO abbreviation_aliases (
                  alias,alias_norm,canonical_name,canonical_name_norm,entity_type,notes,source_label,source_page,confidence,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,0.92,datetime('now'),datetime('now'))
                ON CONFLICT(alias_norm, canonical_name_norm, entity_type) DO UPDATE SET
                  notes=COALESCE(excluded.notes, abbreviation_aliases.notes),
                  source_label=excluded.source_label,
                  source_page=COALESCE(excluded.source_page, abbreviation_aliases.source_page),
                  updated_at=datetime('now')
                """,
                (
                    seed['alias'], normalize_key(seed['alias']), seed.get('canonical_name'), can_norm, entity_type,
                    seed.get('notes'), seed.get('source_label') or source_label, seed.get('source_page'),
                ),
            )

    alias_lookup = {row[0]: row[1] for row in cur.execute('SELECT alias_norm, id FROM abbreviation_aliases')}
    for seed in page_entity_seeds:
        pid = page_ids.get(seed['page_label'])
        if not pid:
            continue
        entity_text_norm = normalize_key(seed['entity_text'])
        alias_id = alias_lookup.get(entity_text_norm) if seed.get('entity_type') == 'abbreviation' else None
        existing = cur.execute(
            "SELECT id FROM manual_page_entities WHERE page_knowledge_id=? AND lower(entity_text)=lower(?) AND entity_type=? LIMIT 1",
            (pid, seed['entity_text'], seed['entity_type']),
        ).fetchone()
        if existing:
            cur.execute(
                "UPDATE manual_page_entities SET entity_text_norm=?, canonical_name=?, alias_id=COALESCE(?, alias_id), family_name=?, notes=?, confidence=?, updated_at=datetime('now') WHERE id=?",
                (entity_text_norm, seed.get('canonical_name'), alias_id, seed.get('family_name'), seed.get('notes'), float(seed.get('confidence', 0.9)), existing[0]),
            )
        else:
            cur.execute(
                """
                INSERT INTO manual_page_entities (
                  page_knowledge_id,entity_text,entity_text_norm,canonical_name,entity_type,alias_id,family_name,notes,confidence,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,datetime('now'),datetime('now'))
                ON CONFLICT(page_knowledge_id, entity_text_norm, entity_type) DO UPDATE SET
                  canonical_name=excluded.canonical_name,
                  alias_id=COALESCE(excluded.alias_id, manual_page_entities.alias_id),
                  family_name=excluded.family_name,
                  notes=excluded.notes,
                  confidence=excluded.confidence,
                  updated_at=datetime('now')
                """,
                (
                    pid, seed['entity_text'], entity_text_norm, seed.get('canonical_name'), seed['entity_type'], alias_id,
                    seed.get('family_name'), seed.get('notes'), float(seed.get('confidence', 0.9)),
                ),
            )

    conn.commit()
    result = {
        'manual_page_knowledge': cur.execute('SELECT COUNT(*) FROM manual_page_knowledge').fetchone()[0],
        'manual_page_entities': cur.execute('SELECT COUNT(*) FROM manual_page_entities').fetchone()[0],
        'abbreviation_aliases': cur.execute('SELECT COUNT(*) FROM abbreviation_aliases').fetchone()[0],
        'reaction_family_patterns': cur.execute('SELECT COUNT(*) FROM reaction_family_patterns').fetchone()[0],
        f'batch{batch_no}_pages': cur.execute('SELECT COUNT(*) FROM manual_page_knowledge WHERE source_label=?', (source_label,)).fetchone()[0],
        f'batch{batch_no}_entities': cur.execute("SELECT COUNT(*) FROM manual_page_entities mpe JOIN manual_page_knowledge mpk ON mpe.page_knowledge_id=mpk.id WHERE mpk.source_label=?", (source_label,)).fetchone()[0],
    }
    conn.close()
    return result
