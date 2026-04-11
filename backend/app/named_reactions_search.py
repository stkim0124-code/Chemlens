
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception:
        pass
    conn.row_factory = sqlite3.Row
    return conn


def named_reactions_tables_exist(db_path: Path) -> bool:
    conn = _connect(db_path)
    try:
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        return {'page_images', 'scheme_candidates', 'reaction_extracts'}.issubset(names)
    finally:
        conn.close()


def get_named_reactions_counts(db_path: Path) -> Dict[str, Any]:
    if not named_reactions_tables_exist(db_path):
        return {
            'merged': False,
            'page_images': 0,
            'scheme_candidates': 0,
            'reaction_extracts': 0,
            'reaction_families': 0,
            'reaction_content_pages': 0,
            'meta_pages': 0,
        }
    conn = _connect(db_path)
    try:
        return {
            'merged': True,
            'page_images': int(conn.execute('SELECT COUNT(*) FROM page_images').fetchone()[0]),
            'scheme_candidates': int(conn.execute('SELECT COUNT(*) FROM scheme_candidates').fetchone()[0]),
            'reaction_extracts': int(conn.execute('SELECT COUNT(*) FROM reaction_extracts').fetchone()[0]),
            'reaction_families': int(conn.execute("SELECT COUNT(DISTINCT COALESCE(reaction_family_name_norm, reaction_family_name)) FROM reaction_extracts WHERE COALESCE(reaction_family_name_norm, reaction_family_name, '') <> ''").fetchone()[0]),
            'reaction_content_pages': int(conn.execute("SELECT COUNT(*) FROM page_images WHERE page_kind='reaction_content'").fetchone()[0]),
            'meta_pages': int(conn.execute("SELECT COUNT(*) FROM page_images WHERE page_kind='meta_explanatory'").fetchone()[0]),
        }
    finally:
        conn.close()


def get_named_reaction_top_families(db_path: Path, limit: int = 20) -> List[Dict[str, Any]]:
    if not named_reactions_tables_exist(db_path):
        return []
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT COALESCE(reaction_family_name_norm, reaction_family_name) AS family, COUNT(*) AS n
            FROM reaction_extracts
            WHERE COALESCE(reaction_family_name_norm, reaction_family_name, '') <> ''
            GROUP BY COALESCE(reaction_family_name_norm, reaction_family_name)
            ORDER BY n DESC, family ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def search_named_reactions(db_path: Path, query: str = '', limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
    if not named_reactions_tables_exist(db_path):
        return []
    q = (query or '').strip().lower()
    conn = _connect(db_path)
    try:
        if not q:
            rows = conn.execute(
                """
                SELECT
                    re.id AS extract_id,
                    pi.source_zip,
                    pi.source_doc,
                    pi.page_no,
                    pi.image_filename,
                    pi.page_kind,
                    sc.scheme_index,
                    sc.section_type,
                    sc.scheme_role,
                    re.reaction_family_name,
                    re.reaction_family_name_norm,
                    re.extract_kind,
                    re.transformation_text,
                    re.reactants_text,
                    re.products_text,
                    re.reagents_text,
                    re.conditions_text,
                    re.yield_text,
                    re.notes_text,
                    re.reactant_smiles,
                    re.product_smiles,
                    re.extraction_confidence,
                    re.created_at
                FROM reaction_extracts re
                JOIN scheme_candidates sc ON sc.id = re.scheme_candidate_id
                JOIN page_images pi ON pi.id = sc.page_image_id
                ORDER BY re.id DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
            return [dict(r) for r in rows]

        like = f'%{q}%'
        rows = conn.execute(
            """
            SELECT
                re.id AS extract_id,
                pi.source_zip,
                pi.source_doc,
                pi.page_no,
                pi.image_filename,
                pi.page_kind,
                sc.scheme_index,
                sc.section_type,
                sc.scheme_role,
                re.reaction_family_name,
                re.reaction_family_name_norm,
                re.extract_kind,
                re.transformation_text,
                re.reactants_text,
                re.products_text,
                re.reagents_text,
                re.conditions_text,
                re.yield_text,
                re.notes_text,
                re.reactant_smiles,
                re.product_smiles,
                re.extraction_confidence,
                CASE
                    WHEN lower(COALESCE(re.reaction_family_name_norm, re.reaction_family_name, '')) = ? THEN 100
                    WHEN lower(COALESCE(re.reaction_family_name_norm, re.reaction_family_name, '')) LIKE ? THEN 80
                    WHEN lower(COALESCE(re.transformation_text, '')) LIKE ? THEN 60
                    WHEN lower(COALESCE(re.products_text, '')) LIKE ? THEN 45
                    WHEN lower(COALESCE(re.reactants_text, '')) LIKE ? THEN 40
                    WHEN lower(COALESCE(re.reagents_text, '')) LIKE ? THEN 30
                    WHEN lower(COALESCE(re.conditions_text, '')) LIKE ? THEN 20
                    WHEN lower(COALESCE(sc.caption_text, '')) LIKE ? THEN 15
                    WHEN lower(COALESCE(sc.vision_summary, '')) LIKE ? THEN 10
                    ELSE 5
                END AS score
            FROM reaction_extracts re
            JOIN scheme_candidates sc ON sc.id = re.scheme_candidate_id
            JOIN page_images pi ON pi.id = sc.page_image_id
            WHERE (
                lower(COALESCE(re.reaction_family_name, '')) LIKE ? OR
                lower(COALESCE(re.reaction_family_name_norm, '')) LIKE ? OR
                lower(COALESCE(re.transformation_text, '')) LIKE ? OR
                lower(COALESCE(re.reactants_text, '')) LIKE ? OR
                lower(COALESCE(re.products_text, '')) LIKE ? OR
                lower(COALESCE(re.reagents_text, '')) LIKE ? OR
                lower(COALESCE(re.conditions_text, '')) LIKE ? OR
                lower(COALESCE(re.notes_text, '')) LIKE ? OR
                lower(COALESCE(sc.caption_text, '')) LIKE ? OR
                lower(COALESCE(sc.vision_summary, '')) LIKE ? OR
                lower(COALESCE(pi.source_doc, '')) LIKE ?
            )
            ORDER BY score DESC, re.id DESC
            LIMIT ? OFFSET ?
            """,
            (q, like, like, like, like, like, like, like, like,
             like, like, like, like, like, like, like, like, like, like, like,
             limit, offset),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
