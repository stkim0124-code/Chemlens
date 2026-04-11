
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path


TABLES = ["page_images", "scheme_candidates", "reaction_extracts"]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge round9 named reactions staging tables into labint.db")
    ap.add_argument('--main-db', default='app/labint.db')
    ap.add_argument('--staging-db', default='labint_round9_v5_final_staging.db')
    ap.add_argument('--backup-dir', default='merge_backups')
    ap.add_argument('--tag', default='round9_named_reactions_test')
    args = ap.parse_args()

    main_db = Path(args.main_db).resolve()
    staging_db = Path(args.staging_db).resolve()
    backup_dir = Path(args.backup_dir).resolve()
    backup_dir.mkdir(parents=True, exist_ok=True)

    if not main_db.exists():
        raise SystemExit(f"Main DB not found: {main_db}")
    if not staging_db.exists():
        raise SystemExit(f"Staging DB not found: {staging_db}")

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_db = backup_dir / f"{main_db.stem}_before_{args.tag}_{ts}{main_db.suffix}"
    shutil.copy2(main_db, backup_db)

    conn = sqlite3.connect(str(main_db))
    conn.execute('PRAGMA foreign_keys=OFF')
    conn.execute('PRAGMA busy_timeout=5000')
    conn.execute(f"ATTACH DATABASE '{staging_db}' AS stg")

    created_tables = []
    for t in TABLES:
        if not table_exists(conn, t):
            create_sql = conn.execute(
                "SELECT sql FROM stg.sqlite_master WHERE type='table' AND name=?", (t,)
            ).fetchone()[0]
            conn.execute(create_sql)
            created_tables.append(t)

    before = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] if table_exists(conn, t) else 0 for t in TABLES}

    conn.execute("""
        INSERT OR IGNORE INTO page_images
        (id, source_zip, source_doc, page_no, image_path, image_filename, sha256, file_size_bytes, width, height,
         ingest_batch, ingest_status, error_message, created_at, updated_at, page_kind)
        SELECT id, source_zip, source_doc, page_no, image_path, image_filename, sha256, file_size_bytes, width, height,
               ingest_batch, ingest_status, error_message, created_at, updated_at, page_kind
        FROM stg.page_images
    """)
    conn.execute("""
        INSERT OR IGNORE INTO scheme_candidates
        (id, page_image_id, scheme_index, section_type, scheme_role, bbox_x1, bbox_y1, bbox_x2, bbox_y2, crop_path,
         nearby_text, caption_text, vision_summary, vision_raw_json, confidence, review_status, review_notes,
         detector_model, detector_prompt_version, created_at, updated_at)
        SELECT id, page_image_id, scheme_index, section_type, scheme_role, bbox_x1, bbox_y1, bbox_x2, bbox_y2, crop_path,
               nearby_text, caption_text, vision_summary, vision_raw_json, confidence, review_status, review_notes,
               detector_model, detector_prompt_version, created_at, updated_at
        FROM stg.scheme_candidates
    """)
    conn.execute("""
        INSERT OR IGNORE INTO reaction_extracts
        (id, scheme_candidate_id, reaction_family_name, reaction_family_name_norm, extract_kind, transformation_text, reactants_text,
         products_text, intermediates_text, reagents_text, catalysts_text, solvents_text, temperature_text, time_text,
         yield_text, workup_text, conditions_text, notes_text, reactant_smiles, product_smiles, smiles_confidence,
         extraction_confidence, parse_status, promote_decision, rejection_reason, extractor_model, extractor_prompt_version,
         extraction_raw_json, created_at, updated_at)
        SELECT id, scheme_candidate_id, reaction_family_name, reaction_family_name_norm, extract_kind, transformation_text, reactants_text,
               products_text, intermediates_text, reagents_text, catalysts_text, solvents_text, temperature_text, time_text,
               yield_text, workup_text, conditions_text, notes_text, reactant_smiles, product_smiles, smiles_confidence,
               extraction_confidence, parse_status, promote_decision, rejection_reason, extractor_model, extractor_prompt_version,
               extraction_raw_json, created_at, updated_at
        FROM stg.reaction_extracts
    """)

    for sql in [
        'CREATE INDEX IF NOT EXISTS idx_page_images_sourcezip_page ON page_images(source_zip, page_no)',
        'CREATE INDEX IF NOT EXISTS idx_page_images_filename ON page_images(image_filename)',
        'CREATE INDEX IF NOT EXISTS idx_scheme_candidates_page ON scheme_candidates(page_image_id, scheme_index)',
        'CREATE INDEX IF NOT EXISTS idx_reaction_extracts_scheme ON reaction_extracts(scheme_candidate_id)',
        'CREATE INDEX IF NOT EXISTS idx_reaction_extracts_family ON reaction_extracts(reaction_family_name)',
        'CREATE INDEX IF NOT EXISTS idx_reaction_extracts_family_norm ON reaction_extracts(reaction_family_name_norm)',
        'CREATE INDEX IF NOT EXISTS idx_reaction_extracts_kind ON reaction_extracts(extract_kind)'
    ]:
        conn.execute(sql)

    after = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in TABLES}
    conn.commit()
    conn.execute('DETACH DATABASE stg')
    conn.close()

    summary = {
        'sealed_staging_db': str(staging_db),
        'main_db': str(main_db),
        'backup_db': str(backup_db),
        'created_tables': created_tables,
        'before': before,
        'after': after,
        'delta': {t: after[t] - before.get(t, 0) for t in TABLES},
        'main_db_sha256': sha256_file(main_db),
        'policy': {
            'staging_only': True,
            'promote_reaction_cards_forbidden': True,
        },
    }
    summary_path = main_db.parent.parent / f'merge_summary_{args.tag}.json' if main_db.parent.name == 'app' else main_db.parent / f'merge_summary_{args.tag}.json'
    inventory_path = summary_path.with_suffix('.csv')
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding='utf-8')
    with inventory_path.open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['table', 'before', 'after', 'delta'])
        for t in TABLES:
            w.writerow([t, before[t], after[t], summary['delta'][t]])

    print('=== merge_round9_staging_into_labint ===')
    print(f'main_db={main_db}')
    print(f'staging_db={staging_db}')
    print(f'backup_db={backup_db}')
    for t in TABLES:
        print(f'{t}: {before[t]} -> {after[t]} (delta {summary["delta"][t]})')
    print(f'summary: {summary_path}')
    print(f'inventory: {inventory_path}')


if __name__ == '__main__':
    main()
