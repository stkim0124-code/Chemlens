#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""migrate_docs_remove_unique.py

Purpose
- Remove the UNIQUE constraint that blocks multiple documents from sharing the same PDF path.
- In this project, the UNIQUE is implemented as a UNIQUE INDEX on documents(file_path).

Why this version
- The previous approach rebuilt the `documents` table with a hard-coded column list.
  That is brittle and can break as soon as the documents schema changes.
- This version is schema-agnostic: it only drops the UNIQUE index (if present)
  and adds a normal (NON-UNIQUE) index for performance.

What it does
1) Creates a timestamped backup copy next to the DB.
2) Detects UNIQUE indexes on `documents` whose indexed columns are exactly [file_path].
3) Drops those UNIQUE indexes.
4) Ensures a NON-UNIQUE index exists on documents(file_path).

Usage (Windows)
  conda activate chemlens
  cd /d C:\chemlens\backend
  python tools\migrate_docs_remove_unique.py --db "app\data\labint_docs.db"

Then run
  python tools\sync_docs_db.py --db "app\data\labint_docs.db" --pdfs "app\data\pdfs" --apply
"""

import argparse
import os
import shutil
import sqlite3
from datetime import datetime


def backup_db(db_path: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{db_path}.bak_{ts}"
    shutil.copy2(db_path, backup_path)
    return backup_path


def find_unique_file_path_indexes(conn: sqlite3.Connection):
    """Return list of UNIQUE index names that index exactly (file_path) on documents."""
    cur = conn.cursor()
    cur.execute("PRAGMA index_list(documents)")
    idxs = []
    for seq, idx_name, is_unique, *rest in cur.fetchall():
        if not bool(is_unique):
            continue
        # columns of index
        cur.execute(f"PRAGMA index_info({idx_name})")
        cols = [r[2] for r in cur.fetchall()]  # r: (seqno, cid, name)
        if cols == ["file_path"]:
            idxs.append(idx_name)
    return idxs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to labint_docs.db")
    args = ap.parse_args()

    db_path = os.path.abspath(args.db)
    if not os.path.exists(db_path):
        raise SystemExit(f"[ERR] DB not found: {db_path}")

    backup_path = backup_db(db_path)
    print(f"[OK] Backup created: {backup_path}")

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE;")

        # sanity
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='documents';")
        if not cur.fetchone():
            raise SystemExit("[ERR] Table 'documents' not found in DB.")

        uniq_idxs = find_unique_file_path_indexes(conn)
        if not uniq_idxs:
            print("[OK] No UNIQUE(file_path) index detected. Nothing to do.")
            conn.rollback()
            return

        for idx in uniq_idxs:
            print(f"[INFO] Dropping UNIQUE index: {idx}")
            cur.execute(f"DROP INDEX IF EXISTS {idx};")

        # Ensure non-unique index exists
        cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_file_path ON documents(file_path);")

        conn.commit()
        print("[OK] Migration complete: UNIQUE(file_path) index removed; non-unique index ensured.")
    except Exception:
        conn.rollback()
        print("[ERR] Migration failed. DB rolled back. Backup is kept.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
