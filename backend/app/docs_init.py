# backend/app/docs_init.py
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  file_path TEXT NOT NULL,
  page_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pages (
  doc_id INTEGER NOT NULL,
  page_no INTEGER NOT NULL,
  text TEXT NOT NULL,
  PRIMARY KEY (doc_id, page_no),
  FOREIGN KEY (doc_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS chunks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id INTEGER NOT NULL,
  page_from INTEGER NOT NULL,
  page_to INTEGER NOT NULL,
  text TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (doc_id) REFERENCES documents(id) ON DELETE CASCADE
);

-- FTS5 virtual table for chunks
CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(text, content='chunks', content_rowid='id');

-- Triggers to keep FTS in sync
CREATE TRIGGER IF NOT EXISTS chunks_ai AFTER INSERT ON chunks BEGIN
  INSERT INTO chunks_fts(rowid, text) VALUES (new.id, new.text);
END;

CREATE TRIGGER IF NOT EXISTS chunks_ad AFTER DELETE ON chunks BEGIN
  INSERT INTO chunks_fts(chunks_fts, rowid, text) VALUES('delete', old.id, old.text);
END;

CREATE TRIGGER IF NOT EXISTS chunks_au AFTER UPDATE ON chunks BEGIN
  INSERT INTO chunks_fts(chunks_fts, rowid, text) VALUES('delete', old.id, old.text);
  INSERT INTO chunks_fts(rowid, text) VALUES (new.id, new.text);
END;
"""

def ensure_docs_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if not db_path.exists():
        db_path.touch()
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(SCHEMA_SQL)

        # --- forward-compatible migrations (no external migrate step required) ---
        # Add columns that improve reproducibility + de-duplication.
        # - original_name: the uploaded filename as-is
        # - stored_name  : the canonical stored filename under app/data/pdfs (uuid.pdf)
        # - content_hash : sha256 for strict de-dupe and provenance
        cur = con.cursor()
        cur.execute("PRAGMA table_info(documents)")
        cols = {r[1] for r in cur.fetchall()}  # (cid, name, type, ...)

        if "original_name" not in cols:
            cur.execute("ALTER TABLE documents ADD COLUMN original_name TEXT")
        if "stored_name" not in cols:
            cur.execute("ALTER TABLE documents ADD COLUMN stored_name TEXT")
        if "content_hash" not in cols:
            cur.execute("ALTER TABLE documents ADD COLUMN content_hash TEXT")

        cur.execute("CREATE INDEX IF NOT EXISTS documents_content_hash_idx ON documents(content_hash)")
        con.commit()
    finally:
        con.close()
