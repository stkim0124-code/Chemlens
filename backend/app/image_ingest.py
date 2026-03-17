# backend/app/image_ingest.py
from __future__ import annotations

import os
import re
import sqlite3
import hashlib
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from app.docs_init import ensure_docs_db
from app.ingest_utils import iter_image_files_recursive, rel_top_folder_tag
from app.ocr_utils import ocr_image, get_ocr_info

def _parse_page_no_from_stem(stem: str) -> Optional[int]:
    # common pattern: *_NN (1-based)
    m = re.search(r"_([0-9]{1,4})$", stem)
    if not m:
        return None
    try:
        return max(0, int(m.group(1)) - 1)
    except Exception:
        return None

def ingest_images_to_docs_db(
    images_dir: Path,
    docs_db_path: Path,
    limit: int = 0,
) -> Dict[str, int | str]:
    """OCR images under images_dir and store into docs DB (documents/pages/chunks/fts).

    Policy:
      - One 'document' per top folder under images_dir (e.g. Organic_Chemistry_[Clayden]_...)
      - Each image becomes a 'page'
      - file_path stored as images/<relative path>
      - chunks are created from page text similarly to pdf_ingest
    """
    ensure_docs_db(docs_db_path)
    images_dir = Path(images_dir)

    oinfo = get_ocr_info()
    if not (oinfo.paddle_available or oinfo.tesseract_available):
        raise RuntimeError(f"No OCR engine available. Details: {oinfo.details}")

    files = iter_image_files_recursive(images_dir, limit=0)
    files.sort(key=lambda p: str(p))
    if limit:
        files = files[:limit]

    con = sqlite3.connect(str(docs_db_path))
    try:
        con.row_factory = sqlite3.Row
        cols = {r["name"] for r in con.execute("PRAGMA table_info(documents)").fetchall()}
        has_hash = "content_hash" in cols
        has_original = "original_name" in cols
        has_stored = "stored_name" in cols

        # map: doc_title -> doc_id
        doc_map: Dict[str,int] = {}

        inserted_docs=0
        inserted_pages=0
        inserted_chunks=0
        skipped=0
        ocr_failed=0

        def ensure_doc(doc_title: str) -> int:
            nonlocal inserted_docs
            if doc_title in doc_map:
                return doc_map[doc_title]
            # De-dupe by title+file_path prefix (not strict)
            row = con.execute("SELECT id FROM documents WHERE title = ? AND file_path LIKE ? LIMIT 1", (doc_title, "images/%")).fetchone()
            if row:
                doc_id = int(row["id"])
                doc_map[doc_title]=doc_id
                return doc_id
            file_path = f"images/{doc_title}"
            if has_hash or has_original or has_stored:
                cur = con.execute(
                    """
                    INSERT INTO documents(title, file_path, page_count, original_name, stored_name, content_hash)
                    VALUES (?,?,?,?,?,?)
                    """,
                    (doc_title, file_path, 0, None, None, None),
                )
            else:
                cur = con.execute("INSERT INTO documents(title, file_path, page_count) VALUES (?,?,?)", (doc_title, file_path, 0))
            doc_id = int(cur.lastrowid)
            inserted_docs += 1
            doc_map[doc_title]=doc_id
            return doc_id

        # chunk helper (same spirit as pdf_ingest)
        def chunk_text(t: str, chunk_chars: int = 2500, overlap: int = 200) -> List[str]:
            t = (t or "").strip()
            if not t:
                return []
            out=[]
            pos=0
            while pos < len(t):
                end=min(len(t), pos+chunk_chars)
                chunk=t[pos:end].strip()
                if chunk:
                    out.append(chunk)
                if end==len(t):
                    break
                pos=end-overlap if end-overlap>pos else end
            return out

        for fp in files:
            rel = fp.resolve().relative_to(images_dir.resolve())
            doc_title = rel_top_folder_tag(images_dir, fp) or rel.parts[0] if rel.parts else fp.stem
            doc_id = ensure_doc(doc_title)

            # page no
            page_no = _parse_page_no_from_stem(fp.stem)
            if page_no is None:
                # fall back: append at end
                row = con.execute("SELECT COALESCE(MAX(page_no), -1) AS mx FROM pages WHERE doc_id = ?", (doc_id,)).fetchone()
                mx = int(row["mx"]) if row and row["mx"] is not None else -1
                page_no = mx + 1

            try:
                text = ocr_image(fp)
            except Exception:
                ocr_failed += 1
                continue

            con.execute("INSERT OR REPLACE INTO pages(doc_id, page_no, text) VALUES (?,?,?)", (doc_id, page_no, text or ""))
            inserted_pages += 1

            # chunks
            for ch in chunk_text(text or ""):
                con.execute("INSERT INTO chunks(doc_id, page_from, page_to, text) VALUES (?,?,?,?)", (doc_id, page_no, page_no, ch))
                inserted_chunks += 1

        # update page_count
        for doc_title, doc_id in doc_map.items():
            row = con.execute("SELECT COUNT(*) AS c FROM pages WHERE doc_id = ?", (doc_id,)).fetchone()
            c = int(row["c"]) if row else 0
            con.execute("UPDATE documents SET page_count = ? WHERE id = ?", (c, doc_id))

        con.commit()
    finally:
        con.close()

    return {
        "images_scanned": len(files),
        "docs_inserted": inserted_docs,
        "pages_inserted": inserted_pages,
        "chunks_inserted": inserted_chunks,
        "ocr_failed": ocr_failed,
        "docs_db_path": str(docs_db_path),
    }
