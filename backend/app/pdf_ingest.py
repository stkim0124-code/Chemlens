# backend/app/pdf_ingest.py
from __future__ import annotations

import os
import re
import sqlite3
import hashlib
import shutil
import uuid
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from app.docs_init import ensure_docs_db
from app.ocr_utils import get_ocr_info, ocr_image

def _safe_title_from_path(p: Path) -> str:
    # prefer stem; keep human-readable even if user passed escaped names
    return p.stem

def _extract_pdf_text_pages(pdf_path: Path) -> Tuple[List[str], str]:
    """Return list of page texts and engine name.

    Primary: PyMuPDF text layer (fast)
    Fallback (optional): page render -> OCR for pages that look empty.

    Env flags:
      - LABINT_PDF_OCR=1 to enable OCR fallback (default: 0)
      - LABINT_PDF_OCR_MIN_CHARS: if extracted text length < N => OCR (default: 30)
      - LABINT_PDF_OCR_MAX_PAGES: max pages per PDF to OCR (0=all, default: 0)
    """
    try:
        import fitz  # type: ignore
    except Exception as e:
        raise RuntimeError("PyMuPDF not installed. Install: pip install pymupdf") from e

    use_ocr = (os.environ.get("LABINT_PDF_OCR", "0").strip() == "1")
    min_chars = int(os.environ.get("LABINT_PDF_OCR_MIN_CHARS", "30") or "30")
    max_ocr_pages = int(os.environ.get("LABINT_PDF_OCR_MAX_PAGES", "0") or "0")

    doc = fitz.open(str(pdf_path))
    pages: List[str] = []
    ocr_engine = ""
    ocr_used_pages = 0

    # check OCR availability once
    oinfo = get_ocr_info()
    ocr_available = (oinfo.paddle_available or oinfo.tesseract_available)

    for i in range(doc.page_count):
        page = doc.load_page(i)
        txt = (page.get_text("text") or "").strip()

        if use_ocr and ocr_available and len(txt) < min_chars:
            if max_ocr_pages == 0 or ocr_used_pages < max_ocr_pages:
                # Render page to a temp PNG and OCR it
                try:
                    pix = page.get_pixmap(dpi=250, alpha=False)
                    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tf:
                        tmp_path = Path(tf.name)
                        pix.save(str(tmp_path))
                    try:
                        ocr_txt = (ocr_image(tmp_path) or "").strip()
                        if ocr_txt:
                            txt = ocr_txt
                            ocr_engine = oinfo.engine or "auto"
                            ocr_used_pages += 1
                    finally:
                        try:
                            tmp_path.unlink(missing_ok=True)  # type: ignore[arg-type]
                        except Exception:
                            pass
                except Exception:
                    # ignore OCR failure; keep empty text
                    pass

        pages.append(txt)

    doc.close()
    engine = "pymupdf+ocr" if (use_ocr and ocr_used_pages) else "pymupdf"
    return pages, engine

def _chunk_text(pages: List[str], chunk_chars: int = 2500, overlap: int = 200) -> List[Tuple[int,int,str]]:
    """Return list of (page_from, page_to, text). Here page_to==page_from for simplicity."""
    chunks=[]
    for i, t in enumerate(pages):
        t=(t or "").strip()
        if not t:
            continue
        # split long pages
        pos=0
        while pos < len(t):
            end=min(len(t), pos+chunk_chars)
            chunk=t[pos:end].strip()
            if chunk:
                chunks.append((i,i,chunk))
            if end==len(t):
                break
            pos=end-overlap if end-overlap>pos else end
    return chunks

def ingest_pdfs_to_docs_db(
    pdfs_dir: Path,
    docs_db_path: Path,
    limit: int = 0,
) -> Dict[str, int | str]:
    ensure_docs_db(docs_db_path)
    pdfs_dir = Path(pdfs_dir)
    files = sorted([p for p in list(pdfs_dir.rglob("*.pdf")) + list(pdfs_dir.rglob("*.PDF")) if p.is_file()], key=lambda x: str(x))
    if limit:
        files = files[:limit]

    inserted_docs=0
    inserted_pages=0
    inserted_chunks=0
    skipped=0
    failed_files: List[Dict[str, str]] = []

    con = sqlite3.connect(str(docs_db_path))
    try:
        # detect optional columns (forward compatible)
        con.row_factory = sqlite3.Row
        cols = {r["name"] for r in con.execute("PRAGMA table_info(documents)").fetchall()}
        has_hash = "content_hash" in cols
        has_original = "original_name" in cols
        has_stored = "stored_name" in cols

        for pdf in files:
            try:
                title=_safe_title_from_path(pdf)

                # --- content hash (strict de-dupe) ---
                content_hash = None
                try:
                    h = hashlib.sha256()
                    with pdf.open("rb") as f:
                        for chunk in iter(lambda: f.read(1024 * 1024), b""):
                            h.update(chunk)
                    content_hash = h.hexdigest()
                except Exception:
                    content_hash = None

                if has_hash and content_hash:
                    row = con.execute(
                        "SELECT id FROM documents WHERE content_hash = ? LIMIT 1",
                        (content_hash,),
                    ).fetchone()
                    if row:
                        skipped += 1
                        continue

                canonicalize = (os.environ.get("LABINT_CANONICALIZE_PDFS", "1").strip() != "0")
                problematic = bool(re.search(r"[^A-Za-z0-9._-]", pdf.name))
                if canonicalize or problematic:
                    stored_name = f"{uuid.uuid4().hex}.pdf"
                    dst = pdfs_dir / stored_name
                    if not dst.exists():
                        try:
                            shutil.copy2(str(pdf), str(dst))
                        except Exception:
                            stored_name = pdf.name
                    stored = f"pdfs/{stored_name}"
                else:
                    stored_name = pdf.name
                    stored = f"pdfs/{stored_name}"

                row = con.execute(
                    "SELECT id FROM documents WHERE file_path = ? OR file_path = ? OR lower(file_path) = lower(?) OR lower(file_path) = lower(?) OR file_path LIKE ? LIMIT 1",
                    (stored, stored_name, stored, stored_name, f"%{stored_name}")
                ).fetchone()
                if row:
                    skipped += 1
                    continue

                pages, engine = _extract_pdf_text_pages(pdf)
                if has_hash or has_original or has_stored:
                    cur = con.execute(
                        """
                        INSERT INTO documents(title, file_path, page_count, original_name, stored_name, content_hash)
                        VALUES (?,?,?,?,?,?)
                        """,
                        (
                            title,
                            stored,
                            len(pages),
                            pdf.name if has_original else None,
                            stored_name if has_stored else None,
                            content_hash if has_hash else None,
                        ),
                    )
                else:
                    cur = con.execute(
                        "INSERT INTO documents(title, file_path, page_count) VALUES (?,?,?)",
                        (title, stored, len(pages)),
                    )
                doc_id = int(cur.lastrowid)
                inserted_docs += 1

                for i, txt in enumerate(pages):
                    con.execute("INSERT OR REPLACE INTO pages(doc_id, page_no, text) VALUES (?,?,?)", (doc_id, i, txt or ""))
                    inserted_pages += 1

                for page_from, page_to, chunk in _chunk_text(pages):
                    con.execute(
                        "INSERT INTO chunks(doc_id, page_from, page_to, text) VALUES (?,?,?,?)",
                        (doc_id, page_from, page_to, chunk),
                    )
                    inserted_chunks += 1
            except Exception as e:
                failed_files.append({"file": str(pdf), "error": str(e)})
                continue

        con.commit()
    finally:
        con.close()

    return {
        "pdfs_scanned": len(files),
        "docs_inserted": inserted_docs,
        "pages_inserted": inserted_pages,
        "chunks_inserted": inserted_chunks,
        "skipped_existing": skipped,
        "failed_files": failed_files,
        "docs_db_path": str(docs_db_path),
    }
