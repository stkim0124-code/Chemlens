from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any

# -----------------------------
# Load .env (backend/.env) safely
# -----------------------------
# Why: API keys must stay on the server side (FastAPI), not in the React bundle.
# This will load variables like GEMINI_API_KEY, GEMINI_MODEL, LABINT_DB_PATH.
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None  # type: ignore

APP_DIR = Path(__file__).resolve().parent          # .../backend/app
BACKEND_DIR = APP_DIR.parent                      # .../backend
if load_dotenv is not None:
    load_dotenv(dotenv_path=str(BACKEND_DIR / ".env"), override=False)

import requests
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.ingest_utils import (
    extract_procedure_blocks,
    extract_concept_headings,
    build_procedure_cards,
    build_concept_cards,
    ExtractedCard,
    rel_top_folder_tag,
)

from app.ocr_utils import get_ocr_info, ocr_image
from app.docs_init import ensure_docs_db
from app.pdf_ingest import ingest_pdfs_to_docs_db
from app.image_ingest import ingest_images_to_docs_db


# -----------------------------
# RDKit
# -----------------------------
try:
    from rdkit import Chem
    from rdkit.Chem import AllChem, DataStructs
    from rdkit.Chem.Draw import rdMolDraw2D
except Exception as e:
    Chem = None  # type: ignore
    AllChem = None  # type: ignore
    DataStructs = None  # type: ignore
    rdMolDraw2D = None  # type: ignore
    _RDKit_IMPORT_ERROR = str(e)
else:
    _RDKit_IMPORT_ERROR = None

DB_PATH = Path(os.environ.get("LABINT_DB_PATH", str(APP_DIR / "labint.db")))

# -----------------------------
# Docs KB (PDF text index) - optional
# -----------------------------
DOCS_DB_PATH = Path(os.environ.get("LABINT_DOCS_DB_PATH", str(APP_DIR / "data" / "labint_docs.db")))
THUMBS_DIR = Path(os.environ.get("LABINT_THUMBS_DIR", str(APP_DIR / "data" / "thumbs")))
PDFS_DIR = Path(os.environ.get("LABINT_PDFS_DIR", str(APP_DIR / "data" / "pdfs")))
IMAGES_DIR = Path(os.environ.get("LABINT_IMAGES_DIR", str(APP_DIR / "data" / "images")))


def _pick_existing_thumb(doc_id: int) -> Optional[Path]:
    """Return first existing thumbnail path for a document.

    Historical filenames observed:
      - doc_{id}.png
      - doc{id}_p0.png
    """
    cands = [
        THUMBS_DIR / f"doc_{doc_id}.png",
        THUMBS_DIR / f"doc{doc_id}_p0.png",
        THUMBS_DIR / f"doc{doc_id}_p0.jpg",
        THUMBS_DIR / f"doc{doc_id}_p0.jpeg",
    ]
    for p in cands:
        if p.exists():
            return p
    return None


# Ensure docs DB schema exists (creates tables if missing)
try:
    ensure_docs_db(DOCS_DB_PATH)
except Exception as _e:
    # Don't crash server on startup; surface via /api/ocr_info etc.
    pass

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-1.5-pro").strip() or "gemini-1.5-pro"

app = FastAPI(title="Chemlens Backend")

def _parse_allowed_origins() -> List[str]:
    """Parse ALLOWED_ORIGINS env (comma-separated).

    Security rule:
    - Never default to "*". If env is missing, fall back to local dev origins.
    """
    raw = (os.environ.get("ALLOWED_ORIGINS") or "").strip()
    if raw:
        parts = [p.strip() for p in raw.split(",")]
        return [p for p in parts if p]
    # safe dev defaults
    return [
        "http://127.0.0.1:5173",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://localhost:3000",
    ]


app.add_middleware(
    CORSMiddleware,
    allow_origins=_parse_allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Pydantic models
# -----------------------------
class SearchRequest(BaseModel):
    smiles: str = Field(..., description="Query SMILES")
    top_k: int = Field(5, ge=1, le=200, description="Top-K hits to return")
    min_tanimoto: float = Field(0.25, ge=0.0, le=1.0, description="Minimum Tanimoto similarity")


class GeminiRequest(BaseModel):
    prompt: str = Field(..., description="User prompt for Gemini")


class IngestMolCardRequest(BaseModel):
    title: str
    transformation: str
    substrate_smiles: str
    product_smiles: str
    reagents: str = ""
    solvent: str = ""
    conditions: str = ""
    yield_pct: Optional[float] = None
    source: str = "user upload"
    notes: str = ""


class IngestFromDocsRequest(BaseModel):
    mode: str = Field("both", description="procedure|concept|both")
    doc_ids: Optional[List[int]] = Field(None, description="If omitted, ingest all documents")
    max_pages_per_doc: int = Field(200, ge=1, le=2000)
    max_cards: int = Field(5000, ge=1, le=200000)


class IngestFromImagesRequest(BaseModel):
    mode: str = Field("both", description="procedure|concept|both")
    glob: str = Field("**/*.png", description="Glob under LABINT_IMAGES_DIR")
    max_images: int = Field(500, ge=1, le=50000)
    max_cards: int = Field(20000, ge=1, le=500000)



class IngestPdfsRequest(BaseModel):
    limit: int = Field(0, ge=0, le=5000, description="0이면 전체 PDF, 1 이상이면 최대 N개만 인제스트")


class IngestImagesToDocsRequest(BaseModel):
    limit: int = Field(0, ge=0, le=200000, description="0이면 전체 이미지, 1 이상이면 최대 N개만 OCR 인제스트")

# -----------------------------
# Helpers
# -----------------------------
def _ensure_rdkit():
    if Chem is None:
        raise HTTPException(
            status_code=500,
            detail=f"RDKit import failed. Install via conda-forge on Windows. Details: {_RDKit_IMPORT_ERROR}",
        )


def db_connect():
    if not DB_PATH.exists():
        raise HTTPException(status_code=500, detail=f"DB not found: {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    # Improve concurrency + reduce "database is locked" in mixed read/write workloads.
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception:
        pass
    conn.row_factory = sqlite3.Row
    return conn



# -----------------------------
# Docs KB helpers (SQLite + FTS5)
# -----------------------------
def docs_db_connect():
    if not DOCS_DB_PATH.exists():
        raise HTTPException(status_code=500, detail=f"Docs DB not found: {DOCS_DB_PATH}")
    conn = sqlite3.connect(str(DOCS_DB_PATH))
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception:
        pass
    conn.row_factory = sqlite3.Row
    return conn


def docs_list_documents(limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    q = """
    SELECT id, title, file_path, page_count, created_at
    FROM documents
    ORDER BY id DESC
    LIMIT ? OFFSET ?
    """
    conn = docs_db_connect()
    rows = conn.execute(q, (limit, offset)).fetchall()
    conn.close()
    items = []
    for r in rows:
        doc_id = int(r["id"])
        thumb = None
        thumb_path = _pick_existing_thumb(doc_id)
        if thumb_path is not None:
            thumb = f"/api/docs/{doc_id}/thumb"
        items.append(
            {
                "id": doc_id,
                "title": r["title"],
                "file_path": r["file_path"],
                "page_count": r["page_count"],
                "created_at": r["created_at"],
                "thumb_url": thumb,
            }
        )
    return items


def docs_get_document(doc_id: int) -> Optional[Dict[str, Any]]:
    q = """
    SELECT id, title, file_path, page_count, created_at
    FROM documents
    WHERE id = ?
    """
    conn = docs_db_connect()
    r = conn.execute(q, (doc_id,)).fetchone()
    conn.close()
    if not r:
        return None
    thumb = None
    thumb_path = _pick_existing_thumb(doc_id)
    if thumb_path is not None:
        thumb = f"/api/docs/{doc_id}/thumb"
    return {
        "id": int(r["id"]),
        "title": r["title"],
        "file_path": r["file_path"],
        "page_count": r["page_count"],
        "created_at": r["created_at"],
        "thumb_url": thumb,
    }


def docs_get_page_text(doc_id: int, page_no: int) -> Optional[Dict[str, Any]]:
    q = """
    SELECT doc_id, page_no, text
    FROM pages
    WHERE doc_id = ? AND page_no = ?
    """
    conn = docs_db_connect()
    r = conn.execute(q, (doc_id, page_no)).fetchone()
    conn.close()
    return dict(r) if r else None


def docs_search_chunks(query: str, limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
    # FTS5 snippet(table, col, start, end, ellipsis, tokens)
    q = """
    SELECT
        c.id AS chunk_id,
        c.doc_id,
        c.page_from,
        c.page_to,
        snippet(chunks_fts, 0, '[', ']', ' … ', 12) AS snippet,
        d.title AS doc_title
    FROM chunks_fts
    JOIN chunks c ON c.id = chunks_fts.rowid
    JOIN documents d ON d.id = c.doc_id
    WHERE chunks_fts MATCH ?
    ORDER BY bm25(chunks_fts)
    LIMIT ? OFFSET ?
    """
    conn = docs_db_connect()
    try:
        rows = conn.execute(q, (query, limit, offset)).fetchall()
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=f"FTS query error: {e}")
    conn.close()
    return [dict(r) for r in rows]

def list_cards(limit: int = 200) -> List[Dict[str, Any]]:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, title, transformation, substrate_smiles, product_smiles, reagents, solvent, conditions, yield_pct, source, notes "
        "FROM reaction_cards ORDER BY id DESC LIMIT ?",
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "id": r["id"],
            "title": r["title"],
            "transformation": r["transformation"],
            "substrate_smiles": r["substrate_smiles"],
            "product_smiles": r["product_smiles"],
            "reagents": r["reagents"],
            "solvent": r["solvent"],
            "conditions": r["conditions"],
            "yield_pct": r["yield_pct"],
            "source": r["source"],
            "notes": r["notes"],
        }
        for r in rows
    ]


def _insert_cards(cards: List[ExtractedCard]) -> Dict[str, int]:
    """Insert extracted cards into reaction_cards, de-duplicating by (source, notes tag)."""
    if not cards:
        return {"inserted": 0, "skipped": 0}
    conn = db_connect()
    cur = conn.cursor()
    inserted = 0
    skipped = 0
    for c in cards:
        tag = (c.notes.split("\n", 1)[0] if c.notes else "")
        if tag:
            exists = cur.execute(
                "SELECT 1 FROM reaction_cards WHERE source = ? AND notes LIKE ? LIMIT 1",
                (c.source, f"{tag}%"),
            ).fetchone()
            if exists:
                skipped += 1
                continue
        cur.execute(
            """
            INSERT INTO reaction_cards
            (title, transformation, substrate_smiles, product_smiles, reagents, solvent, conditions, yield_pct, source, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                c.title,
                c.transformation,
                c.substrate_smiles,
                c.product_smiles,
                c.reagents,
                c.solvent,
                c.conditions,
                c.yield_pct,
                c.source,
                c.notes,
            ),
        )
        inserted += 1
    conn.commit()
    conn.close()
    return {"inserted": inserted, "skipped": skipped}


def fp(mol):
    _ensure_rdkit()
    return AllChem.GetMorganFingerprintAsBitVect(mol, 2, nBits=2048)


def tanimoto(fp1, fp2) -> float:
    _ensure_rdkit()
    return float(DataStructs.TanimotoSimilarity(fp1, fp2))


def mol_to_svg(smiles: str, w: int = 220, h: int = 180) -> Optional[str]:
    """Render SMILES -> SVG text (no file I/O)."""
    _ensure_rdkit()
    if not smiles:
        return None
    m = Chem.MolFromSmiles(smiles)
    if m is None:
        return None
    drawer = rdMolDraw2D.MolDraw2DSVG(w, h)
    drawer.DrawMolecule(m)
    drawer.FinishDrawing()
    return drawer.GetDrawingText()


# -----------------------------
# Routes
# -----------------------------

# -----------------------------
# Docs KB routes (/api/docs/*)
# -----------------------------
@app.get("/api/docs")
def api_docs_list(limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)):
    return docs_list_documents(limit=limit, offset=offset)


@app.get("/api/docs/search")
def api_docs_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    hits = docs_search_chunks(query=q, limit=limit, offset=offset)
    return {"query": q, "total": len(hits), "hits": hits}


@app.get("/api/docs/{doc_id}")
def api_docs_get(doc_id: int):
    d = docs_get_document(doc_id)
    if not d:
        raise HTTPException(status_code=404, detail="Document not found")
    return d


@app.get("/api/docs/{doc_id}/page/{page_no}")
def api_docs_get_page(doc_id: int, page_no: int):
    p = docs_get_page_text(doc_id, page_no)
    if not p:
        raise HTTPException(status_code=404, detail="Page not found")
    return p


@app.get("/api/docs/{doc_id}/thumb")
def api_docs_thumb(doc_id: int):
    thumb_path = _pick_existing_thumb(doc_id)
    if thumb_path is None:
        raise HTTPException(status_code=404, detail="Thumbnail not found")
    # infer media type
    mt = "image/png" if thumb_path.suffix.lower()==".png" else "image/jpeg"
    resp = FileResponse(str(thumb_path), media_type=mt)
    resp.headers["Cross-Origin-Resource-Policy"] = "cross-origin"
    return resp



@app.get("/api/docs/{doc_id}/pdf")
def api_docs_pdf(doc_id: int):
    """Serve original PDF file for a document.

    Resolution rule:
    - Use documents.file_path basename from docs DB
    - Look for it under PDFS_DIR
    """
    d = docs_get_document(doc_id)
    if not d:
        raise HTTPException(status_code=404, detail="Document not found")

    file_path = (d.get("file_path") or "").strip()
    # file_path may be stored as:
    #   - "pdfs/<name>.pdf"
    #   - "pdfs/<name>.pdf#<doc_id>" (uniqueness suffix)
    #   - "<name>.pdf"
    #   - absolute path (legacy)
    fp = file_path.split("#", 1)[0] if file_path else ""
    # Normalize Windows backslashes to URL-style slashes.
    fp = fp.replace("\\", "/")
    # Strip portable prefix if present
    fp = fp.split("/", 1)[-1] if fp.lower().startswith("pdfs/") else fp
    filename = os.path.basename(fp) if fp else ""
    if not filename:
        raise HTTPException(status_code=404, detail="No file_path in docs DB for this document")

    pdf_path = PDFS_DIR / filename
    if not pdf_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"PDF not found. Expected: {pdf_path} (put the original PDF under backend/app/data/pdfs/)",
        )

    resp = FileResponse(str(pdf_path), media_type="application/pdf", filename=filename)
    resp.headers["Cross-Origin-Resource-Policy"] = "cross-origin"
    return resp


# -----------------------------
# OCR routes
# -----------------------------
@app.get("/api/ocr/info")
def api_ocr_info():
    info = get_ocr_info()
    return {
        "ok": True,
        "engine": info.engine,
        "paddle_available": info.paddle_available,
        "tesseract_available": info.tesseract_available,
        "images_dir": str(IMAGES_DIR),
        "details": info.details,
    }


@app.post("/api/ocr/test")
def api_ocr_test(filename: str = Query(..., min_length=1)):
    """OCR a single image under LABINT_IMAGES_DIR (quick sanity test)."""
    p = (IMAGES_DIR / filename).resolve()
    if IMAGES_DIR.resolve() not in p.parents and p != IMAGES_DIR.resolve():
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not p.exists():
        raise HTTPException(status_code=404, detail=f"Image not found: {p}")
    try:
        text = ocr_image(p)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OCR failed: {e}")
    return {"ok": True, "filename": filename, "chars": len(text), "text": text[:4000]}

# Optional: /api/health alias for frontends expecting /api/*
@app.get("/api/health")
def api_health_alias():
    return health()
@app.get("/health")
def health():
    # Summarize server readiness for the React UI
    docs_ok = DOCS_DB_PATH.exists()
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.execute("SELECT count(*) FROM reaction_cards")
        cards_count = int(cur.fetchone()[0])
        conn.close()
    except Exception:
        cards_count = None
    try:
        conn = sqlite3.connect(str(DOCS_DB_PATH))
        cur = conn.execute("SELECT count(*) FROM documents")
        docs_count = int(cur.fetchone()[0])
        conn.close()
    except Exception:
        docs_count = None
    o = get_ocr_info()
    return {
        "ok": True,
        "db_path": str(DB_PATH),
        "cards_count": cards_count,
        "docs_db_path": str(DOCS_DB_PATH),
        "docs_count": docs_count,
        "thumbs_dir": str(THUMBS_DIR),
        "pdfs_dir": str(PDFS_DIR),
        "images_dir": str(IMAGES_DIR),
        "rdkit_ok": Chem is not None,
        "rdkit_error": _RDKit_IMPORT_ERROR,
        "ocr": {
            "engine": o.engine,
            "paddle_available": o.paddle_available,
            "tesseract_available": o.tesseract_available,
            "details": o.details,
        },
    }


@app.get("/cards")
@app.get("/api/cards")
def api_cards(limit: int = 200):
    return {"items": list_cards(limit=limit)}


@app.get("/render/svg")
def render_svg(smiles: str, w: int = 260, h: int = 200):
    """SMILES를 SVG로 단독 렌더 (프론트 미리보기용)"""
    svg = mol_to_svg(smiles, w=w, h=h)
    if svg is None:
        raise HTTPException(status_code=400, detail="Invalid SMILES")
    return {"svg": svg}


@app.post("/search")
@app.post("/api/search")
def api_search(req: SearchRequest):
    """
    hits[] 원소는 Streamlit 카드와 1:1 동일 필드로 반환:
      id, title, similarity,
      transformation, reagents, solvent, conditions, yield_pct, source, notes,
      substrate_smiles, product_smiles,
      substrate_svg, product_svg
    """
    _ensure_rdkit()

    qm = Chem.MolFromSmiles(req.smiles)
    if qm is None:
        raise HTTPException(status_code=400, detail="Invalid SMILES")
    q_smi = Chem.MolToSmiles(qm, canonical=True)
    qfp = fp(qm)

    cards = list_cards(limit=5000)

    hits: List[Dict[str, Any]] = []
    for c in cards:
        sub = (c.get("substrate_smiles") or "").strip()
        prod = (c.get("product_smiles") or "").strip()

        # substrate가 비었으면 product도 후보로 사용
        best_sim = -1.0

        if sub:
            m = Chem.MolFromSmiles(sub)
            if m:
                best_sim = max(best_sim, tanimoto(qfp, fp(m)))

        if prod:
            m = Chem.MolFromSmiles(prod)
            if m:
                best_sim = max(best_sim, tanimoto(qfp, fp(m)))

        if best_sim < 0:
            continue

        if best_sim >= req.min_tanimoto:
            hits.append(
                {
                    **c,
                    "similarity": float(best_sim),
                    "substrate_svg": mol_to_svg(sub) if sub else None,
                    "product_svg": mol_to_svg(prod) if prod else None,
                }
            )

    hits.sort(key=lambda x: x["similarity"], reverse=True)
    return {"query_canonical_smiles": q_smi, "hits": hits[: req.top_k]}


def _call_gemini(prompt: str, system_instruction: str) -> str:
    if not GEMINI_API_KEY:
        raise HTTPException(status_code=400, detail="GEMINI_API_KEY is not set on the server")

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "systemInstruction": {"parts": [{"text": system_instruction}]},
    }
    r = requests.post(url, json=payload, timeout=60)
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Gemini API error: {r.status_code} {r.text}")

    data = r.json()
    try:
        return data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception:
        raise HTTPException(status_code=502, detail=f"Gemini API unexpected response: {data}")


@app.post("/gemini")
def api_gemini(req: GeminiRequest):
    system = "You are a chemistry assistant. Answer concisely and accurately."
    text = _call_gemini(req.prompt, system_instruction=system)
    return {"text": text}


@app.post("/upload/mol")
async def upload_mol(file: UploadFile = File(...)):
    """MOL 업로드 → canonical SMILES 반환"""
    _ensure_rdkit()
    content = await file.read()
    try:
        mol = Chem.MolFromMolBlock(content.decode("utf-8", errors="ignore"), sanitize=True)
    except Exception:
        mol = None

    if mol is None:
        raise HTTPException(status_code=400, detail="Invalid MOL file")

    smi = Chem.MolToSmiles(mol, canonical=True)
    return {"smiles": smi}


@app.post("/ingest/mol-card")
def ingest_mol_card(req: IngestMolCardRequest):
    """React에서 DB에 카드 추가(프로토)"""
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO reaction_cards
           (title, transformation, substrate_smiles, product_smiles, reagents, solvent, conditions, yield_pct, source, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            req.title,
            req.transformation,
            req.substrate_smiles,
            req.product_smiles,
            req.reagents,
            req.solvent,
            req.conditions,
            req.yield_pct,
            req.source,
            req.notes,
        ),
    )
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return {"ok": True, "id": new_id}



@app.post("/api/ingest/images-to-docs")
def ingest_images_to_docs(req: IngestImagesToDocsRequest):
    """OCR images under LABINT_IMAGES_DIR into labint_docs.db (documents/pages/chunks+FTS).

    This makes /api/docs/search cover scanned textbook images too.
    """
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    ensure_docs_db(DOCS_DB_PATH)
    try:
        res = ingest_images_to_docs_db(IMAGES_DIR, DOCS_DB_PATH, limit=req.limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"ok": True, **res}

@app.post("/api/ingest/pdfs")
def ingest_pdfs(req: IngestPdfsRequest):
    """
    Ingest PDFs under LABINT_PDFS_DIR into labint_docs.db:
      documents, pages, chunks(+FTS).
    Requires PyMuPDF (pymupdf). If not installed, returns a clear error.
    """
    PDFS_DIR.mkdir(parents=True, exist_ok=True)
    ensure_docs_db(DOCS_DB_PATH)
    try:
        res = ingest_pdfs_to_docs_db(PDFS_DIR, DOCS_DB_PATH, limit=req.limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"ok": True, **res}

@app.post("/api/ingest/from-docs")
def ingest_from_docs(req: IngestFromDocsRequest):
    """Create reaction_cards from the PDF text KB (labint_docs.db).

    Modes:
      - procedure: extract procedure-like paragraphs (best quality)
      - concept: extract reaction-name headings (coverage)
      - both: do both
    """
    mode = (req.mode or "both").lower().strip()
    if mode not in {"procedure", "concept", "both"}:
        raise HTTPException(status_code=400, detail="mode must be procedure|concept|both")

    # Pick documents
    doc_rows = docs_list_documents(limit=100000, offset=0)
    if req.doc_ids:
        wanted = set(int(x) for x in req.doc_ids)
        doc_rows = [d for d in doc_rows if int(d["id"]) in wanted]

    total_inserted = 0
    total_skipped = 0
    scanned_pages = 0
    produced = 0

    conn = docs_db_connect()
    try:
        for d in doc_rows:
            doc_id = int(d["id"])
            doc_title = d.get("title") or f"doc_{doc_id}"

            rows = conn.execute(
                "SELECT page_no, text FROM pages WHERE doc_id = ? ORDER BY page_no ASC LIMIT ?",
                (doc_id, req.max_pages_per_doc),
            ).fetchall()

            for r in rows:
                page_no = int(r["page_no"])
                text = r["text"] or ""
                scanned_pages += 1

                cards: List[ExtractedCard] = []
                rdkit_ok = Chem is not None
                if mode in {"procedure", "both"}:
                    blocks = extract_procedure_blocks(text)
                    cards.extend(build_procedure_cards(doc_title, page_no, blocks, rdkit_available=rdkit_ok))
                if mode in {"concept", "both"}:
                    headings = extract_concept_headings(text)
                    cards.extend(build_concept_cards(doc_title, page_no, headings, rdkit_available=rdkit_ok))

                if cards:
                    produced += len(cards)
                    res = _insert_cards(cards)
                    total_inserted += res["inserted"]
                    total_skipped += res["skipped"]

                if produced >= req.max_cards:
                    break
            if produced >= req.max_cards:
                break
    finally:
        conn.close()

    return {
        "ok": True,
        "mode": mode,
        "documents": len(doc_rows),
        "scanned_pages": scanned_pages,
        "produced_cards": produced,
        "inserted": total_inserted,
        "skipped": total_skipped,
        "db_path": str(DB_PATH),
        "docs_db_path": str(DOCS_DB_PATH),
    }


@app.post("/api/ingest/all")
def ingest_all(pdfs_limit: int = 0, images_limit: int = 500):
    """
    Convenience: ingest PDFs -> docs DB, then build cards from docs and images OCR.
    Order:
      1) /api/ingest/pdfs
      2) /api/ingest/from-docs (procedure+concept)
      3) /api/ingest/from-images (procedure+concept)
    """
    errors = []
    try:
        res_pdfs = ingest_pdfs(IngestPdfsRequest(limit=pdfs_limit))
    except Exception as e:
        res_pdfs = {"ok": False, "error": str(e)}
        errors.append({"step": "pdfs", "error": str(e)})
    try:
        res_docs = ingest_from_docs(IngestFromDocsRequest(mode="both", doc_ids=None, max_pages_per_doc=200, max_cards=200000))
    except Exception as e:
        res_docs = {"ok": False, "error": str(e)}
        errors.append({"step": "from_docs", "error": str(e)})
    try:
        res_imgs = ingest_from_images(IngestFromImagesRequest(mode="both", glob="**/*.*", max_images=images_limit, max_cards=200000))
    except Exception as e:
        res_imgs = {"ok": False, "error": str(e)}
        errors.append({"step": "from_images", "error": str(e)})
    return {"ok": len(errors) == 0, "pdfs": res_pdfs, "from_docs": res_docs, "from_images": res_imgs, "errors": errors}


@app.post("/api/ingest/from-images")
def ingest_from_images(req: IngestFromImagesRequest):
    """Create reaction_cards from OCR'd image pages under LABINT_IMAGES_DIR.

    This is for scanned PDFs / slide exports where text is not available in labint_docs.db.
    """
    mode = (req.mode or "both").lower().strip()
    if mode not in {"procedure", "concept", "both"}:
        raise HTTPException(status_code=400, detail="mode must be procedure|concept|both")

    # Ensure images dir exists (user can drop images here)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    # gather images
    exts = {".png", ".jpg", ".jpeg", ".webp"}
    files = [p for p in IMAGES_DIR.glob(req.glob) if p.is_file() and p.suffix.lower() in exts]
    files.sort(key=lambda x: str(x))
    files = files[: req.max_images]

    scanned_images = 0
    produced = 0
    total_inserted = 0
    total_skipped = 0
    failed = 0

    for img_path in files:
        scanned_images += 1
        try:
            text = ocr_image(img_path)
        except Exception:
            failed += 1
            continue

        # Use top folder as doc_title if available
        doc_title = rel_top_folder_tag(IMAGES_DIR, img_path) or img_path.stem
        # Try to parse trailing _NN page index
        page_no = 0
        m = re.search(r"_([0-9]{1,4})$", img_path.stem)
        if m:
            try:
                page_no = max(0, int(m.group(1)) - 1)
            except Exception:
                page_no = 0
        cards: List[ExtractedCard] = []
        rdkit_ok = Chem is not None
        if mode in {"procedure", "both"}:
            blocks = extract_procedure_blocks(text)
            cards.extend(build_procedure_cards(doc_title, page_no, blocks, rdkit_available=rdkit_ok))
        if mode in {"concept", "both"}:
            headings = extract_concept_headings(text)
            cards.extend(build_concept_cards(doc_title, page_no, headings, rdkit_available=rdkit_ok))

        # Strengthen source to point to image filename
        for c in cards:
            c.source = f"{img_path.name}"

        if cards:
            produced += len(cards)
            res = _insert_cards(cards)
            total_inserted += res["inserted"]
            total_skipped += res["skipped"]

        if produced >= req.max_cards:
            break

    return {
        "ok": True,
        "mode": mode,
        "images_dir": str(IMAGES_DIR),
        "glob": req.glob,
        "scanned_images": scanned_images,
        "ocr_failed": failed,
        "produced_cards": produced,
        "inserted": total_inserted,
        "skipped": total_skipped,
        "db_path": str(DB_PATH),
    }
