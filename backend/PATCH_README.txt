BACKEND PATCH (PDFs + Images ingest)

What this patch fixes / adds
- Fixes ingest_utils: provides ExtractedCard + builders that app.main expects.
- Adds docs DB schema init (documents/pages/chunks + FTS5).
- Adds /api/ingest/pdfs endpoint:
    - reads PDFs under backend/app/data/pdfs
    - extracts per-page text with PyMuPDF (pymupdf)
    - stores into docs DB (backend/app/data/labint_docs.db by default)
- Improves /api/ingest/from-images:
    - uses top folder name as doc_title
    - parses trailing _NN in filename for page index
- Adds /api/ingest/all convenience endpoint.

How to apply (NO manual editing)
1) Unzip this patch.
2) Copy/merge into your backend folder, overwriting:
   - backend/app/main.py
   - backend/app/ingest_utils.py
   and adding:
   - backend/app/docs_init.py
   - backend/app/pdf_ingest.py
   - backend/requirements.txt (adds pymupdf)

3) Install deps (Windows):
   cd C:\chemlens\backend
   pip install -r requirements.txt

4) Place your files:
   - backend/app/data/pdfs/*.pdf
   - backend/app/data/images/** (can have subfolders)

5) Run backend:
   uvicorn app.main:app --reload --port 8000

6) Ingest:
   - POST http://127.0.0.1:8000/api/ingest/pdfs   body: {"limit":0}
   - POST http://127.0.0.1:8000/api/ingest/from-docs body: {"mode":"both"}
   - POST http://127.0.0.1:8000/api/ingest/from-images body: {"mode":"both","glob":"**/*.*"}
   or just:
   - POST http://127.0.0.1:8000/api/ingest/all?pdfs_limit=0&images_limit=500

Notes
- Image OCR requires PaddleOCR or pytesseract + system tesseract (see OCR_SETUP.md).
- If PyMuPDF is missing, /api/ingest/pdfs will return a clear install error.
