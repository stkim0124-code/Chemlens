CHEMLENS Patch: Dataization Phase-1 "Text Search 봉인" + PDF OCR Fallback + Images->Docs OCR

What this patch does
1) PDF ingest now supports OCR fallback for scanned pages (optional, env-controlled)
   - Enable with: LABINT_PDF_OCR=1
   - Threshold: LABINT_PDF_OCR_MIN_CHARS (default 30)
   - Cap OCR pages per PDF: LABINT_PDF_OCR_MAX_PAGES (0=all)

2) Adds endpoint: POST /api/ingest/images-to-docs
   - OCR images under app/data/images into labint_docs.db (documents/pages/chunks+FTS)
   - This makes /api/docs/search cover scanned textbook image pages too.

Files changed/added
- app/pdf_ingest.py  (OCR fallback)
- app/image_ingest.py (NEW)
- app/main.py (NEW route + model)

How to run (Windows)
1) Put PDFs under: backend/app/data/pdfs/*.pdf
2) Put images under: backend/app/data/images/**/*
3) In backend/.env (or system env), set OCR + PDF OCR flags:
   OCR_ENGINE=auto
   LABINT_PDF_OCR=1
   LABINT_PDF_OCR_MIN_CHARS=30
   LABINT_PDF_OCR_MAX_PAGES=0

4) Double-click: run_backend.bat
5) Call endpoints (Swagger: http://localhost:8000/docs)
   - POST /api/ingest/pdfs      {"limit":0}
   - POST /api/ingest/images-to-docs {"limit":0}
   - GET  /api/docs/search?q=...  (FTS keyword search)

Notes
- This patch does NOT touch reaction_cards 구조검색 파이프라인 (SMILES 자동추출은 현실적으로 PDF에 거의 없어 후순위).
- 지금은 "검색이 된다"를 텍스트 기준으로 즉시 확보하는 단계입니다.
