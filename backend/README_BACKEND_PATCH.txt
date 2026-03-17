# Backend root patch (safety + reproducibility)

What changed
- requirements.txt: add pymupdf (PyMuPDF) needed by PDF ingestion endpoint
- run_backend.bat: make path portable (runs from this folder), consistent conda activation
- run_docs_sync.bat: now calls tools\sync_docs_db.py (you renamed fixed version to sync_docs_db.py)
- run_fix_docs_db.bat: make path portable

Security note
- Your real .env contains API keys. Do NOT commit it.
- This patch does not overwrite .env. Keep your existing .env locally.
- Use .env.ocr.example as a reference for OCR settings.
