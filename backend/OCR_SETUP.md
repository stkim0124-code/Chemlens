# Local OCR setup (Windows-focused)

This project can ingest *image pages* (scanned PDFs / exported slides) by running OCR locally on your PC.

We support **two OCR engines**:

## Option A) PaddleOCR (recommended for Korean + mixed content)

1. In your backend venv:

```bash
pip install "paddleocr>=2.7" "paddlepaddle>=2.6" pillow opencv-python
```

2. Set env (backend/.env):

```ini
OCR_ENGINE=paddle
```

Pros: best KR/EN OCR, good layout handling.

## Option B) Tesseract (lighter, requires system install)

1. Install **Tesseract OCR** for Windows.
   - Install it and add the install directory (contains `tesseract.exe`) to PATH.

2. Make sure language data exists:
   - `eng.traineddata`
   - `kor.traineddata`

3. In your backend venv:

```bash
pip install pytesseract pillow opencv-python
```

4. Set env (backend/.env):

```ini
OCR_ENGINE=tesseract
TESSERACT_CMD=C:\Program Files\Tesseract-OCR\tesseract.exe
TESS_LANG=kor+eng
```

Pros: small footprint.

## Where to put image pages

Put page images under:

```
backend/app/data/images/
```

Supported extensions: `.png .jpg .jpeg .webp`

If you prefer another folder, set:

```ini
LABINT_IMAGES_DIR=C:\chemlens\backend\app\data\images
```

## API endpoints

### OCR sanity test

`GET /api/ocr/info`  → shows which engines are available.

`POST /api/ocr/test?filename=...`  → OCR a single image file under `LABINT_IMAGES_DIR`.

### Ingest from images into reaction_cards

`POST /api/ingest/from-images`

Example JSON body:

```json
{
  "mode": "both",
  "glob": "**/*.jpg",
  "max_images": 500,
  "max_cards": 20000
}
```

Modes:
- `procedure` (B): procedure-like blocks (highest value)
- `concept` (A): reaction headings / concept cards (coverage)
- `both`: do both
