from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

# NOTE:
# - PaddleOCR 초기화 비용이 매우 큽니다. 이미지마다 새로 생성하면 속도가 심각하게 느려집니다.
# - 따라서 프로세스 내에서 1회 생성 후 재사용합니다.

@dataclass
class OcrInfo:
    engine: str
    paddle_available: bool
    tesseract_available: bool
    details: str = ""


_PADDLE_OK: Optional[bool] = None
_PADDLE_ERR: str = ""
_PADDLE_OCR_CLS = None
_PADDLE_INSTANCE = None
_PADDLE_LANG: str = ""

_TESS_OK: Optional[bool] = None
_TESS_ERR: str = ""
_TESS_MOD = None


def _try_import_paddle() -> Tuple[bool, Optional[object], str]:
    global _PADDLE_OK, _PADDLE_ERR, _PADDLE_OCR_CLS
    if _PADDLE_OK is not None:
        return _PADDLE_OK, _PADDLE_OCR_CLS, _PADDLE_ERR
    try:
        from paddleocr import PaddleOCR  # type: ignore
        _PADDLE_OK = True
        _PADDLE_OCR_CLS = PaddleOCR
        _PADDLE_ERR = ""
        return True, PaddleOCR, ""
    except Exception as e:
        _PADDLE_OK = False
        _PADDLE_OCR_CLS = None
        _PADDLE_ERR = str(e)
        return False, None, str(e)


def _try_import_tesseract() -> Tuple[bool, Optional[object], str]:
    global _TESS_OK, _TESS_ERR, _TESS_MOD
    if _TESS_OK is not None:
        return _TESS_OK, _TESS_MOD, _TESS_ERR
    try:
        import pytesseract  # type: ignore
        _TESS_OK = True
        _TESS_MOD = pytesseract
        _TESS_ERR = ""
        return True, pytesseract, ""
    except Exception as e:
        _TESS_OK = False
        _TESS_MOD = None
        _TESS_ERR = str(e)
        return False, None, str(e)


def _get_paddle_instance() -> object:
    global _PADDLE_INSTANCE, _PADDLE_LANG
    p_ok, PaddleOCR, _ = _try_import_paddle()
    if not p_ok or PaddleOCR is None:
        raise RuntimeError("PaddleOCR not available")
    lang = (os.environ.get("PADDLE_LANG", "korean") or "korean").strip().lower()
    if _PADDLE_INSTANCE is None or _PADDLE_LANG != lang:
        # use_angle_cls helps with rotated scans
        _PADDLE_INSTANCE = PaddleOCR(use_angle_cls=True, lang=lang)
        _PADDLE_LANG = lang
    return _PADDLE_INSTANCE


def get_ocr_info() -> OcrInfo:
    engine = (os.environ.get("OCR_ENGINE", "auto") or "auto").strip().lower()
    p_ok, _, p_err = _try_import_paddle()
    t_ok, _, t_err = _try_import_tesseract()

    details = []
    if not p_ok and p_err:
        details.append(f"paddleocr import error: {p_err}")
    if not t_ok and t_err:
        details.append(f"pytesseract import error: {t_err}")

    return OcrInfo(
        engine=engine,
        paddle_available=p_ok,
        tesseract_available=t_ok,
        details="\n".join(details).strip(),
    )


def ocr_image(path: Path) -> str:
    """OCR a single image file.

    Selection:
      OCR_ENGINE=auto|paddle|tesseract
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(str(path))

    engine = (os.environ.get("OCR_ENGINE", "auto") or "auto").strip().lower()

    # Prefer Paddle if auto and available
    p_ok, _, p_err = _try_import_paddle()
    t_ok, pytesseract, t_err = _try_import_tesseract()

    if engine in ("auto", "paddle") and p_ok:
        ocr = _get_paddle_instance()
        result = ocr.ocr(str(path), cls=True)  # type: ignore[attr-defined]
        lines = []
        for page in result:
            for item in page:
                try:
                    lines.append(item[1][0])
                except Exception:
                    continue
        return "\n".join(lines).strip()

    if engine in ("auto", "tesseract") and t_ok and pytesseract is not None:
        # Configure tesseract command path if provided
        tess_cmd = os.environ.get("TESSERACT_CMD", "").strip()
        if tess_cmd:
            pytesseract.pytesseract.tesseract_cmd = tess_cmd

        lang = (os.environ.get("TESS_LANG", "kor+eng") or "kor+eng").strip()
        from PIL import Image  # type: ignore

        img = Image.open(str(path))
        config = os.environ.get("TESS_CONFIG", "--oem 3 --psm 6")
        text = pytesseract.image_to_string(img, lang=lang, config=config)
        return (text or "").strip()

    raise RuntimeError(
        "No OCR engine available. "
        f"OCR_ENGINE={engine}. "
        f"paddle_available={p_ok} ({p_err}); tesseract_available={t_ok} ({t_err})"
    )
