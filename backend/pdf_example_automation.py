from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import re
import sqlite3
import sys
import time
import getpass
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlsplit, urlunsplit

try:
    import fitz  # PyMuPDF
except Exception as e:
    raise SystemExit("PyMuPDF is required. Install/enable pymupdf in the chemlens env.") from e

try:
    import requests
except Exception:
    requests = None

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

DEFAULT_DB = Path("app") / "labint.db"
DEFAULT_PDF = Path("app") / "data" / "pdfs" / "named reactions.pdf"
DEFAULT_REPORTS = Path("reports") / "pdf_example_automation"
DEFAULT_STAGE_DB = Path("app") / "labint_pdf_examples_stage.db"

PROMPT = """You are extracting ONE synthetic application example region from a named-reaction textbook page.
The crop contains exactly one recent synthetic example when possible, but may occasionally contain two if the layout is dense.
Be conservative.

Rules:
1. Output ONE JSON object only. No markdown. No prose.
2. If chemical structures are not readable enough for confident exact SMILES, set reactant_smiles/product_smiles to null and smiles_confidence to 0.0.
3. Prefer text evidence over invented structure strings.
4. Preserve target/natural-product names when visible.
5. The crop is from the Synthetic Applications page, so extract_kind must be \"application_example\".

Return this schema:
{
  "family_name": "<string>",
  "region_confidence": 0.0,
  "example_target_name": null,
  "example_summary": null,
  "extracts": [
    {
      "extract_kind": "application_example",
      "reaction_family_name": "<string>",
      "transformation_text": null,
      "reactants_text": null,
      "products_text": null,
      "intermediates_text": null,
      "reagents_text": null,
      "catalysts_text": null,
      "solvents_text": null,
      "temperature_text": null,
      "time_text": null,
      "yield_text": null,
      "workup_text": null,
      "conditions_text": null,
      "notes_text": null,
      "reactant_smiles": null,
      "product_smiles": null,
      "smiles_confidence": 0.0,
      "extraction_confidence": 0.0
    }
  ]
}
"""


@dataclass
class PageRow:
    page_knowledge_id: int
    family_name: str
    title: str
    book_page_no: int


@dataclass
class RegionRow:
    family_name: str
    page_knowledge_id: int
    book_page_no: int
    pdf_page_no: int
    region_index: int
    x1: int
    y1: int
    x2: int
    y2: int
    paragraph_text: str
    nearby_text: str
    crop_path: str


def now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def title_case(text: str) -> str:
    lower_words = {"a","an","the","and","or","of","in","on","at","to","for","by","via","with"}
    words = text.split()
    out = []
    for i, w in enumerate(words):
        if "-" in w:
            parts = [p.capitalize() if (j == 0 or p.lower() not in lower_words) else p.lower() for j, p in enumerate(w.split("-"))]
            out.append("-".join(parts))
        else:
            out.append(w.capitalize() if (i == 0 or w.lower() not in lower_words) else w.lower())
    return " ".join(out)


def ensure_stage_db(path: Path) -> None:
    con = sqlite3.connect(path)
    try:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS pdf_example_pages (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              page_knowledge_id INTEGER NOT NULL,
              family_name TEXT NOT NULL,
              family_name_norm TEXT NOT NULL,
              book_page_no INTEGER NOT NULL,
              pdf_page_no INTEGER NOT NULL,
              title TEXT,
              report_run TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT (datetime('now')),
              UNIQUE(page_knowledge_id, report_run)
            );

            CREATE TABLE IF NOT EXISTS pdf_example_regions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              page_knowledge_id INTEGER NOT NULL,
              family_name TEXT NOT NULL,
              family_name_norm TEXT NOT NULL,
              book_page_no INTEGER NOT NULL,
              pdf_page_no INTEGER NOT NULL,
              region_index INTEGER NOT NULL,
              x1 INTEGER NOT NULL,
              y1 INTEGER NOT NULL,
              x2 INTEGER NOT NULL,
              y2 INTEGER NOT NULL,
              paragraph_text TEXT,
              nearby_text TEXT,
              crop_path TEXT,
              report_run TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT (datetime('now')),
              UNIQUE(page_knowledge_id, region_index, report_run)
            );

            CREATE TABLE IF NOT EXISTS pdf_example_extractions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              page_knowledge_id INTEGER NOT NULL,
              family_name TEXT NOT NULL,
              family_name_norm TEXT NOT NULL,
              book_page_no INTEGER NOT NULL,
              pdf_page_no INTEGER NOT NULL,
              region_index INTEGER NOT NULL,
              model_name TEXT,
              prompt_version TEXT,
              status TEXT NOT NULL,
              example_target_name TEXT,
              example_summary TEXT,
              region_confidence REAL,
              raw_json TEXT,
              error_message TEXT,
              report_run TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT (datetime('now')),
              UNIQUE(page_knowledge_id, region_index, report_run)
            );
            """
        )
        con.commit()
    finally:
        con.close()


def detect_named_reaction_offset(doc: fitz.Document, con: sqlite3.Connection) -> int:
    row = con.execute(
        """
        SELECT page_no, reference_family_name
        FROM manual_page_knowledge
        WHERE page_kind = 'canonical_overview'
          AND reference_family_name IS NOT NULL
        ORDER BY page_no ASC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return 52
    book_page_no = int(row[0])
    fam = row[1]
    fam_upper = fam.upper()
    for idx in range(min(doc.page_count, 120)):
        text = doc.load_page(idx).get_text("text") or ""
        if fam_upper in text.upper() and "Importance:" in text:
            pdf_page_no = idx + 1
            return pdf_page_no - book_page_no
    return 52


def fetch_application_pages(con: sqlite3.Connection, families: Optional[Sequence[str]] = None, limit_pages: int = 0) -> List[PageRow]:
    sql = (
        "SELECT id, reference_family_name, title, page_no "
        "FROM manual_page_knowledge "
        "WHERE page_kind = 'application_example' AND reference_family_name IS NOT NULL"
    )
    params: List[Any] = []
    if families:
        placeholders = ",".join("?" for _ in families)
        sql += f" AND reference_family_name IN ({placeholders})"
        params.extend(families)
    sql += " ORDER BY page_no ASC"
    rows = [PageRow(int(r[0]), r[1], r[2] or r[1], int(r[3])) for r in con.execute(sql, params).fetchall()]
    if limit_pages:
        rows = rows[:limit_pages]
    return rows


def is_paragraph_block(block: Tuple[float, float, float, float, str], page_width: float, heading_y: float) -> bool:
    x0, y0, x1, y1, txt = block
    width = x1 - x0
    if y0 <= heading_y + 5:
        return False
    if width < page_width * 0.72:
        return False
    if x0 > page_width * 0.20:
        return False
    if len(txt) < 70:
        return False
    if txt.upper() == txt and len(txt.split()) < 12:
        return False
    if txt.strip().startswith("Mechanism"):
        return False
    return True


def page_text_blocks(page: fitz.Page) -> List[Tuple[float, float, float, float, str]]:
    blocks = []
    for b in page.get_text("blocks"):
        x0, y0, x1, y1, text, *_ = b
        txt = " ".join((text or "").split())
        if txt:
            blocks.append((float(x0), float(y0), float(x1), float(y1), txt))
    return blocks


def find_example_regions(page: fitz.Page, family_name: str) -> List[Tuple[int, int, int, int, str, str]]:
    blocks = page_text_blocks(page)
    heading = next((b for b in blocks if "Synthetic Applications" in b[4]), None)
    heading_y = heading[1] if heading else 50.0
    paragraphs = [b for b in blocks if is_paragraph_block(b, page.rect.width, heading_y)]
    paragraphs.sort(key=lambda x: (x[1], x[0]))
    regions = []
    if not paragraphs:
        return regions
    page_w = int(page.rect.width)
    page_h = int(page.rect.height)
    for i, (x0, y0, x1, y1, txt) in enumerate(paragraphs):
        next_y0 = paragraphs[i + 1][1] if i + 1 < len(paragraphs) else page_h - 10
        top = max(int(y0) - 10, int(heading_y) + 10)
        bottom = min(int(next_y0) - 10, page_h - 8)
        if bottom - top < 120:
            continue
        left = 55
        right = page_w - 50
        region_blocks = [t for bx0, by0, bx1, by1, t in blocks if by0 >= top - 5 and by1 <= bottom + 5 and bx0 >= 40 and bx1 <= page_w - 20]
        nearby = " ".join(region_blocks)
        regions.append((left, top, right, bottom, txt, nearby))
    return regions


def render_crop(page: fitz.Page, bbox: Tuple[int, int, int, int], out_path: Path, dpi: int = 160) -> None:
    x1, y1, x2, y2 = bbox
    clip = fitz.Rect(x1, y1, x2, y2)
    pix = page.get_pixmap(dpi=dpi, clip=clip, alpha=False)
    pix.save(out_path)


PLACEHOLDER_API_KEY_PATTERNS = [
    "여기에_본인_api",
    "your_api_key",
    "replace_with",
    "replace-me",
    "replace_me",
    "paste_your",
    "api key here",
    "dummy",
    "example",
]


def is_placeholder_api_key(val: Optional[str]) -> bool:
    if not val:
        return True
    raw = val.strip()
    if not raw:
        return True
    lowered = raw.lower()
    if any(pat in lowered for pat in PLACEHOLDER_API_KEY_PATTERNS):
        return True
    if raw in {"YOUR_API_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY"}:
        return True
    return False


def load_api_key_from_env() -> Optional[str]:
    if load_dotenv is not None:
        load_dotenv()
    for key in ["GEMINI_API_KEY", "GOOGLE_API_KEY"]:
        val = os.environ.get(key)
        if val and not is_placeholder_api_key(val):
            return val.strip()
    return None


def prompt_for_api_key() -> Optional[str]:
    if not sys.stdin or not sys.stdin.isatty():
        return None
    try:
        val = getpass.getpass("Enter Gemini API key (input hidden): ").strip()
    except Exception:
        return None
    if is_placeholder_api_key(val):
        return None
    return val


def resolve_api_key(args: argparse.Namespace) -> Optional[str]:
    if args.api_key and not is_placeholder_api_key(args.api_key):
        return args.api_key.strip()
    if args.prompt_api_key:
        prompted = prompt_for_api_key()
        if prompted:
            os.environ["GEMINI_API_KEY"] = prompted
            return prompted
        return None
    env_key = load_api_key_from_env()
    if env_key:
        return env_key
    # Auto-fallback: if interactive prompt is available, ask the user instead of failing silently.
    prompted = prompt_for_api_key()
    if prompted:
        os.environ["GEMINI_API_KEY"] = prompted
        return prompted
    return None




def _compact_text(text: str, limit: int) -> str:
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= limit:
        return text
    cut = text[:limit]
    # try to cut at last sentence-ish boundary or space
    for sep in [". ", "; ", ": ", ", ", " "]:
        idx = cut.rfind(sep)
        if idx >= int(limit * 0.6):
            cut = cut[: idx + len(sep)].strip()
            break
    return cut + " ..."


def build_lean_user_text(family_name: str, paragraph_text: str, nearby_text: str) -> str:
    para = _compact_text(paragraph_text, 500)
    # Use only a very short OCR hint. Large OCR blobs likely inflate TPM and trigger rate limits.
    nearby = _compact_text(nearby_text, 700)
    parts = [
        f"Family name: {family_name}",
        "Task: extract ONE main synthetic application example from this crop.",
    ]
    if para:
        parts.append(f"Paragraph hint: {para}")
    if nearby:
        parts.append(f"Short OCR hint: {nearby}")
    parts.append("Important: ignore minor side steps; pick the main example only.")
    return "\n".join(parts)


def sanitize_error_message(message: str, api_key: Optional[str] = None) -> str:
    msg = str(message or "")
    if api_key:
        msg = msg.replace(api_key, "<REDACTED>")
    msg = re.sub(r"([?&]key=)[^&\s]+", r"\1<REDACTED>", msg)
    msg = re.sub(r"(x-goog-api-key[:=]\s*)[^,;\s]+", r"\1<REDACTED>", msg, flags=re.IGNORECASE)
    return msg


def _response_preview(resp: Any, limit: int = 240) -> str:
    try:
        text = resp.text or ""
    except Exception:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > limit:
        return text[:limit] + " ..."
    return text


def _exception_status_code(exc: Exception) -> Optional[int]:
    response = getattr(exc, "response", None)
    if response is not None:
        return getattr(response, "status_code", None)
    return None


def _is_retryable_transport_error(exc: Exception) -> bool:
    if requests is None:
        return False
    try:
        timeout_types = (requests.exceptions.Timeout,)
        connection_types = (requests.exceptions.ConnectionError,)
    except Exception:
        return False
    if isinstance(exc, timeout_types + connection_types):
        return True
    msg = str(exc or "").lower()
    return (
        "read timed out" in msg
        or "timed out" in msg
        or "connection reset" in msg
        or "connection aborted" in msg
        or "temporary failure" in msg
    )


def _build_gemini_url(model: str, api_auth: str, api_key: str) -> str:
    base = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    if api_auth == "query":
        return f"{base}?key={api_key}"
    return base


def gemini_call(
    api_key: str,
    model: str,
    family_name: str,
    paragraph_text: str,
    nearby_text: str,
    image_path: Path,
    timeout_s: int = 180,
    max_retries: int = 2,
    retry_initial_sleep: float = 20.0,
    retry_backoff: float = 2.0,
    retry_statuses: Optional[Sequence[int]] = None,
    api_auth: str = "header",
) -> Dict[str, Any]:
    if requests is None:
        raise RuntimeError("requests is required for Gemini mode")
    retry_statuses = tuple(int(x) for x in (retry_statuses or (429, 500, 503)))
    with image_path.open("rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    user_text = build_lean_user_text(family_name, paragraph_text, nearby_text)
    print(f"    [PAYLOAD] {family_name}: prompt_chars={len(user_text)} image_file={image_path.name}")
    body = {
        "contents": [{
            "parts": [
                {"text": PROMPT},
                {"text": user_text},
                {"inline_data": {"mime_type": "image/png", "data": b64}},
            ]
        }],
        "generationConfig": {
            "temperature": 0.1,
            "topP": 0.9,
            "responseMimeType": "application/json",
        },
    }
    headers = {"Content-Type": "application/json"}
    if api_auth == "header":
        headers["x-goog-api-key"] = api_key
    url = _build_gemini_url(model, api_auth, api_key)
    attempts = max(1, int(max_retries) + 1)
    sleep_s = max(0.0, float(retry_initial_sleep))

    last_err: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            r = requests.post(url, json=body, headers=headers, timeout=(30, timeout_s))
            if r.status_code >= 400:
                preview = _response_preview(r)
                if preview:
                    print(f"    [HTTP {r.status_code}] {family_name} attempt {attempt}/{attempts}: {preview}")
            r.raise_for_status()
            data = r.json()
            text = None
            for cand in data.get("candidates", []):
                parts = cand.get("content", {}).get("parts", [])
                for p in parts:
                    if "text" in p:
                        text = p["text"]
                        break
                if text:
                    break
            if not text:
                raise RuntimeError("Gemini returned no text payload")
            return json.loads(text)
        except Exception as e:
            last_err = e
            status = _exception_status_code(e)
            err_msg = sanitize_error_message(str(e), api_key=api_key)
            retryable_transport = _is_retryable_transport_error(e)
            if (status in retry_statuses or retryable_transport) and attempt < attempts:
                retry_label = f"status={status}" if status is not None else "transport=timeout_or_connection"
                print(
                    f"    [RETRY] {family_name} attempt {attempt}/{attempts} {retry_label} "
                    f"sleep={sleep_s:.1f}s error={err_msg}"
                )
                if sleep_s > 0:
                    time.sleep(sleep_s)
                sleep_s = max(sleep_s * max(1.0, float(retry_backoff)), sleep_s + 1.0) if sleep_s > 0 else max(1.0, float(retry_backoff))
                continue
            raise RuntimeError(err_msg) from e

    raise RuntimeError(sanitize_error_message(str(last_err), api_key=api_key) if last_err else "Gemini call failed")


def write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def write_md(path: Path, lines: Sequence[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def normalize_families(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    vals = [x.strip() for x in re.split(r"[,;|]", raw) if x.strip()]
    return vals or None


def resolve_pdf_path(backend_root: Path, pdf_arg: Path) -> Path:
    """Resolve the named-reactions PDF from common CHEMLENS locations.

    The original patch assumed backend/app/data/pdfs/named reactions.pdf always existed.
    In real user setups the PDF may live in backend root, one level above backend, or any
    sibling data folder. We search a small set of common paths first, then do a bounded
    recursive search by filename.
    """
    searched: List[Path] = []

    def _add(path: Path) -> None:
        if path not in searched:
            searched.append(path)

    # 1) Honor explicit absolute path first.
    if pdf_arg.is_absolute():
        _add(pdf_arg)
        if pdf_arg.exists():
            return pdf_arg
    else:
        # 2) Original relative behavior.
        path = (backend_root / pdf_arg).resolve()
        _add(path)
        if path.exists():
            return path

    pdf_name = pdf_arg.name or 'named reactions.pdf'
    lower_name = pdf_name.lower()

    # 3) Common CHEMLENS locations.
    common = [
        backend_root / 'app' / 'data' / 'pdfs' / pdf_name,
        backend_root / 'app' / 'data' / 'pdfs' / 'named reactions.pdf',
        backend_root / pdf_name,
        backend_root / 'named reactions.pdf',
        backend_root.parent / pdf_name,
        backend_root.parent / 'named reactions.pdf',
        backend_root / 'data' / 'pdfs' / pdf_name,
        backend_root / 'data' / 'pdfs' / 'named reactions.pdf',
    ]
    for c in common:
        c = c.resolve()
        _add(c)
        if c.exists():
            return c

    # 4) Search pdfs_inventory.csv if present for filename hint.
    inventory_candidates = [backend_root / 'pdfs_inventory.csv', backend_root.parent / 'pdfs_inventory.csv']
    for inv in inventory_candidates:
        inv = inv.resolve()
        _add(inv)
        if inv.exists():
            try:
                lines = inv.read_text(encoding='utf-8', errors='ignore').splitlines()
                for line in lines[1:]:
                    parts = line.split(',')
                    if not parts:
                        continue
                    fname = parts[0].strip()
                    if fname.lower() == lower_name or fname.lower() == 'named reactions.pdf':
                        for base in [backend_root, backend_root.parent]:
                            cand = (base / fname).resolve()
                            _add(cand)
                            if cand.exists():
                                return cand
            except Exception:
                pass

    # 5) Bounded recursive search by exact filename under backend_root and its parent.
    seen_dirs = set()
    for base in [backend_root, backend_root.parent]:
        base = base.resolve()
        if base in seen_dirs or not base.exists():
            continue
        seen_dirs.add(base)
        try:
            for found in base.rglob('*.pdf'):
                _add(found.resolve())
                if found.name.lower() == lower_name or found.name.lower() == 'named reactions.pdf':
                    return found.resolve()
        except Exception:
            continue

    msg = ['[ERROR] PDF file not found.', 'Searched paths:']
    msg.extend([f'  - {x}' for x in searched[:40]])
    msg.append('Put the PDF in one of those locations, or run with --pdf "absolute\path\named reactions.pdf".')
    raise FileNotFoundError('\n'.join(msg))


def _parse_retry_statuses(raw: Any) -> Tuple[int, ...]:
    if raw is None:
        return (429, 500, 503)
    if isinstance(raw, (list, tuple, set)):
        vals = list(raw)
    else:
        vals = [x.strip() for x in str(raw).split(",") if x.strip()]
    out = []
    for v in vals:
        try:
            out.append(int(v))
        except Exception:
            continue
    return tuple(out) if out else (429, 500, 503)


def run(args: argparse.Namespace) -> int:
    args.retry_statuses = _parse_retry_statuses(args.retry_statuses)
    backend_root = Path(args.backend_root).resolve()
    db_path = (backend_root / args.db).resolve()
    pdf_path = resolve_pdf_path(backend_root, args.pdf)
    stage_db = (backend_root / args.stage_db).resolve()
    report_run = now_ts()
    report_dir = (backend_root / args.reports_dir / report_run).resolve()
    crops_dir = report_dir / "crops"
    json_dir = report_dir / "json"
    crops_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)
    ensure_stage_db(stage_db)

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = fetch_application_pages(con, normalize_families(args.families), args.limit_pages)
        if not rows:
            print("[ERROR] No application_example pages found for the given filter.")
            return 1
        doc = fitz.open(str(pdf_path))
        offset = detect_named_reaction_offset(doc, con)
        print("=" * 72)
        print("PDF EXAMPLE AUTOMATION")
        print("=" * 72)
        print(f"backend_root: {backend_root}")
        print(f"db:           {db_path}")
        print(f"pdf:          {pdf_path}")
        print(f"stage_db:     {stage_db}")
        print(f"report_dir:   {report_dir}")
        print(f"mode:         {'GEMINI' if args.call_gemini else 'DRY-RUN'}")
        print(f"page_offset:  {offset}")
        if args.call_gemini:
            print(f"api_auth:     {args.api_auth}")
            print(f"model:        {args.model}")
            print(f"retry_status: {args.retry_statuses}")
            print(f"max_retries:  {args.max_retries}")

        api_key = None
        if args.call_gemini:
            api_key = resolve_api_key(args)
            if not api_key:
                print("[ERROR] No usable Gemini API key found.")
                print("        Use --prompt-api-key to enter it interactively, or set GEMINI_API_KEY in this Anaconda Prompt session.")
                return 1

        summary_rows: List[Dict[str, Any]] = []
        extraction_rows: List[Dict[str, Any]] = []
        stop_after_rate_limit = False
        stage = sqlite3.connect(stage_db)
        stage.row_factory = sqlite3.Row
        try:
            for row in rows:
                if stop_after_rate_limit:
                    break
                pdf_page_no = row.book_page_no + offset
                pdf_idx = pdf_page_no - 1
                if pdf_idx < 0 or pdf_idx >= doc.page_count:
                    print(f"[WARN] skip {row.family_name}: computed pdf_page_no={pdf_page_no} out of range")
                    continue
                page = doc.load_page(pdf_idx)
                regions = find_example_regions(page, row.family_name)
                print(f"[PAGE] {row.family_name} | book p{row.book_page_no} -> pdf p{pdf_page_no} | regions={len(regions)}")
                stage.execute(
                    "INSERT OR IGNORE INTO pdf_example_pages(page_knowledge_id,family_name,family_name_norm,book_page_no,pdf_page_no,title,report_run) VALUES (?,?,?,?,?,?,?)",
                    (row.page_knowledge_id, row.family_name, row.family_name.lower(), row.book_page_no, pdf_page_no, row.title, report_run),
                )
                summary_entry = {
                    "family_name": row.family_name,
                    "page_knowledge_id": row.page_knowledge_id,
                    "book_page_no": row.book_page_no,
                    "pdf_page_no": pdf_page_no,
                    "regions_detected": len(regions),
                }
                summary_rows.append(summary_entry)
                selected_regions = regions[: max(1, args.max_regions_per_page)]
                if len(regions) > len(selected_regions):
                    print(f"    [THROTTLE] {row.family_name}: using {len(selected_regions)}/{len(regions)} regions this run")
                for idx, (x1, y1, x2, y2, para, nearby) in enumerate(selected_regions, start=1):
                    crop_name = f"{row.book_page_no:03d}_{re.sub(r'[^A-Za-z0-9]+','_',row.family_name)[:60]}_r{idx}.png"
                    crop_path = crops_dir / crop_name
                    render_crop(page, (x1, y1, x2, y2), crop_path, dpi=args.dpi)
                    stage.execute(
                        "INSERT OR IGNORE INTO pdf_example_regions(page_knowledge_id,family_name,family_name_norm,book_page_no,pdf_page_no,region_index,x1,y1,x2,y2,paragraph_text,nearby_text,crop_path,report_run) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (row.page_knowledge_id, row.family_name, row.family_name.lower(), row.book_page_no, pdf_page_no, idx, x1, y1, x2, y2, para, nearby[:20000], str(crop_path), report_run),
                    )
                    extraction_entry = {
                        "family_name": row.family_name,
                        "book_page_no": row.book_page_no,
                        "pdf_page_no": pdf_page_no,
                        "region_index": idx,
                        "crop_path": str(crop_path),
                        "paragraph_text": para,
                    }
                    if args.call_gemini:
                        try:
                            print(f"    [CALL] {row.family_name} r{idx} -> Gemini")
                            payload = gemini_call(
                                api_key,
                                args.model,
                                row.family_name,
                                para,
                                nearby,
                                crop_path,
                                timeout_s=args.timeout,
                                max_retries=args.max_retries,
                                retry_initial_sleep=args.retry_initial_sleep,
                                retry_backoff=args.retry_backoff,
                                retry_statuses=args.retry_statuses,
                                api_auth=args.api_auth,
                            )
                            extraction_entry["status"] = "ok"
                            extraction_entry["raw_json_path"] = str(json_dir / (crop_name.replace('.png', '.json')))
                            write_json(Path(extraction_entry["raw_json_path"]), payload)
                            stage.execute(
                                "INSERT OR REPLACE INTO pdf_example_extractions(page_knowledge_id,family_name,family_name_norm,book_page_no,pdf_page_no,region_index,model_name,prompt_version,status,example_target_name,example_summary,region_confidence,raw_json,error_message,report_run) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (
                                    row.page_knowledge_id,
                                    row.family_name,
                                    row.family_name.lower(),
                                    row.book_page_no,
                                    pdf_page_no,
                                    idx,
                                    args.model,
                                    "pdf_example_region_v1",
                                    "ok",
                                    payload.get("example_target_name"),
                                    payload.get("example_summary"),
                                    payload.get("region_confidence"),
                                    json.dumps(payload, ensure_ascii=False),
                                    None,
                                    report_run,
                                ),
                            )
                            print(f"    [OK] {row.family_name} r{idx}")
                            time.sleep(args.sleep)
                        except Exception as e:
                            extraction_entry["status"] = "error"
                            extraction_entry["error"] = sanitize_error_message(str(e), api_key=api_key)
                            stage.execute(
                                "INSERT OR REPLACE INTO pdf_example_extractions(page_knowledge_id,family_name,family_name_norm,book_page_no,pdf_page_no,region_index,model_name,prompt_version,status,example_target_name,example_summary,region_confidence,raw_json,error_message,report_run) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (
                                    row.page_knowledge_id,
                                    row.family_name,
                                    row.family_name.lower(),
                                    row.book_page_no,
                                    pdf_page_no,
                                    idx,
                                    args.model,
                                    "pdf_example_region_v1",
                                    "error",
                                    None,
                                    None,
                                    None,
                                    None,
                                    sanitize_error_message(str(e), api_key=api_key),
                                    report_run,
                                ),
                            )
                            if "429" in str(e) or "Too Many Requests" in str(e):
                                stop_after_rate_limit = True
                                stage.commit()
                                print(f"    [RATE LIMIT] {row.family_name} r{idx} -> stop run immediately; cooldown={args.cooldown_on_429}s; error={e}")
                                if args.cooldown_on_429 > 0:
                                    time.sleep(args.cooldown_on_429)
                    else:
                        extraction_entry["status"] = "dryrun"
                    extraction_rows.append(extraction_entry)
                    if stop_after_rate_limit:
                        break
            stage.commit()
        finally:
            stage.close()
            doc.close()
    finally:
        con.close()

    summary = {
        "report_run": report_run,
        "mode": "gemini" if args.call_gemini else "dryrun",
        "db": str(db_path),
        "pdf": str(pdf_path),
        "stage_db": str(stage_db),
        "families_processed": len(summary_rows),
        "regions_detected_total": sum(int(x["regions_detected"]) for x in summary_rows),
        "pages": summary_rows,
    }
    write_json(report_dir / "pdf_example_automation_summary.json", summary)
    with (report_dir / "pdf_example_regions.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["family_name", "book_page_no", "pdf_page_no", "region_index", "status", "crop_path", "paragraph_text", "raw_json_path", "error"])
        writer.writeheader()
        for row in extraction_rows:
            writer.writerow({k: row.get(k) for k in ["family_name", "book_page_no", "pdf_page_no", "region_index", "status", "crop_path", "paragraph_text", "raw_json_path", "error"]})

    md_lines = [
        "# PDF Example Automation Summary",
        "",
        f"- run: `{report_run}`",
        f"- mode: `{summary['mode']}`",
        f"- families_processed: **{summary['families_processed']}**",
        f"- regions_detected_total: **{summary['regions_detected_total']}**",
        "",
        "## Per page",
        "",
        "| family | book page | pdf page | regions |",
        "|---|---:|---:|---:|",
    ]
    for row in summary_rows:
        md_lines.append(f"| {row['family_name']} | {row['book_page_no']} | {row['pdf_page_no']} | {row['regions_detected']} |")
    write_md(report_dir / "pdf_example_automation_summary.md", md_lines)

    print("=" * 72)
    print(f"[DONE] summary: {report_dir / 'pdf_example_automation_summary.json'}")
    print("=" * 72)
    return 0


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Extract synthetic-application example regions from named reactions PDF.")
    p.add_argument("--backend-root", default=".")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--pdf", type=Path, default=DEFAULT_PDF)
    p.add_argument("--stage-db", type=Path, default=DEFAULT_STAGE_DB)
    p.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS)
    p.add_argument("--families", default=None, help="Comma/semicolon separated family names to limit the run")
    p.add_argument("--limit-pages", type=int, default=0)
    p.add_argument("--dpi", type=int, default=120)
    p.add_argument("--call-gemini", action="store_true")
    p.add_argument("--api-key", default=None, help="Gemini API key passed directly on the command line (not recommended for shared terminals)")
    p.add_argument("--prompt-api-key", action="store_true", help="Prompt for Gemini API key interactively in the current Anaconda Prompt session")
    p.add_argument("--model", default="gemini-2.5-flash")
    p.add_argument("--api-auth", choices=["header", "query"], default="header", help="How to send the Gemini API key. 'header' matches the official curl examples.")
    p.add_argument("--timeout", type=int, default=180, help="Read timeout seconds for Gemini response body; connect timeout is fixed at 30s")
    p.add_argument("--sleep", type=float, default=30.0, help="Seconds between successful LLM calls")
    p.add_argument("--max-retries", type=int, default=2, help="Retry count for transient Gemini HTTP errors such as 429/500/503")
    p.add_argument("--retry-initial-sleep", type=float, default=20.0, help="Initial backoff sleep in seconds after a transient Gemini HTTP error")
    p.add_argument("--retry-backoff", type=float, default=2.0, help="Multiplier applied to retry sleep after each transient Gemini HTTP error")
    p.add_argument("--retry-statuses", default="429,500,503", help="Comma-separated HTTP statuses that should be retried")
    p.add_argument("--max-regions-per-page", type=int, default=1, help="Process only the first N detected example regions per page")
    p.add_argument("--cooldown-on-429", type=float, default=0.0, help="Cooldown seconds after first 429 before stopping the run")
    return p


if __name__ == "__main__":
    raise SystemExit(run(build_argparser().parse_args()))
